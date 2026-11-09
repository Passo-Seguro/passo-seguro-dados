"""SP156 (dados abertos da Prefeitura): solicitações de calçada abertas em um dia.

A Prefeitura publica um arquivo por trimestre (~100 MB), semanas depois do fim do trimestre,
em CSV e/ou XLSX. A coleta baixa o trimestre uma vez (cache), preferindo o XLSX (datas
tipadas), que converte para o mesmo CSV do 2º TRI (';', DD/MM/AAAA). Depois filtra as linhas do
dia e guarda só os serviços de calçada de SERVICOS_CALCADA. Trimestres já em cache não são
baixados de novo (o 2º TRI 2026 está em cache como o CSV original).

O filtro é por "Serviço", não por "Assunto": em 05/2026 a Prefeitura mudou o Assunto dos mesmos
serviços de calçada de "Calçadas, guias e postes" para "Fiação e Postes", e o Assunto "Árvore"
mistura poda com plantio em calçada. O nome do Serviço não mudou.
"""

from __future__ import annotations

import csv
import fcntl
import logging
import os
import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from airflow.exceptions import AirflowFailException

from . import comum

log = logging.getLogger(__name__)

CKAN_URL = "https://dados.prefeitura.sp.gov.br/api/3/action/package_show?id=dados-do-sp156"

# Os 4 serviços do núcleo calçada (valor exato da coluna "Serviço" do SP156).
# Fora de propósito: tapa-buraco (asfalto), poda de árvore, estacionamento e veículo abandonado.
SERVICOS_CALCADA = (
    "Calçada pública - solicitar manutenção",
    "Calçada particular - denunciar calçada danificada ou inexistente",
    "Acessibilidade - solicitar avaliação de obstáculo na calçada",
    "Guias, sarjetas e sarjetões - solicitar manutenção",
)

# Colunas dos arquivos do SP156 (iguais no CSV e no XLSX), nesta ordem.
COLUNAS = (
    "Data de Abertura", "Data de Finalização", "Meio de Comunicação", "Tema", "Assunto", "Serviço",
    "Status", "Órgão", "Logradouro", "Número", "CEP", "Latitude", "Longitude",
    "Prefeitura Operacional", "Distrito",
)
COLUNAS_DATA = ("Data de Abertura", "Data de Finalização")
COLUNAS_COORDENADA = ("Latitude", "Longitude")
IDX_SERVICO = COLUNAS.index("Serviço")

# O nome do recurso varia: "2º TRI", "3° TRI" (grau, não ordinal), com ou sem espaços.
RESOURCE_TRI = re.compile(r"(\d)\s*[º°ª]?\s*TRI\s*(\d{4})", re.IGNORECASE)
ENCODING_CONVERTIDO = "utf-8"


def _normalizar(texto: str) -> str:
    """Compara sem depender de travessão x hífen, espaços repetidos ou caixa."""
    texto = unicodedata.normalize("NFC", texto).replace("–", "-").replace("—", "-")
    return " ".join(texto.split()).casefold()


SERVICOS_NORMALIZADOS = frozenset(_normalizar(s) for s in SERVICOS_CALCADA)


def _recurso_do_trimestre(ano: int, tri: int) -> tuple[str, str] | None:
    """(url, extensão) do recurso do trimestre no CKAN, preferindo CSV; None se não publicado.

    A extensão vem da URL, não do campo `format` do CKAN, que erra em alguns recursos.
    """
    resp = requests.get(CKAN_URL, timeout=60)
    resp.raise_for_status()
    achados: dict[str, str] = {}
    for recurso in resp.json()["result"]["resources"]:
        nome = recurso.get("name", "")
        casou = RESOURCE_TRI.search(nome)
        if not casou or "sp156" not in nome.lower() or (int(casou[2]), int(casou[1])) != (ano, tri):
            continue
        extensao = os.path.splitext(urlparse(recurso["url"]).path)[1].lower().lstrip(".")
        if extensao in ("csv", "xlsx"):
            achados.setdefault(extensao, recurso["url"])
    # XLSX primeiro: as datas vêm tipadas. O CSV do 3º TRI 2026 veio em outro dialeto (vírgula,
    # UTF-8, datas MM/DD/AAAA), e ler "07/01/2026" como DD/MM mudaria o dia sem dar erro.
    for extensao in ("xlsx", "csv"):
        if extensao in achados:
            return achados[extensao], extensao
    return None


def _baixar(url: str, destino: Path) -> None:
    parcial = destino.with_name(destino.name + ".parcial")
    with requests.get(url, stream=True, timeout=(30, 120)) as resp:
        resp.raise_for_status()
        esperado = int(resp.headers.get("Content-Length", 0))
        with open(parcial, "wb") as saida:
            for bloco in resp.iter_content(1 << 20):
                saida.write(bloco)
    if esperado and parcial.stat().st_size != esperado:
        parcial.unlink()
        raise RuntimeError("Download do SP156 incompleto (tamanho diferente do Content-Length).")
    os.replace(parcial, destino)


def _celula(valor, coluna: str) -> str:
    """Valor de uma célula do XLSX no texto que o CSV da Prefeitura teria."""
    from openpyxl.utils.datetime import from_excel

    if valor is None:
        return ""
    if coluna in COLUNAS_DATA:
        if isinstance(valor, (int, float)):
            valor = from_excel(valor)
        if isinstance(valor, (datetime, date)):
            return valor.strftime("%d/%m/%Y")
        return str(valor)
    if isinstance(valor, float) and valor.is_integer() and coluna not in COLUNAS_COORDENADA:
        valor = int(valor)
    if coluna == "CEP" and isinstance(valor, int):
        return f"{valor:08d}"  # o número do Excel perde o zero à esquerda
    return str(valor)


def _xlsx_para_csv(xlsx: Path, destino: Path) -> None:
    """Converte o XLSX do SP156 em CSV (';', UTF-8) com as mesmas colunas, lendo em streaming."""
    from openpyxl import load_workbook

    planilha = load_workbook(xlsx, read_only=True, data_only=True)
    parcial = destino.with_name(destino.name + ".parcial")
    try:
        linhas = planilha.worksheets[0].iter_rows(values_only=True)
        cabecalho = tuple(str(c).strip() if c is not None else "" for c in next(linhas))[: len(COLUNAS)]
        if cabecalho != COLUNAS:
            raise AirflowFailException(f"Colunas do XLSX do SP156 mudaram: {cabecalho}")
        total = 0
        primeira = ultima = None
        with open(parcial, "w", encoding=ENCODING_CONVERTIDO, newline="") as saida:
            escritor = csv.writer(saida, delimiter=";", lineterminator="\n")
            escritor.writerow(COLUNAS)
            for linha in linhas:
                linha = tuple(linha) + (None,) * (len(COLUNAS) - len(linha))
                campos = [_celula(v, c) for v, c in zip(linha, COLUNAS)]
                if not any(campos):
                    continue
                escritor.writerow(campos)
                total += 1
                abertura = campos[0]
                if len(abertura) == 10:  # DD/MM/AAAA -> AAAA-MM-DD só para achar o intervalo
                    iso = f"{abertura[6:]}-{abertura[3:5]}-{abertura[:2]}"
                    primeira = iso if primeira is None or iso < primeira else primeira
                    ultima = iso if ultima is None or iso > ultima else ultima
        os.replace(parcial, destino)
        log.info("XLSX do SP156 convertido: %s linhas, aberturas de %s a %s.", total, primeira, ultima)
    finally:
        planilha.close()
        parcial.unlink(missing_ok=True)


def _baixar_trimestre(ano: int, tri: int) -> tuple[Path, str] | None:
    """(CSV do trimestre em cache, encoding) ou None se não publicado.

    Baixa (e converte, se for XLSX) uma única vez, mesmo com várias tarefas ao mesmo tempo.
    """
    pasta = comum.CACHE_DIR / "sp156"
    pasta.mkdir(parents=True, exist_ok=True)
    bruto = pasta / f"sp156_{ano}_T{tri}.csv"  # CSV da Prefeitura, como veio (cp1252)
    convertido = pasta / f"sp156_{ano}_T{tri}_xlsx.csv"  # vindo do XLSX, já convertido (UTF-8)
    with open(pasta / ".trava", "w") as trava:
        fcntl.flock(trava, fcntl.LOCK_EX)  # quem chegar depois espera e encontra o arquivo pronto
        if bruto.exists():
            return bruto, comum.ENCODING_ORIGEM
        if convertido.exists():
            return convertido, ENCODING_CONVERTIDO
        achado = _recurso_do_trimestre(ano, tri)
        if achado is None:
            return None
        url, extensao = achado
        log.info("Baixando SP156 %sº TRI %s em %s...", tri, ano, extensao.upper())
        if extensao == "csv":
            _baixar(url, bruto)
            try:  # recusa já no download um CSV em outro dialeto, para não envenenar o cache
                with open(bruto, "rb") as fh:
                    cabecalho = next(csv.reader([fh.readline().decode(comum.ENCODING_ORIGEM).strip("\r\n")], delimiter=";"))
                if tuple(cabecalho) != COLUNAS:
                    raise AirflowFailException(f"CSV do SP156 {tri}º TRI {ano} fora do formato esperado (';', cp1252): {cabecalho[:3]}")
            except Exception:
                bruto.unlink(missing_ok=True)
                raise
            return bruto, comum.ENCODING_ORIGEM
        xlsx = pasta / f"sp156_{ano}_T{tri}.xlsx"
        try:
            _baixar(url, xlsx)
            _xlsx_para_csv(xlsx, convertido)
        finally:
            xlsx.unlink(missing_ok=True)
        return convertido, ENCODING_CONVERTIDO


def coletar(dia: date) -> dict:
    comum.checar_falha_forcada("sp156")
    tri = (dia.month - 1) // 3 + 1
    trimestre = _baixar_trimestre(dia.year, tri)
    if trimestre is None:
        comum.sem_dados(f"SP156 ainda não publicou o {tri}º trimestre de {dia.year}.")
    arquivo, encoding = trimestre

    alvo = dia.strftime("%d/%m/%Y").encode() + b";"
    with open(arquivo, "rb", buffering=8 << 20) as fh:
        cabecalho = next(csv.reader([fh.readline().decode(encoding).strip("\r\n")], delimiter=";"))
        if tuple(cabecalho) != COLUNAS:
            raise AirflowFailException(f"Colunas do SP156 mudaram em {arquivo.name}: {cabecalho}")
        total, linhas, malformadas = 0, [], 0
        for bruta in fh:
            if not bruta.startswith(alvo):  # filtro barato antes de interpretar a linha
                continue
            campos = next(csv.reader([bruta.decode(encoding).strip("\r\n")], delimiter=";"))
            if len(campos) != len(cabecalho):
                malformadas += 1
                continue
            total += 1
            if _normalizar(campos[IDX_SERVICO]) in SERVICOS_NORMALIZADOS:
                linhas.append(campos)

    if total == 0:
        comum.sem_dados(f"Nenhuma solicitação do SP156 aberta em {dia}.")
    if malformadas:
        log.warning("%s linha(s) do SP156 ignoradas por número de colunas diferente do cabeçalho.", malformadas)

    saida = comum.destino("sp156", dia)
    comum.gravar_atomico(saida, lambda p: p.write_bytes(comum.csv_para_bytes(cabecalho, linhas)))
    log.info("SP156 %s: %s solicitações no dia, %s de calçada gravadas em %s.", dia, total, len(linhas), saida)
    return {"arquivo": str(saida), "total_dia": total, "calcada": len(linhas)}
