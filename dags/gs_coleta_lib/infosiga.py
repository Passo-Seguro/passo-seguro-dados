"""Infosiga (Detran-SP): sinistros da capital com pedestre envolvido, em um dia.

A fonte é manual: o ZIP (ou os CSVs já extraídos) baixado do site do Infosiga fica em
entrada_manual/infosiga/. Esta coleta lê tudo de lá, sem acesso à internet, e grava em
chegada/infosiga/infosiga_AAAA-MM-DD.csv uma linha por sinistro do dia, no município de
SAO PAULO, que tenha ao menos uma vítima pedestre (pessoas.tipo_de_vitima = 'PEDESTRE',
ligada por id_sinistro). Só contagens de pedestres: sem veículos e sem dados individuais
de pessoa (idade, sexo etc.).

Aceita os dois formatos de entrada: CSVs soltos (`sinistros_2025-2026.csv`) e ZIPs que
contenham CSVs com esses mesmos nomes.
"""

from __future__ import annotations

import csv
import logging
import re
import zipfile
from collections.abc import Iterator
from contextlib import ExitStack
from datetime import date
from typing import BinaryIO

from airflow.exceptions import AirflowFailException

from . import comum

log = logging.getLogger(__name__)

BUFFER_LEITURA = 8 << 20
MUNICIPIO = "SAO PAULO"
TIPO_PEDESTRE = "PEDESTRE"  # pessoas.tipo_de_vitima
GRAVIDADE_MORTO = "FATAL"  # pessoas.gravidade_lesao
GRAVIDADE_FERIDO = ("GRAVE", "LEVE")  # ILESO e NAO DISPONIVEL só entram em qtd_pedestres
# Colunas copiadas do sinistro, na ordem de saída (a 3ª, hora_sinistro, sai como "hora").
COLUNAS_ORIGEM = ("id_sinistro", "data_sinistro", "hora_sinistro", "municipio", "logradouro", "latitude", "longitude")
COLUNAS_SAIDA = ("id_sinistro", "data_sinistro", "hora", "municipio", "logradouro", "latitude", "longitude")
FAIXA_DE_ANOS = re.compile(r"_(\d{4})-(\d{4})\.csv$", re.IGNORECASE)


def _cobre_o_ano(nome: str, ano: int) -> bool:
    faixa = FAIXA_DE_ANOS.search(nome)
    return True if faixa is None else int(faixa[1]) <= ano <= int(faixa[2])


def _fontes(tipo: str, ano: int, pilha: ExitStack) -> Iterator[tuple[str, BinaryIO]]:
    """CSVs (soltos ou dentro de ZIPs) de um tipo que cobrem o ano, abertos em binário."""
    pasta = comum.ENTRADA_MANUAL_DIR / "infosiga"
    prefixo = f"{tipo}_"
    for caminho in sorted(pasta.glob(f"{prefixo}*.csv")):
        if _cobre_o_ano(caminho.name, ano):
            # Buffer grande: com o padrão (8 KB) o bind mount do Docker no Windows fica ~30x mais lento.
            yield caminho.name, pilha.enter_context(open(caminho, "rb", buffering=BUFFER_LEITURA))
    for caminho_zip in sorted(pasta.glob("*.zip")):
        pacote = pilha.enter_context(zipfile.ZipFile(caminho_zip))
        for membro in sorted(pacote.namelist()):
            base = membro.rsplit("/", 1)[-1]
            if base.lower().startswith(prefixo) and base.lower().endswith(".csv") and _cobre_o_ano(base, ano):
                yield f"{caminho_zip.name}:{base}", pilha.enter_context(pacote.open(membro))


def _ler(linha: bytes) -> list[str]:
    return next(csv.reader([linha.decode(comum.ENCODING_ORIGEM).strip("\r\n")], delimiter=";"))


def _linhas_do_dia(tipo: str, dia: date, aceitar=None) -> tuple[list[str], list[list[str]]]:
    alvo = dia.strftime("%d/%m/%Y")
    agulha = f";{alvo};".encode()  # filtro barato por bytes antes de interpretar a linha
    cabecalho: list[str] | None = None
    achadas: list[list[str]] = []
    with ExitStack() as pilha:
        encontrou_arquivo = False
        for nome, fh in _fontes(tipo, dia.year, pilha):
            encontrou_arquivo = True
            cab = _ler(fh.readline())
            cabecalho = cabecalho or cab
            idx_data = cab.index("data_sinistro")
            lidas = 0
            for bruta in fh:
                if agulha not in bruta:
                    continue
                campos = _ler(bruta)
                # data_obito também pode ser igual ao dia: confere a coluna certa.
                if len(campos) == len(cab) and campos[idx_data] == alvo and (aceitar is None or aceitar(cab, campos)):
                    achadas.append(campos)
                    lidas += 1
            log.info("%s: %s linha(s) de %s em %s.", tipo, lidas, alvo, nome)
    if not encontrou_arquivo:
        raise AirflowFailException(
            f"Nenhum arquivo '{tipo}_*.csv' (ou ZIP com ele) para {dia.year} em {comum.ENTRADA_MANUAL_DIR / 'infosiga'}."
        )
    return cabecalho, achadas


def coletar(dia: date) -> dict:
    comum.checar_falha_forcada("infosiga")

    cab_sin, sinistros = _linhas_do_dia(
        "sinistros", dia, lambda cab, c: c[cab.index("municipio")].strip() == MUNICIPIO
    )
    if not sinistros:
        comum.sem_dados(f"Infosiga sem sinistros em {MUNICIPIO} em {dia} (provável defasagem de publicação).")

    # Pedestres do dia, ligados ao sinistro por pessoas.id_sinistro -> sinistros.id_sinistro.
    # Só a contagem por gravidade sai daqui: nenhuma coluna individual da pessoa é gravada.
    ids = {c[cab_sin.index("id_sinistro")] for c in sinistros}
    eh_pedestre = lambda cab, c: (  # noqa: E731
        c[cab.index("id_sinistro")] in ids and c[cab.index("tipo_de_vitima")].strip() == TIPO_PEDESTRE
    )
    cab_pes, pedestres = _linhas_do_dia("pessoas", dia, eh_pedestre)
    i_id, i_grav = cab_pes.index("id_sinistro"), cab_pes.index("gravidade_lesao")
    contagem: dict[str, list[int]] = {}  # id_sinistro -> [pedestres, feridos, mortos]
    for pessoa in pedestres:
        n = contagem.setdefault(pessoa[i_id], [0, 0, 0])
        n[0] += 1
        gravidade = pessoa[i_grav].strip()
        n[1] += gravidade in GRAVIDADE_FERIDO
        n[2] += gravidade == GRAVIDADE_MORTO

    i = {origem: cab_sin.index(origem) for origem in COLUNAS_ORIGEM}
    linhas = [  # sinistros sem coordenada ficam (a Prata trata); latitude/longitude como vêm, com vírgula
        [s[i[o]] for o in COLUNAS_ORIGEM] + [str(v) for v in contagem[s[i["id_sinistro"]]]]
        for s in sinistros
        if s[i["id_sinistro"]] in contagem
    ]

    saida = comum.destino("infosiga", dia)
    cabecalho = list(COLUNAS_SAIDA) + ["qtd_pedestres", "qtd_pedestres_feridos", "qtd_pedestres_mortos"]
    comum.gravar_atomico(saida, lambda p: p.write_bytes(comum.csv_para_bytes(cabecalho, linhas)))
    sem_coord = sum(1 for l in linhas if not (l[5].strip() and l[6].strip()))
    log.info(
        "Infosiga %s: %s sinistros em %s, %s com pedestre (%s sem lat/lon) gravados em %s.",
        dia, len(sinistros), MUNICIPIO, len(linhas), sem_coord, saida,
    )
    return {"arquivo": str(saida), "sinistros_municipio": len(sinistros), "com_pedestre": len(linhas), "sem_coordenada": sem_coord}
