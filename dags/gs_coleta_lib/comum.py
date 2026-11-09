"""Utilidades compartilhadas pelas coletas."""

from __future__ import annotations

import csv
import io
import os
from collections.abc import Callable
from datetime import date
from pathlib import Path

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable

CHEGADA_DIR = Path(os.environ.get("CHEGADA_DIR", "/opt/airflow/chegada"))
ENTRADA_MANUAL_DIR = Path(os.environ.get("ENTRADA_MANUAL_DIR", "/opt/airflow/entrada_manual"))
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/opt/airflow/cache"))

# Sufixo de arquivo de cada fonte em chegada/<fonte>/<fonte>_AAAA-MM-DD.<ext>
EXTENSOES = {"sp156": "csv", "infosiga": "csv", "open_meteo": "json", "relatos": "json"}

# Encoding dos CSVs de origem (SP156 e Infosiga vêm em ISO-8859/Windows-1252).
ENCODING_ORIGEM = "cp1252"


def destino(fonte: str, dia: date) -> Path:
    return CHEGADA_DIR / fonte / f"{fonte}_{dia.isoformat()}.{EXTENSOES[fonte]}"


def gravar_atomico(caminho: Path, escrever: Callable[[Path], None]) -> Path:
    """Escreve num arquivo temporário e troca pelo definitivo, para nunca deixar lote pela metade.

    Reexecutar o mesmo dia sobrescreve o arquivo (a coleta é idempotente).
    """
    caminho.parent.mkdir(parents=True, exist_ok=True)
    parcial = caminho.with_name(caminho.name + ".parcial")
    try:
        escrever(parcial)
        os.replace(parcial, caminho)
    finally:
        parcial.unlink(missing_ok=True)
    return caminho


def csv_para_bytes(cabecalho: list[str], linhas: list[list[str]]) -> bytes:
    """CSV em UTF-8 com ';' e quebra de linha LF (o formato que o Databricks lê sem opções extras)."""
    buf = io.StringIO(newline="")
    escritor = csv.writer(buf, delimiter=";", lineterminator="\n")
    escritor.writerow(cabecalho)
    escritor.writerows(linhas)
    return buf.getvalue().encode("utf-8")


def sem_dados(motivo: str) -> None:
    """Marca a tarefa como 'skipped': a fonte ainda não publicou nada para o dia."""
    raise AirflowSkipException(motivo)


def checar_falha_forcada(fonte: str) -> None:
    """Gancho do teste de alerta: com a Variable FORCAR_FALHA_FONTE=<fonte> a coleta falha de propósito.

    Falha sem nova tentativa (AirflowFailException) para o alerta sair na hora.
    """
    if Variable.get("FORCAR_FALHA_FONTE", default_var="") == fonte:
        raise AirflowFailException(
            f"Falha controlada: FORCAR_FALHA_FONTE={fonte} (teste do alerta; apague a Variable para desfazer)."
        )
