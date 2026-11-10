"""Conferência de chegada: depois das coletas, confere se cada fonte que terminou em 'success' deixou o arquivo do dia.

A lógica fica aqui, separada da DAG, para ser testável sem o Airflow. A tarefa `conferir_chegada`
(dags/gs_coleta.py) só reúne os estados das coletas do run e chama `conferir`.

O que a conferência verifica, para o dia do run:
  - fonte em 'success': existe chegada/<fonte>/<fonte>_AAAA-MM-DD.<ext> e o arquivo não está vazio (0 bytes);
  - fonte em 'skipped': não deve haver arquivo; o motivo vem do log da tarefa e é só informativo;
  - falha se alguma fonte 'success' não deixou arquivo, se um estado for inesperado ou se nenhuma fonte gravou.
Arquivo CSV só com cabeçalho (ex.: dia do Infosiga sem sinistro com pedestre) é válido: vira aviso, não erro.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from airflow.exceptions import AirflowFailException

from . import comum

log = logging.getLogger(__name__)

# task_id de cada coleta -> fonte (nome da pasta em chegada/). O task_id da Open-Meteo segue
# 'coletar_open_meteo' (aparece como 'coletar_clima' na interface; ver task_display_name na DAG).
TAREFA_DA_FONTE = {
    "sp156": "coletar_sp156",
    "infosiga": "coletar_infosiga",
    "open_meteo": "coletar_open_meteo",
    "relatos": "coletar_relatos",
}

# Prefixo de cada linha do log do Airflow: "[2026-11-09T01:54:48.596+0000] {taskinstance.py:302} INFO - "
_PREFIXO_LOG = re.compile(r"^\[[^\]]*\]\s+\{[^}]*\}\s+\w+\s+-\s+")


@dataclass
class Resultado:
    chegaram: list[str]
    skipped: dict[str, str]  # fonte -> motivo
    problemas: list[str]


def motivo_do_skip(ti, base_logs: Path | None) -> str:
    """Motivo do skip, lido do log da última tentativa (o Airflow não o guarda no banco).

    O AirflowSkipException é registrado como a linha INFO logo antes do bloco 'Post task execution logs'.
    Se o log não existir ou mudar de formato, devolve um texto padrão em vez de falhar a conferência.
    """
    padrao = "motivo não encontrado no log da tarefa"
    if base_logs is None:
        return padrao
    try:
        pasta = Path(base_logs) / f"dag_id={ti.dag_id}" / f"run_id={ti.run_id}" / f"task_id={ti.task_id}"
        tentativas = sorted(
            pasta.glob("attempt=*.log"), key=lambda p: int(re.search(r"attempt=(\d+)", p.name).group(1))
        )
        if not tentativas:
            return padrao
        linhas = tentativas[-1].read_text(encoding="utf-8", errors="replace").splitlines()
        for i, linha in enumerate(linhas):
            if "::group::Post task execution logs" in linha and i > 0:
                return _PREFIXO_LOG.sub("", linhas[i - 1]).strip() or padrao
    except Exception as exc:  # noqa: BLE001 - motivo é só informativo
        log.warning("Não consegui ler o log de %s: %s", ti.task_id, type(exc).__name__)
    return padrao


def conferir(dia: date, tis: dict[str, object], base_logs: Path | None = None) -> Resultado:
    """tis: fonte -> TaskInstance da coleta (precisa de .state, .task_id, .dag_id, .run_id).

    Loga o que chegou e o que foi skipped e por quê; levanta AirflowFailException se algo estiver errado.
    """
    chegaram: list[str] = []
    skipped: dict[str, str] = {}
    problemas: list[str] = []

    log.info("Conferência de chegada do dia %s", dia)
    for fonte in TAREFA_DA_FONTE:
        ti = tis.get(fonte)
        estado = getattr(ti, "state", None)
        caminho = comum.destino(fonte, dia)
        rotulo = f"chegada/{fonte}/{caminho.name}"

        if estado == "success":
            if not caminho.is_file():
                problemas.append(f"{fonte}: a coleta terminou em success, mas {rotulo} não existe")
                log.error("[FALTA]   %-10s %s", fonte, rotulo)
            elif caminho.stat().st_size == 0:
                problemas.append(f"{fonte}: {rotulo} está vazio (0 bytes)")
                log.error("[VAZIO]   %-10s %s", fonte, rotulo)
            else:
                chegaram.append(fonte)
                aviso = ""
                if caminho.suffix == ".csv" and len(caminho.read_text(encoding="utf-8").splitlines()) <= 1:
                    aviso = " (só cabeçalho: nenhum registro no dia)"
                log.info("[CHEGOU]  %-10s %s, %s bytes%s", fonte, rotulo, f"{caminho.stat().st_size:,}".replace(",", "."), aviso)
        elif estado == "skipped":
            motivo = motivo_do_skip(ti, base_logs)
            skipped[fonte] = motivo
            log.info("[SKIPPED] %-10s sem arquivo. Motivo: %s", fonte, motivo)
            if caminho.is_file():
                log.warning("          %-10s mas %s existe (de uma coleta anterior?)", fonte, rotulo)
        else:
            problemas.append(f"{fonte}: estado inesperado da coleta ({estado!r})")
            log.error("[ESTADO]  %-10s %r", fonte, estado)

    log.info("Resumo: %s chegaram (%s); %s skipped (%s).",
             len(chegaram), ", ".join(chegaram) or "nenhuma", len(skipped), ", ".join(skipped) or "nenhuma")

    if not chegaram:
        problemas.append("nenhuma fonte gravou arquivo neste dia")
    if problemas:
        raise AirflowFailException(f"Conferência de chegada de {dia} falhou: " + "; ".join(problemas) + ".")
    return Resultado(chegaram, skipped, problemas)
