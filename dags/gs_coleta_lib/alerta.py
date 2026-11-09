"""Alerta de falha de tarefa num webhook do Discord (Variable ALERTA_WEBHOOK_URL)."""

from __future__ import annotations

import logging

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

COR_VERMELHA = 0xE74C3C
# O Discord (Cloudflare) recusa o User-Agent padrão do requests/urllib em alguns casos.
USER_AGENT = "passo-seguro-airflow-alerta/1.0"


def notificar_falha(contexto: dict) -> None:
    """on_failure_callback: nunca levanta erro, para um alerta quebrado não esconder a falha real."""
    url = Variable.get("ALERTA_WEBHOOK_URL", default_var="")
    if not url:
        log.warning("ALERTA_WEBHOOK_URL vazia: alerta de falha não enviado.")
        return

    ti = contexto["task_instance"]
    erro = str(contexto.get("exception") or "sem detalhe")[:900]
    corpo = {
        "username": "Airflow · Passo Seguro",
        "embeds": [
            {
                "title": f"Falha na coleta: {ti.dag_id}.{ti.task_id}",
                "color": COR_VERMELHA,
                "fields": [
                    {"name": "Dia da coleta", "value": str(contexto["data_interval_start"].date()), "inline": True},
                    {"name": "Tentativa", "value": f"{ti.try_number}", "inline": True},
                    {"name": "Run", "value": f"`{contexto['run_id']}`", "inline": False},
                    {"name": "Erro", "value": f"```{erro}```", "inline": False},
                ],
                "footer": {"text": "Logs: Airflow > gs_coleta > Grid"},
            }
        ],
    }
    try:
        resp = requests.post(url, json=corpo, timeout=10, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        log.info("Alerta de falha enviado ao Discord (HTTP %s).", resp.status_code)
    except Exception as exc:  # noqa: BLE001
        # Não registrar str(exc): a mensagem do requests inclui a URL, que contém o token do webhook.
        status = getattr(getattr(exc, "response", None), "status_code", "?")
        log.error("Não consegui enviar o alerta ao Discord (%s, HTTP %s).", type(exc).__name__, status)
