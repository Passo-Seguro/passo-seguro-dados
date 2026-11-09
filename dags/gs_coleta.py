"""DAG gs_coleta: coleta diária das quatro fontes para a pasta chegada/ (zona de aterrissagem).

Os arquivos de chegada/ são enviados ao Databricks manualmente, pela interface; esta DAG
não faz upload. Cada tarefa grava chegada/<fonte>/<fonte>_AAAA-MM-DD.<ext> e é idempotente.
Quando uma fonte ainda não publicou o dia (SP156 e Infosiga têm defasagem), a tarefa fica
'skipped', sem arquivo.

Variables do Airflow:
  ALERTA_WEBHOOK_URL     webhook do Discord que recebe o alerta de falha
  PASSO_SEGURO_API_URL   base da API; vazia = coleta de relatos usa o mock
  PASSO_SEGURO_API_TOKEN bearer da API (só se a URL estiver preenchida)
  FORCAR_FALHA_FONTE     só para testar o alerta: sp156 | infosiga | open_meteo | relatos
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from gs_coleta_lib import infosiga, open_meteo, relatos, sp156
from gs_coleta_lib.alerta import notificar_falha

FUSO = "America/Sao_Paulo"


def _dia():
    """Dia coberto pela execução (início do intervalo de dados), no fuso de São Paulo."""
    return get_current_context()["data_interval_start"].in_timezone(FUSO).date()


@dag(
    dag_id="gs_coleta",
    description="Coleta diária de SP156, Infosiga, Open-Meteo e relatos para chegada/",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz=FUSO),
    catchup=True,
    max_active_runs=4,
    max_active_tasks=8,
    default_args={
        "owner": "dados",
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": notificar_falha,
    },
    tags=["passo-seguro", "coleta"],
)
def gs_coleta():
    @task
    def coletar_sp156():
        return sp156.coletar(_dia())

    @task
    def coletar_infosiga():
        return infosiga.coletar(_dia())

    @task
    def coletar_open_meteo():
        return open_meteo.coletar(_dia())

    @task
    def coletar_relatos():
        return relatos.coletar(_dia())

    coletar_sp156()
    coletar_infosiga()
    coletar_open_meteo()
    coletar_relatos()


gs_coleta()
