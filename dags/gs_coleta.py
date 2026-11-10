"""DAG gs_coleta: coleta diária das quatro fontes para a pasta chegada/ (zona de aterrissagem).

Estrutura:  inicio >> [coletar_sp156, coletar_infosiga, coletar_open_meteo, coletar_relatos] >> conferir_chegada

Os arquivos de chegada/ são enviados ao Databricks manualmente, pela interface; esta DAG
não faz upload. Cada coleta grava chegada/<fonte>/<fonte>_AAAA-MM-DD.<ext> e é idempotente.
Depois das quatro coletas, conferir_chegada confere se cada uma que terminou em 'success' deixou
o arquivo do dia (e se ele não está vazio), loga o que chegou e o que foi 'skipped' com o motivo, e
falha (com o mesmo alerta do Discord) se uma coleta 'success' não deixou arquivo ou se nenhuma
fonte gravou. Ela roda mesmo que todas as coletas tenham ficado 'skipped' (trigger_rule none_failed),
justamente para falhar nesse caso em vez de passar em silêncio.

Por que SP156 e Infosiga aparecem como 'skipped'. As duas fontes só publicam o trimestre depois que
ele fecha, e com semanas de atraso. Dia cujo trimestre ainda não saiu (hoje, de 01/10/2026 em
diante) não tem arquivo, e a coleta fica 'skipped' em vez de falhar: é esperado, não é erro. A
Open-Meteo e os relatos não têm essa defasagem e gravam todos os dias. Quando o trimestre sair, limpe
as coletas 'skipped' daquelas datas para recoletar.

Variables do Airflow:
  ALERTA_WEBHOOK_URL     webhook do Discord que recebe o alerta de falha
  PASSO_SEGURO_API_URL   base da API; vazia = coleta de relatos usa o mock
  PASSO_SEGURO_API_TOKEN bearer da API (só se a URL estiver preenchida)
  FORCAR_FALHA_FONTE     só para testar o alerta: sp156 | infosiga | open_meteo | relatos
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from gs_coleta_lib import conferir, infosiga, open_meteo, relatos, sp156
from gs_coleta_lib.alerta import notificar_falha

FUSO = "America/Sao_Paulo"


def _dia():
    """Dia coberto pela execução (início do intervalo de dados), no fuso de São Paulo."""
    return get_current_context()["data_interval_start"].in_timezone(FUSO).date()


@dag(
    dag_id="gs_coleta",
    description="Coleta diária de SP156, Infosiga, Open-Meteo e relatos para chegada/, com conferência de chegada",
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
    inicio = EmptyOperator(task_id="inicio")

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

    # NONE_FAILED: roda quando nenhuma coleta falhou, mesmo que todas tenham ficado 'skipped'; assim ela
    # falha ("nenhuma fonte gravou") em vez de ficar 'skipped' sem avisar. Se uma coleta falhar, o alerta
    # já sai dela e a conferência fica 'upstream_failed'.
    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def conferir_chegada():
        contexto = get_current_context()
        run = contexto["dag_run"]
        por_tarefa = {ti.task_id: ti for ti in run.get_task_instances()}
        tis = {fonte: por_tarefa.get(tarefa) for fonte, tarefa in conferir.TAREFA_DA_FONTE.items()}
        resultado = conferir.conferir(_dia(), tis, base_logs=conf.get("logging", "base_log_folder"))
        return {"chegaram": resultado.chegaram, "skipped": resultado.skipped}

    coletas = [coletar_sp156(), coletar_infosiga(), coletar_open_meteo(), coletar_relatos()]
    inicio >> coletas >> conferir_chegada()


gs_coleta()
