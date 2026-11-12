"""
=============================================================================
DAG: clima_etl_avancado  (EXTENSÃO — para alunos que terminarem o lab cedo)
Aula 1 — Orquestração de Workflows

Demonstra conceitos avançados:
  1. Fan-out: tasks de fetch para cada cidade em PARALELO
  2. BranchPythonOperator: condicional baseado em dados
  3. Callbacks: on_failure_callback para alertas
  4. SLA: limite de tempo de execução por task
=============================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_lab"

CIDADES = {
    "sao_paulo":      ("São Paulo",      -23.5505, -46.6333),
    "rio_de_janeiro": ("Rio de Janeiro", -22.9068, -43.1729),
    "brasilia":       ("Brasília",       -15.7801, -47.9292),
}

TEMP_ALERTA_C = 35.0   # temperatura máxima que dispara alerta


def on_failure_alert(context):
    """Callback executado quando qualquer task falha."""
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    exception = context.get("exception")
    log.error(
        "🚨 FALHA no DAG '%s' | Task '%s' | Run '%s' | Erro: %s",
        dag_id, task_id, run_id, exception,
    )
    # Em produção: enviar email, Slack, PagerDuty, etc.


@dag(
    dag_id="clima_etl_avancado",
    description="ETL avançado: fan-out paralelo + branch condicional (extensão lab)",
    schedule="0 21 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "lab",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "on_failure_callback": on_failure_alert,
        "sla": timedelta(minutes=10),          # task não pode levar mais de 10min
    },
    tags=["lab", "aula-1", "extensão", "avançado"],
)
def clima_etl_avancado():
    """Pipeline ETL avançado — extensão do lab Aula 1."""

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    # =========================================================================
    # Fan-out: cada cidade roda em PARALELO (task_group por cidade)
    # =========================================================================
    resultados_por_cidade = []

    for cidade_id, (cidade_nome, lat, lon) in CIDADES.items():

        @task_group(group_id=f"etl_{cidade_id}")
        def pipeline_cidade(cidade_nome, lat, lon):

            @task(task_id="buscar")
            def buscar(**context) -> dict:
                import requests
                log.info("Buscando clima para %s", cidade_nome)
                r = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params={
                        "latitude": lat, "longitude": lon,
                        "hourly": "temperature_2m",
                        "daily": "temperature_2m_max",
                        "start_date": context["ds"],  # dia da execução
                        "end_date": context["ds"],
                        "timezone": "America/Sao_Paulo",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                dados = r.json()
                dados["_cidade"] = cidade_nome
                return dados

            @task(task_id="transformar")
            def transformar(dados: dict) -> dict:
                import pandas as pd
                df = pd.DataFrame({
                    "hora": dados["hourly"]["time"],
                    "temperatura_c": dados["hourly"]["temperature_2m"],
                })
                df["hora"] = pd.to_datetime(df["hora"])
                df = df.dropna()
                # XCom serializa em JSON: Timestamp do pandas não é suportado -> string ISO
                df["hora"] = df["hora"].dt.strftime("%Y-%m-%dT%H:%M:%S")
                return {
                    "cidade": dados["_cidade"],
                    "latitude": dados["latitude"],
                    "longitude": dados["longitude"],
                    "registros": df.to_dict(orient="records"),
                    "temp_max": dados["daily"]["temperature_2m_max"][0],
                }

            @task(task_id="carregar")
            def carregar(dados_transformados: dict, **context) -> dict:
                from airflow.providers.postgres.hooks.postgres import PostgresHook
                hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                conn = hook.get_conn()
                cur = conn.cursor()
                run_id = context["run_id"]
                cidade = dados_transformados["cidade"]
                try:
                    cur.execute("DELETE FROM clima WHERE cidade = %s AND DATE(hora) = %s",
                                (cidade, context["ds"]))
                    cur.executemany(
                        "INSERT INTO clima (cidade, latitude, longitude, hora, temperatura_c, dag_run_id) VALUES (%s,%s,%s,%s,%s,%s)",
                        [(cidade, dados_transformados["latitude"], dados_transformados["longitude"],
                          r["hora"], r["temperatura_c"], run_id)
                         for r in dados_transformados["registros"]]
                    )
                    conn.commit()
                    log.info("✓ %s: %d registros inseridos", cidade, len(dados_transformados["registros"]))
                finally:
                    cur.close()
                    conn.close()
                return {"cidade": cidade, "temp_max": dados_transformados["temp_max"]}

            return carregar(transformar(buscar()))

        resultado = pipeline_cidade(cidade_nome, lat, lon)
        inicio >> resultado
        resultados_por_cidade.append(resultado)

    # =========================================================================
    # Fan-in + Branch: verificar se alguma cidade atingiu temperatura de alerta
    # =========================================================================
    @task(task_id="consolidar")
    def consolidar(resultados: list[dict]) -> float:
        temp_max_global = max(r["temp_max"] for r in resultados if r)
        log.info("Temperatura máxima global: %.1f°C (limiar de alerta: %.1f°C)",
                 temp_max_global, TEMP_ALERTA_C)
        return temp_max_global

    def decidir_alerta(ti=None, **kwargs) -> str:
        temp = ti.xcom_pull(task_ids="consolidar")
        if temp and temp >= TEMP_ALERTA_C:
            return "enviar_alerta"
        return "sem_alerta"

    @task(task_id="enviar_alerta")
    def enviar_alerta(ti=None) -> None:
        temp = ti.xcom_pull(task_ids="consolidar")
        log.warning("🌡️ ALERTA: temperatura máxima de %.1f°C detectada!", temp)
        # Em produção: requests.post(slack_webhook, json={"text": ...})

    sem_alerta = EmptyOperator(task_id="sem_alerta")

    branch = BranchPythonOperator(
        task_id="verificar_alerta",
        python_callable=decidir_alerta,
    )

    temp_max = consolidar(resultados_por_cidade)
    temp_max >> branch
    branch >> [enviar_alerta(), sem_alerta]
    [enviar_alerta(), sem_alerta] >> fim


dag_instance = clima_etl_avancado()
