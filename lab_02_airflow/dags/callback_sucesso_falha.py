from __future__ import annotations

from datetime import timedelta

import requests
from airflow.decorators import dag, task


def on_success_callback(context):
    task_id = context['task'].task_id
    print(f"✓ Task {task_id} completou com sucesso!")


def on_failure_callback(context):
    task_id = context['task'].task_id
    exception = context['exception']
    print(f"✗ Task {task_id} falhou: {exception}")


def on_retry_callback(context):
    task_id = context['task'].task_id
    print(f"↻ Task {task_id} vai fazer retry")


@dag(dag_id='callbacks_example', schedule=None, start_date=None)
def pipeline():

    @task
    def reset_counter():
        # Zera o contador da fake-api antes de cada execução
        requests.get('http://fake-api:5000/reset')

    @task(
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        on_retry_callback=on_retry_callback,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def critical_task():
        # Falha nas 2 primeiras chamadas (on_retry_callback)
        # e funciona na 3ª (on_success_callback)
        response = requests.get('http://fake-api:5000/data')
        response.raise_for_status()
        return response.json()

    @task(
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        on_retry_callback=on_retry_callback,
        retries=1,
        retry_delay=timedelta(seconds=10),
    )
    def task_que_falha():
        # Sempre falha: faz 1 retry (on_retry_callback)
        # e depois falha de vez (on_failure_callback)
        response = requests.get('http://fake-api:5000/data/always-fail')
        response.raise_for_status()

    reset_counter() >> [critical_task(), task_que_falha()]


pipeline()
