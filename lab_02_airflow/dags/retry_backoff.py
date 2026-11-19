from __future__ import annotations

from datetime import timedelta

import requests
from airflow.decorators import dag, task


@dag(dag_id='retry_backoff', schedule=None, start_date=None)
def pipeline():

    @task(
        retries=3,
        retry_delay=timedelta(seconds=10)
    )
    def call_unstable_api():
        # Falha nas primeiras 2 chamadas, sucesso na 3ª
        response = requests.get('http://fake-api:5000/data')
        response.raise_for_status()  # lança exceção se status >= 400
        return response.json()

    @task
    def reset_counter():
        # Reseta o contador antes de cada execução do pipeline
        requests.get('http://fake-api:5000/reset')

    reset = reset_counter()
    reset >> call_unstable_api()


pipeline()
