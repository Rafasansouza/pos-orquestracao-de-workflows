from __future__ import annotations

import requests
from airflow.decorators import dag, task


@dag(dag_id='retry_simples', schedule=None, start_date=None)
def pipeline():

    @task(retries=3)
    def call_unstable_api():
        # Tenta 3 vezes SEM ESPERA entre tentativas
        # Se falhar em 1ms, falha novamente em 2ms
        # Se a API está temporariamente indisponível, todas as 3 falham
        response = requests.get('http://fake-api:5000/data/always-fail')
        response.raise_for_status()
        return response.json()

    call_unstable_api()


pipeline()
