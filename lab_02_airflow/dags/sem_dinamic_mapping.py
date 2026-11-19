from __future__ import annotations

from airflow.decorators import dag, task


@dag(dag_id='process_files_hardcoded', schedule=None, start_date=None)
def pipeline():

    @task
    def process_file_1():
        return "arquivo_1.csv processado"

    @task
    def process_file_2():
        return "arquivo_2.csv processado"

    @task
    def process_file_3():
        return "arquivo_3.csv processado"

    # Problema: e se forem 100 arquivos? 1000?
    # Precisa editar o código toda vez
    process_file_1() >> process_file_2() >> process_file_3()


pipeline()
