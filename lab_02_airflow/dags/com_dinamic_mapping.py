from __future__ import annotations

from airflow.decorators import dag, task

@dag(dag_id='process_files_dynamic', schedule=None, start_date=None)
def pipeline():
    
    @task
    def list_files():
        # Retorna lista dinâmica de arquivos
        return ['arquivo_1.csv', 'arquivo_2.csv', 'arquivo_3.csv']
    
    @task
    def process_file(file_path):
        # Task parametrizada
        print(f"Processando: {file_path}")
        return f"{file_path} processado"
    
    @task
    def consolidate(resultados):
        # Consolida resultados de todas as tasks
        print(f"Todos os arquivos: {resultados}")
        return resultados
    
    # Dynamic mapping: cria uma task por arquivo
    files = list_files()
    processed = process_file.expand(file_path=files)
    consolidate(processed)


pipeline()