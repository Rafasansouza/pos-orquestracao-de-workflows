"""
DAG monolítico com código repetido
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
}


@dag(
    dag_id='pipeline_clima_monolitico',
    default_args=default_args,
    schedule='@daily',
    catchup=False,  # parâmetro do DAG: dentro de default_args ele é ignorado
    tags=['monolitico', 'aula2'],
)
def pipeline_clima():
    """Pipeline monolítico de coleta de clima de 3 cidades - COM CÓDIGO REPETIDO"""

    @task
    def fetch_sao_paulo():
        """Busca dados de clima de São Paulo"""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": -23.5505,
            "longitude": -46.6333,
            "hourly": "temperature_2m,precipitation",
            "forecast_days": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @task
    def fetch_rio_de_janeiro():
        """Busca dados de clima do Rio de Janeiro"""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": -22.9068,
            "longitude": -43.1729,
            "hourly": "temperature_2m,precipitation",
            "forecast_days": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @task
    def fetch_belo_horizonte():
        """Busca dados de clima de Belo Horizonte"""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": -19.8267,
            "longitude": -43.9345,
            "hourly": "temperature_2m,precipitation",
            "forecast_days": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @task
    def validate_sao_paulo(dados):
        """Valida dados de São Paulo - CÓDIGO REPETIDO"""
        if 'hourly' not in dados:
            raise ValueError("Dados incompletos: falta 'hourly'")
        
        if 'temperature_2m' not in dados['hourly']:
            raise ValueError("Dados incompletos: falta 'temperature_2m'")
        
        temps = dados['hourly']['temperature_2m']
        if len(temps) == 0:
            raise ValueError("Lista de temperaturas vazia")
        
        if max(temps) > 60 or min(temps) < -60:
            raise ValueError("Temperatura fora do range esperado")
        
        return True

    @task
    def validate_rio_de_janeiro(dados):
        """Valida dados do Rio - CÓDIGO REPETIDO (idêntico)"""
        if 'hourly' not in dados:
            raise ValueError("Dados incompletos: falta 'hourly'")
        
        if 'temperature_2m' not in dados['hourly']:
            raise ValueError("Dados incompletos: falta 'temperature_2m'")
        
        temps = dados['hourly']['temperature_2m']
        if len(temps) == 0:
            raise ValueError("Lista de temperaturas vazia")
        
        if max(temps) > 60 or min(temps) < -60:
            raise ValueError("Temperatura fora do range esperado")
        
        return True

    @task
    def validate_belo_horizonte(dados):
        """Valida dados de Belo Horizonte - CÓDIGO REPETIDO (idêntico)"""
        if 'hourly' not in dados:
            raise ValueError("Dados incompletos: falta 'hourly'")
        
        if 'temperature_2m' not in dados['hourly']:
            raise ValueError("Dados incompletos: falta 'temperature_2m'")
        
        temps = dados['hourly']['temperature_2m']
        if len(temps) == 0:
            raise ValueError("Lista de temperaturas vazia")
        
        if max(temps) > 60 or min(temps) < -60:
            raise ValueError("Temperatura fora do range esperado")
        
        return True

    @task
    def transform_sao_paulo(dados):
        """Transforma dados de SP - CÓDIGO REPETIDO"""
        horas = dados['hourly']['time']
        temps = dados['hourly']['temperature_2m']
        precip = dados['hourly']['precipitation']
        
        df = pd.DataFrame({
            'hora': horas,
            'temperatura_c': temps,
            'precipitacao_mm': precip,
            'cidade': 'São Paulo'
        })
        
        df['hora'] = pd.to_datetime(df['hora'])
        df = df.dropna()
        df['temperatura_c'] = df['temperatura_c'].round(2)
        df['precipitacao_mm'] = df['precipitacao_mm'].round(2)
        # XCom serializa em JSON: Timestamp do pandas não é suportado -> string ISO
        df['hora'] = df['hora'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        return df.to_dict(orient='records')

    @task
    def transform_rio_de_janeiro(dados):
        """Transforma dados do Rio - CÓDIGO REPETIDO (idêntico)"""
        horas = dados['hourly']['time']
        temps = dados['hourly']['temperature_2m']
        precip = dados['hourly']['precipitation']
        
        df = pd.DataFrame({
            'hora': horas,
            'temperatura_c': temps,
            'precipitacao_mm': precip,
            'cidade': 'Rio de Janeiro'
        })
        
        df['hora'] = pd.to_datetime(df['hora'])
        df = df.dropna()
        df['temperatura_c'] = df['temperatura_c'].round(2)
        df['precipitacao_mm'] = df['precipitacao_mm'].round(2)
        # XCom serializa em JSON: Timestamp do pandas não é suportado -> string ISO
        df['hora'] = df['hora'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        return df.to_dict(orient='records')

    @task
    def transform_belo_horizonte(dados):
        """Transforma dados de BH - CÓDIGO REPETIDO (idêntico)"""
        horas = dados['hourly']['time']
        temps = dados['hourly']['temperature_2m']
        precip = dados['hourly']['precipitation']
        
        df = pd.DataFrame({
            'hora': horas,
            'temperatura_c': temps,
            'precipitacao_mm': precip,
            'cidade': 'Belo Horizonte'
        })
        
        df['hora'] = pd.to_datetime(df['hora'])
        df = df.dropna()
        df['temperatura_c'] = df['temperatura_c'].round(2)
        df['precipitacao_mm'] = df['precipitacao_mm'].round(2)
        # XCom serializa em JSON: Timestamp do pandas não é suportado -> string ISO
        df['hora'] = df['hora'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        return df.to_dict(orient='records')

    @task
    def consolidar(dados_sp, dados_rj, dados_bh):
        """Consolida os 3 datasets em um"""
        todos = dados_sp + dados_rj + dados_bh
        return todos

    @task
    def salvar_no_banco(registros):
        """Salva dados consolidados no PostgreSQL"""
        hook = PostgresHook(postgres_conn_id='postgres_lab')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # A tabela clima já existe (criada pelo sql/init.sql).
        # Inclui a coluna de precipitação, caso ainda não exista.
        cursor.execute("""
            ALTER TABLE clima ADD COLUMN IF NOT EXISTS precipitacao_mm NUMERIC(6, 2)
        """)
        
        # Inserir dados
        for reg in registros:
            cursor.execute(
                """INSERT INTO clima (hora, temperatura_c, precipitacao_mm, cidade)
                   VALUES (%s, %s, %s, %s)""",
                (reg['hora'], reg['temperatura_c'], reg['precipitacao_mm'], reg['cidade'])
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return len(registros)

    # Execução do pipeline - tudo linear
    dados_sp = fetch_sao_paulo()
    dados_rj = fetch_rio_de_janeiro()
    dados_bh = fetch_belo_horizonte()

    val_sp = validate_sao_paulo(dados_sp)
    val_rj = validate_rio_de_janeiro(dados_rj)
    val_bh = validate_belo_horizonte(dados_bh)

    transform_sp = transform_sao_paulo(dados_sp)
    transform_rj = transform_rio_de_janeiro(dados_rj)
    transform_bh = transform_belo_horizonte(dados_bh)

    val_sp >> transform_sp
    val_rj >> transform_rj
    val_bh >> transform_bh
    
    consolidado = consolidar(transform_sp, transform_rj, transform_bh)
    salvar_no_banco(consolidado)


# Instanciar o DAG
dag_instance = pipeline_clima()
