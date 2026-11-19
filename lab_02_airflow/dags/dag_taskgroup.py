"""
DAG refatorado com TaskGroups — mesmo pipeline do pipeline_clima_monolitico,
agora SEM código repetido.

Contraste com o monolítico:
  - 1 função de busca, 1 de validação e 1 de transformação (antes: 3 de cada)
  - As cidades ficam em uma lista: incluir uma cidade = incluir uma linha
  - TaskGroups agrupam as etapas na UI: busca, validacao, transformacao
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


# Antes: latitude/longitude espalhadas em 3 funções copiadas
CIDADES = [
    {'id': 'sao_paulo',      'nome': 'São Paulo',      'latitude': -23.5505, 'longitude': -46.6333},
    {'id': 'rio_de_janeiro', 'nome': 'Rio de Janeiro', 'latitude': -22.9068, 'longitude': -43.1729},
    {'id': 'belo_horizonte', 'nome': 'Belo Horizonte', 'latitude': -19.8267, 'longitude': -43.9345},
]


@dag(
    dag_id='pipeline_clima_taskgroup',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['taskgroup', 'aula2'],
)
def pipeline_clima():
    """Pipeline de clima de 3 cidades - REFATORADO com TaskGroups"""

    @task
    def buscar(latitude, longitude):
        """Busca dados de clima de UMA cidade (serve para todas)"""
        import requests

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,precipitation",
            "forecast_days": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @task
    def validar(dados):
        """Mesma validação do monolítico — escrita UMA vez"""
        if 'hourly' not in dados:
            raise ValueError("Dados incompletos: falta 'hourly'")

        if 'temperature_2m' not in dados['hourly']:
            raise ValueError("Dados incompletos: falta 'temperature_2m'")

        temps = dados['hourly']['temperature_2m']
        if len(temps) == 0:
            raise ValueError("Lista de temperaturas vazia")

        if max(temps) > 60 or min(temps) < -60:
            raise ValueError("Temperatura fora do range esperado")

        return dados  # repassa os dados validados para a transformação

    @task
    def transformar(dados, cidade):
        """Mesma transformação do monolítico — escrita UMA vez"""
        import pandas as pd

        df = pd.DataFrame({
            'hora': dados['hourly']['time'],
            'temperatura_c': dados['hourly']['temperature_2m'],
            'precipitacao_mm': dados['hourly']['precipitation'],
            'cidade': cidade
        })

        df['hora'] = pd.to_datetime(df['hora'])
        df = df.dropna()
        df['temperatura_c'] = df['temperatura_c'].round(2)
        df['precipitacao_mm'] = df['precipitacao_mm'].round(2)
        # XCom serializa em JSON: Timestamp do pandas não é suportado -> string ISO
        df['hora'] = df['hora'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        return df.to_dict(orient='records')

    @task
    def consolidar(listas):
        """Junta os registros de todas as cidades em uma única lista"""
        todos = []
        for registros in listas:
            todos.extend(registros)
        return todos

    @task
    def salvar_no_banco(registros):
        """Salva dados consolidados no PostgreSQL (igual ao monolítico)"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id='postgres_lab')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # A tabela clima já existe (criada pelo sql/init.sql).
        # Inclui a coluna de precipitação, caso ainda não exista.
        cursor.execute("""
            ALTER TABLE clima ADD COLUMN IF NOT EXISTS precipitacao_mm NUMERIC(6, 2)
        """)

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

    # ── Execução do pipeline ────────────────────────────────────────────────
    # Cada TaskGroup cria uma task por cidade usando a MESMA função.
    # .override(task_id=...) dá um nome diferente para cada cópia.
    # Na UI os nomes ficam: busca.sao_paulo, validacao.sao_paulo, ...

    dados = {}
    with TaskGroup('busca'):
        for cidade in CIDADES:
            dados[cidade['id']] = buscar.override(task_id=cidade['id'])(
                cidade['latitude'], cidade['longitude']
            )

    validados = {}
    with TaskGroup('validacao'):
        for cidade in CIDADES:
            validados[cidade['id']] = validar.override(task_id=cidade['id'])(
                dados[cidade['id']]
            )

    transformados = []
    with TaskGroup('transformacao'):
        for cidade in CIDADES:
            transformados.append(
                transformar.override(task_id=cidade['id'])(
                    validados[cidade['id']], cidade['nome']
                )
            )

    consolidado = consolidar(transformados)
    salvar_no_banco(consolidado)


# Instanciar o DAG
dag_instance = pipeline_clima()
