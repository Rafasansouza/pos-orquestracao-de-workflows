"""
=============================================================================
DAG: clima_etl
Aula 1 — Orquestração de Workflows | Lab Prático

Pipeline ETL completo usando TaskFlow API:
  1. buscar_clima()    → busca dados de temperatura horária da Open-Meteo API
  2. transformar()     → limpa e normaliza com Pandas
  3. carregar()        → insere no PostgreSQL via PostgresHook

Agendamento: diário às 6h (UTC)
Backfill: desativado (catchup=False)

Configurações para explorar na UI:
  - Gantt chart: Admin → DAGs → clima_etl → (selecionar um run) → Gantt
  - XComs:       Admin → XComs
  - Logs:        clicar em qualquer task → Log
=============================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

log = logging.getLogger(__name__)

# =============================================================================
# Configurações do DAG (centralizadas para fácil ajuste)
# =============================================================================

# Cidades monitoradas: (nome, latitude, longitude)
CIDADES = [
    ("São Paulo",     -23.5505, -46.6333),
    ("Rio de Janeiro", -22.9068, -43.1729),
    ("Brasília",       -15.7801, -47.9292),
]

POSTGRES_CONN_ID = "postgres_lab"      # Connection criada pelo airflow-init

DEFAULT_ARGS = {
    "owner": "lab",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2min → 4min → 8min
    "email_on_failure": False,
    "email_on_retry": False,
}


# =============================================================================
# DAG
# =============================================================================

@dag(
    dag_id="clima_etl",
    description="ETL: Open-Meteo API → Pandas → PostgreSQL (lab Aula 1)",
    schedule="0 6 * * *",                          # todo dia às 6h UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,                                  # não executa runs passadas
    default_args=DEFAULT_ARGS,
    tags=["lab", "aula-1", "etl", "airflow"],
    doc_md=__doc__,
    max_active_runs=1,
)
def clima_etl():
    """Pipeline ETL de dados climáticos — Lab Aula 1."""

    # =========================================================================
    # TASK 1: Buscar dados da API
    # =========================================================================
    @task(task_id="buscar_clima")
    def buscar_clima(**context) -> list[dict]:
        """
        Busca temperatura horária das próximas 24h para cada cidade
        usando a Open-Meteo API (gratuita, sem autenticação).

        Retorna:
            list[dict] com os dados brutos por cidade
        """
        import requests  # importar dentro da task = isolamento correto

        resultados = []

        for cidade, lat, lon in CIDADES:
            log.info("Buscando dados para %s (%.4f, %.4f)", cidade, lat, lon)

            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m",
                "start_date": context["ds"],     # dia da execução (idempotência)
                "end_date": context["ds"],
                "timezone": "America/Sao_Paulo",
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()         # lança exceção em erro HTTP

            dados = response.json()
            dados["_cidade"] = cidade           # enriquecer com metadado
            resultados.append(dados)

            log.info(
                "✓ %s — %d horas coletadas",
                cidade,
                len(dados["hourly"]["time"]),
            )

        log.info("Total de cidades coletadas: %d", len(resultados))
        return resultados

    # =========================================================================
    # TASK 2: Transformar e limpar
    # =========================================================================
    @task(task_id="transformar")
    def transformar(dados_brutos: list[dict]) -> list[dict]:
        """
        Normaliza e limpa os dados usando Pandas.

        Operações:
          - Parseia timestamps ISO 8601
          - Remove registros com temperatura nula
          - Arredonda temperatura para 2 casas decimais
          - Estrutura para inserção no banco

        Retorna:
            list[dict] pronta para INSERT
        """
        import pandas as pd

        registros_finais = []

        for dados in dados_brutos:
            cidade = dados["_cidade"]
            lat = dados["latitude"]
            lon = dados["longitude"]

            df = pd.DataFrame({
                "hora": dados["hourly"]["time"],
                "temperatura_c": dados["hourly"]["temperature_2m"],
            })

            # Limpeza
            df["hora"] = pd.to_datetime(df["hora"])
            df["temperatura_c"] = pd.to_numeric(df["temperatura_c"], errors="coerce")
            df = df.dropna(subset=["temperatura_c"])
            df["temperatura_c"] = df["temperatura_c"].round(2)
            # XCom serializa em JSON: Timestamp do pandas não é suportado -> string ISO
            df["hora"] = df["hora"].dt.strftime("%Y-%m-%dT%H:%M:%S")

            # Adicionar metadados
            df["cidade"] = cidade
            df["latitude"] = lat
            df["longitude"] = lon

            registros_finais.extend(df.to_dict(orient="records"))
            log.info("✓ %s — %d registros válidos após limpeza", cidade, len(df))

        log.info("Total de registros a inserir: %d", len(registros_finais))
        return registros_finais

    # =========================================================================
    # TASK 3: Carregar no PostgreSQL
    # =========================================================================
    @task(task_id="carregar")
    def carregar(registros: list[dict], **context) -> int:
        """
        Insere os registros no PostgreSQL usando PostgresHook.

        Features demonstradas:
          - PostgresHook (abstração de Connection)
          - Idempotência via DELETE antes do INSERT
          - dag_run_id nos registros para rastreabilidade

        Retorna:
            Número de registros inseridos
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        run_id = context["run_id"]
        data_execucao = context["ds"]          # YYYY-MM-DD da execução

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            # Idempotência: apagar registros desta data antes de reinserir
            # (seguro para reexecutar sem duplicar)
            cur.execute(
                "DELETE FROM clima WHERE DATE(hora) = %s",
                (data_execucao,),
            )
            deletados = cur.rowcount
            if deletados > 0:
                log.info("Idempotência: %d registros anteriores removidos para %s", deletados, data_execucao)

            # INSERT em lote
            insert_sql = """
                INSERT INTO clima (cidade, latitude, longitude, hora, temperatura_c, dag_run_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            valores = [
                (
                    r["cidade"],
                    r["latitude"],
                    r["longitude"],
                    r["hora"],
                    r["temperatura_c"],
                    run_id,
                )
                for r in registros
            ]
            cur.executemany(insert_sql, valores)
            conn.commit()

            total = len(valores)
            log.info("✓ %d registros inseridos na tabela 'clima'", total)
            log.info("  run_id: %s | data: %s", run_id, data_execucao)

            return total

        except Exception as e:
            conn.rollback()
            log.error("Erro ao inserir no banco: %s", e)
            raise
        finally:
            cur.close()
            conn.close()

    # =========================================================================
    # TASK 4: Verificar resultado (bonus — demonstra task de validação)
    # =========================================================================
    @task(task_id="verificar")
    def verificar(total_inserido: int, **context) -> None:
        """
        Valida que os dados foram inseridos corretamente.
        Task de qualidade de dados — boa prática em pipelines de produção.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        data_execucao = context["ds"]
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Consulta de verificação
        resultado = hook.get_first(
            "SELECT COUNT(*), ROUND(AVG(temperatura_c)::numeric, 2) FROM clima WHERE DATE(hora) = %s",
            parameters=(data_execucao,),
        )
        count, temp_media = resultado

        log.info("=== VERIFICAÇÃO ===")
        log.info("Registros na tabela para %s: %d (esperado: %d)", data_execucao, count, total_inserido)
        log.info("Temperatura média geral: %.2f°C", temp_media or 0)

        if count != total_inserido:
            raise ValueError(
                f"Verificação falhou! Inseridos={total_inserido}, encontrados={count}"
            )

        log.info("✓ Verificação OK!")

    # =========================================================================
    # Pipeline — encadeamento das tasks (TaskFlow API)
    # A ordem é definida pelo fluxo de dados entre as funções
    # =========================================================================
    dados_brutos = buscar_clima()
    registros = transformar(dados_brutos)
    total = carregar(registros)
    verificar(total)

    dados_brutos >> registros >> total >> verificar(total)  # encadeamento explícito (opcional)

# Instanciar o DAG (obrigatório no módulo)
dag_instance = clima_etl()
