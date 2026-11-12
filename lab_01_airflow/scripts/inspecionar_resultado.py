#!/usr/bin/env python3
"""
=============================================================================
scripts/inspecionar_resultado.py
Aula 1 — Orquestração de Workflows

Consulta o PostgreSQL do lab para verificar o que o DAG inseriu.
Útil para demonstração na UI durante o lab.

USO (com o ambiente Docker rodando):
  python scripts/inspecionar_resultado.py
  python scripts/inspecionar_resultado.py --data 2024-03-15

DEPENDÊNCIAS:
  pip install psycopg2-binary pandas tabulate
=============================================================================
"""

import argparse
import logging
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Configuração de conexão com o postgres-lab (porta mapeada no docker-compose)
DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,           # mapeado no docker-compose
    "database": "labdb",
    "user":     "lab",
    "password": "lab123",
}


def conectar():
    import psycopg2
    return psycopg2.connect(**DB_CONFIG)


def listar_runs(conn) -> None:
    """Mostra as runs do DAG que inseriram dados."""
    import pandas as pd
    df = pd.read_sql(
        """
        SELECT dag_run_id, COUNT(*) AS registros,
               MIN(inserido_em) AS primeiro_insert,
               MAX(inserido_em) AS ultimo_insert
        FROM clima
        GROUP BY dag_run_id
        ORDER BY primeiro_insert DESC
        LIMIT 10
        """,
        conn,
    )
    print("\n=== RUNS REGISTRADAS ===")
    if df.empty:
        print("Nenhum dado encontrado. O DAG já foi executado?")
    else:
        print(df.to_string(index=False))


def resumo_por_cidade(conn, data_filtro: str) -> None:
    """Exibe resumo de temperatura por cidade para uma data."""
    import pandas as pd
    df = pd.read_sql(
        """
        SELECT cidade,
               COUNT(*) AS horas,
               ROUND(MIN(temperatura_c)::numeric, 1) AS temp_min,
               ROUND(MAX(temperatura_c)::numeric, 1) AS temp_max,
               ROUND(AVG(temperatura_c)::numeric, 1) AS temp_media
        FROM clima
        WHERE DATE(hora) = %s
        GROUP BY cidade
        ORDER BY cidade
        """,
        conn,
        params=(data_filtro,),
    )
    print(f"\n=== RESUMO POR CIDADE — {data_filtro} ===")
    if df.empty:
        print(f"Sem dados para {data_filtro}.")
    else:
        print(df.to_string(index=False))


def serie_horaria(conn, cidade: str, data_filtro: str) -> None:
    """Exibe série horária de temperatura para uma cidade."""
    import pandas as pd
    df = pd.read_sql(
        """
        SELECT hora, temperatura_c
        FROM clima
        WHERE cidade = %s AND DATE(hora) = %s
        ORDER BY hora
        """,
        conn,
        params=(cidade, data_filtro),
    )
    print(f"\n=== SÉRIE HORÁRIA — {cidade} — {data_filtro} ===")
    if df.empty:
        print("Sem dados.")
    else:
        print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Inspeciona dados inseridos pelo DAG")
    parser.add_argument("--data", default=str(date.today()), help="Data no formato YYYY-MM-DD")
    parser.add_argument("--cidade", default="São Paulo", help="Nome da cidade para série horária")
    args = parser.parse_args()

    try:
        conn = conectar()
        log.info("Conectado ao banco labdb em localhost:5433")
    except Exception as e:
        log.error("Não foi possível conectar ao banco: %s", e)
        log.error("Verifique se o ambiente Docker está rodando (docker compose ps)")
        return

    with conn:
        listar_runs(conn)
        resumo_por_cidade(conn, args.data)
        serie_horaria(conn, args.cidade, args.data)

    conn.close()
    log.info("Consulta concluída.")


if __name__ == "__main__":
    main()
