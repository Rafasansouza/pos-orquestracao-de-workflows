#!/usr/bin/env python3
"""
=============================================================================
scripts/testar_dag_local.py
Aula 1 — Orquestração de Workflows

Testa cada função do DAG de forma isolada, sem precisar do Airflow.
Útil para debugar a lógica antes de subir o ambiente Docker.

USO:
  python scripts/testar_dag_local.py              → testa tudo
  python scripts/testar_dag_local.py --step fetch → testa só o fetch
  python scripts/testar_dag_local.py --step all   → testa pipeline completo

DEPENDÊNCIAS:
  pip install requests pandas
=============================================================================
"""

import argparse
import json
import logging
import sys
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CIDADES = [
    ("São Paulo",      -23.5505, -46.6333),
    ("Rio de Janeiro", -22.9068, -43.1729),
    ("Brasília",       -15.7801, -47.9292),
]


# =============================================================================
# Funções espelhadas do DAG (sem decoradores Airflow)
# =============================================================================

def buscar_clima() -> list[dict]:
    """Replica a task buscar_clima do DAG."""
    import requests

    resultados = []
    for cidade, lat, lon in CIDADES:
        log.info("Buscando dados para %s...", cidade)
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m",
                "forecast_days": 1,
                "timezone": "America/Sao_Paulo",
            },
            timeout=30,
        )
        r.raise_for_status()
        dados = r.json()
        dados["_cidade"] = cidade
        resultados.append(dados)
        log.info("  ✓ %d horas coletadas", len(dados["hourly"]["time"]))
    return resultados


def transformar(dados_brutos: list[dict]) -> list[dict]:
    """Replica a task transformar do DAG."""
    import pandas as pd

    registros = []
    for dados in dados_brutos:
        df = pd.DataFrame({
            "hora": dados["hourly"]["time"],
            "temperatura_c": dados["hourly"]["temperature_2m"],
        })
        df["hora"] = pd.to_datetime(df["hora"])
        df["temperatura_c"] = pd.to_numeric(df["temperatura_c"], errors="coerce")
        df = df.dropna(subset=["temperatura_c"])
        df["temperatura_c"] = df["temperatura_c"].round(2)
        df["cidade"] = dados["_cidade"]
        df["latitude"] = dados["latitude"]
        df["longitude"] = dados["longitude"]
        registros.extend(df.to_dict(orient="records"))
        log.info("  ✓ %s: %d registros válidos", dados["_cidade"], len(df))

    return registros


def mostrar_preview(registros: list[dict], n: int = 5) -> None:
    """Exibe os primeiros N registros de forma legível."""
    import pandas as pd

    df = pd.DataFrame(registros)
    df["hora"] = pd.to_datetime(df["hora"])
    print("\n" + "=" * 60)
    print(f"PREVIEW — primeiros {n} registros:")
    print("=" * 60)
    print(df[["cidade", "hora", "temperatura_c"]].head(n).to_string(index=False))
    print(f"\nTotal de registros: {len(df)}")
    print("\nResumo por cidade:")
    print(
        df.groupby("cidade")["temperatura_c"]
        .agg(["min", "max", "mean"])
        .round(2)
        .rename(columns={"min": "Mín (°C)", "max": "Máx (°C)", "mean": "Média (°C)"})
        .to_string()
    )
    print("=" * 60 + "\n")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Testa o DAG clima_etl localmente")
    parser.add_argument(
        "--step",
        choices=["fetch", "transform", "all"],
        default="all",
        help="Qual etapa testar (padrão: all)",
    )
    args = parser.parse_args()

    try:
        if args.step in ("fetch", "all"):
            log.info("=== STEP 1: buscar_clima ===")
            dados_brutos = buscar_clima()
            log.info("Dados brutos coletados: %d cidades", len(dados_brutos))

            # Salvar para inspeção
            with open("/tmp/dados_brutos.json", "w") as f:
                json.dump(dados_brutos, f, indent=2, default=str)
            log.info("Dados brutos salvos em /tmp/dados_brutos.json")

        if args.step in ("transform", "all"):
            if args.step == "transform":
                # Carregar dados do step anterior
                with open("/tmp/dados_brutos.json") as f:
                    dados_brutos = json.load(f)

            log.info("=== STEP 2: transformar ===")
            registros = transformar(dados_brutos)
            mostrar_preview(registros)

            with open("/tmp/registros.json", "w") as f:
                json.dump(registros, f, indent=2, default=str)
            log.info("Registros transformados salvos em /tmp/registros.json")

        log.info("✓ Teste concluído com sucesso!")
        log.info("  Para carregar no banco, suba o ambiente Docker e execute o DAG na UI.")

    except ImportError as e:
        log.error("Dependência faltando: %s", e)
        log.error("Instale com: pip install requests pandas")
        sys.exit(1)
    except Exception as e:
        log.error("Erro durante o teste: %s", e)
        raise


if __name__ == "__main__":
    main()
