"""
DAG d'ingestion temps réel OpenSky → Snowflake.
"""

from __future__ import annotations

import os
import sys
import json
import tempfile
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator

for _path in ("/opt/airflow/projects/flight_tracking", "/opt/airflow"):
    if _path not in sys.path:
        sys.path.insert(0, _path)

_GE_EVERY_N_RUNS = 6
_GE_SAMPLE_SIZE  = 500
_GE_ROOT         = "/opt/airflow/projects/flight_tracking/great_expectations"
_CHECKPOINT_NAME = "checkpoint_raw"


# Helpers 

def _load_env_vars() -> None:
    """
    Lit les Airflow Variables ET la Connection Snowflake,
    et les injecte dans os.environ pour les modules métier.
    """
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook

    def _get(key: str, default: str = "") -> str:
        try:
            return Variable.get(key) or default
        except Exception:
            return default

    # Credentials OpenSky from Airflow Variables 
    opensky_id     = _get("OPENSKY_CLIENT_ID")
    opensky_secret = _get("OPENSKY_CLIENT_SECRET")

    missing = []
    if not opensky_id:
        missing.append("OPENSKY_CLIENT_ID")
    if not opensky_secret:
        missing.append("OPENSKY_CLIENT_SECRET")
    if missing:
        raise ValueError(
            f"Airflow Variables manquantes : {missing}\n"
            "Crée-les dans Admin → Variables avec exactement ces noms :\n"
            "  OPENSKY_CLIENT_ID\n"
            "  OPENSKY_CLIENT_SECRET"
        )

    os.environ["OPENSKY_CLIENT_ID"]     = opensky_id
    os.environ["OPENSKY_CLIENT_SECRET"] = opensky_secret

    # Credentials Snowflake depuis Airflow Connection 
    conn  = BaseHook.get_connection("FT_snowflake_default")
    extra = conn.extra_dejson or {}

    os.environ["SNOWFLAKE_ACCOUNT"]   = conn.host or ""
    os.environ["SNOWFLAKE_USER"]      = conn.login or ""
    os.environ["SNOWFLAKE_PASSWORD"]  = conn.password or ""
    os.environ["SNOWFLAKE_DATABASE"]  = extra.get("database",  "FLIGHT_TRACKING")
    os.environ["SNOWFLAKE_SCHEMA"]    = extra.get("schema",    "RAW")
    os.environ["SNOWFLAKE_WAREHOUSE"] = extra.get("warehouse", "COMPUTE_WH")
    os.environ["SNOWFLAKE_ROLE"]      = extra.get("role",      "ACCOUNTADMIN")


def _push_positions_to_tmp(ti, positions: list[dict]) -> str:
    """Écrit les positions dans un fichier JSON temporaire et pousse le chemin en XCom."""
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        prefix="opensky_positions_",
        delete=False,
    ) as f:
        json.dump(positions, f)
        tmp_path = f.name

    ti.xcom_push(key="positions_path", value=tmp_path)
    ti.xcom_push(key="count", value=len(positions))
    return tmp_path


def _pull_positions(ti) -> list[dict] | None:
    """Lit les positions depuis le fichier temp référencé par XCom."""
    path = ti.xcom_pull(task_ids="fetch_opensky_states", key="positions_path")
    if not path or not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


# ── Callables des tâches ──────────────────────────────────────────────────────

def _fetch_opensky(**context) -> None:
    """Appelle l'API OpenSky et persiste le résultat sur le filesystem local."""
    from ingestion.opensky_client import OpenSkyClient
    _load_env_vars()

    client = OpenSkyClient(
        client_id=os.environ["OPENSKY_CLIENT_ID"],
        client_secret=os.environ["OPENSKY_CLIENT_SECRET"],
    )
    try:
        positions = client.get_states_france()
    finally:
        client.close()

    _push_positions_to_tmp(context["ti"], positions)


def _branch(**context) -> str:
    """Court-circuite le pipeline si l'API OpenSky renvoie 0 position."""
    count = context["ti"].xcom_pull(task_ids="fetch_opensky_states", key="count")
    return "validate_raw_data" if count and count > 0 else "skip_empty_response"


def _validate_ge(**context) -> None:
    """
    Validation Great Expectations
    """
    run_id: str = context.get("run_id", "")
    try:
        run_index = hash(run_id) % _GE_EVERY_N_RUNS
    except Exception:
        run_index = 0

    if run_index != 0:
        print(f"⏭️  GE ignoré ce run (index={run_index}, throttle=1/{_GE_EVERY_N_RUNS})")
        return

    try:
        import great_expectations as gx
        import pandas as pd
    except ImportError:
        print("⚠️  great_expectations non installé — validation ignorée")
        return

    positions = _pull_positions(context["ti"])
    if not positions:
        return

    sample = positions[:_GE_SAMPLE_SIZE]
    if len(positions) > _GE_SAMPLE_SIZE:
        print(f"ℹ️  GE : échantillon de {_GE_SAMPLE_SIZE}/{len(positions)} positions utilisé")

    df = pd.DataFrame(sample)

    try:
        _load_env_vars()
        ctx = gx.get_context(context_root_dir=_GE_ROOT)
        datasource = ctx.sources.add_or_update_pandas(name="opensky_raw")
        asset = datasource.add_dataframe_asset(name="raw_positions_batch")
        batch_request = asset.build_batch_request(dataframe=df)
        result = ctx.run_checkpoint(
            checkpoint_name=_CHECKPOINT_NAME,
            batch_request=batch_request,
        )
        if not result["success"]:
            failed = sum(
                1
                for r in result.run_results.values()
                for er in r["validation_result"]["results"]
                if not er["success"]
            )
            print(f"⚠️  GE : {failed} expectations échouées (ingestion continue)")
        else:
            print("✅ GE : toutes les validations passées")
    except Exception as e:
        print(f"⚠️  GE erreur non bloquante : {e}")


def _load_snowflake(**context) -> None:
    """Charge les positions dans Snowflake via SnowflakeLoader."""
    from ingestion.snowflake_loader import SnowflakeLoader
    _load_env_vars()

    positions = _pull_positions(context["ti"])
    if not positions:
        print("ℹ️  Aucune position à charger.")
        return

    with SnowflakeLoader() as loader:
        stats = loader.upsert_positions(positions)

    context["ti"].xcom_push(key="load_stats", value=stats)

    # Nettoyage du fichier temporaire
    tmp_path = context["ti"].xcom_pull(task_ids="fetch_opensky_states", key="positions_path")
    if tmp_path and os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except OSError as e:
            print(f"⚠️  Impossible de supprimer le fichier temp {tmp_path} : {e}")


def _log_stats(**context) -> None:
    """Affiche un résumé structuré de l'ingestion."""
    count = context["ti"].xcom_pull(task_ids="fetch_opensky_states", key="count") or 0
    stats = context["ti"].xcom_pull(task_ids="load_to_snowflake",   key="load_stats") or {}

    skipped    = stats.get("skipped", 0) or 0
    total      = stats.get("total",   1) or 1
    dedup_rate = skipped / total * 100

    now = datetime.now(timezone.utc).isoformat()

    print("=" * 55)
    print(f" STATS — {now}")
    print(f"   Avions récupérés   : {count}")
    print(f"   Insérés Snowflake  : {stats.get('inserted', 'N/A')}")
    print(f"   Doublons ignorés   : {skipped}")
    print(f"   Taux déduplication : {dedup_rate:.1f}%")
    print("=" * 55)


# ── Définition du DAG ─────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(seconds=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_ingest_flights",
    description="Ingestion temps réel OpenSky → Snowflake (toutes les 10 min)",
    default_args=DEFAULT_ARGS,
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["flight-tracking", "ingestion", "opensky", "snowflake"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_opensky_states",
        python_callable=_fetch_opensky,
    )

    t_branch = BranchPythonOperator(
        task_id="check_data_not_empty",
        python_callable=_branch,
    )

    t_skip = EmptyOperator(task_id="skip_empty_response")

    t_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=_validate_ge,
    )

    t_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=_load_snowflake,
    )

    t_stats = PythonOperator(
        task_id="log_ingestion_stats",
        python_callable=_log_stats,
        trigger_rule="all_done",
    )

    t_fetch >> t_branch
    t_branch >> [t_validate, t_skip]
    t_validate >> t_load >> t_stats
    t_skip >> t_stats
