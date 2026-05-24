from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_PATH = "/opt/airflow/projects/flight_tracking/dbt_project"
GE_PATH = "/opt/airflow/projects/flight_tracking/great_expectations"

MARTS_TABLES = [
    "RAW_MARTS.FACT_POSITIONS",
    "RAW_MARTS.DIM_VOLS",
    "RAW_MARTS.DIM_AEROPORTS",
]


def _snowflake_env():
    try:
        conn = BaseHook.get_connection("FT_snowflake_default")
        extra = conn.extra_dejson or {}
        return {
            "SNOWFLAKE_ACCOUNT": conn.host,
            "SNOWFLAKE_USER": conn.login,
            "SNOWFLAKE_PASSWORD": conn.password,
            "SNOWFLAKE_DATABASE": extra.get("database", "FLIGHT_TRACKING"),
            "SNOWFLAKE_SCHEMA": extra.get("schema", "RAW"),
            "SNOWFLAKE_WAREHOUSE": extra.get("warehouse", "COMPUTE_WH"),
            "SNOWFLAKE_ROLE": extra.get("role", "ACCOUNTADMIN"),
        }
    except Exception as e:
        raise Exception(
            f"❌ Impossible de récupérer la connection FT_snowflake_default : {e}"
        )


def _get_snowflake_conn():
    """Helper : retourne une connexion Snowflake prête à l'emploi."""
    import snowflake.connector

    conn = BaseHook.get_connection("FT_snowflake_default")
    extra = conn.extra_dejson or {}
    return snowflake.connector.connect(
        account=conn.host,
        user=conn.login,
        password=conn.password,
        role=extra.get("role", "ACCOUNTADMIN"),
        database=extra.get("database", "FLIGHT_TRACKING"),
        warehouse=extra.get("warehouse", "COMPUTE_WH"),
    )


def _log_row_counts():
    """Vérifie et logue le nombre de lignes dans chaque table mart."""
    con = _get_snowflake_conn()
    cur = con.cursor()

    print("Nombre de lignes par table marts :")
    print("─" * 45)

    anomalies = []
    for table in MARTS_TABLES:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table} : {count:,} lignes")

        if count == 0:
            anomalies.append(table)

    con.close()
    print("─" * 45)

    if anomalies:
        raise Exception(
            f"❌ Tables vides détectées : {anomalies}. "
            "Le pipeline a peut-être échoué silencieusement."
        )

    print("✅ Toutes les tables marts contiennent des données")


def _run_ge():
    """Lance le checkpoint Great Expectations et lève une exception si échec."""
    import os
    import great_expectations as gx

    conn = BaseHook.get_connection("FT_snowflake_default")
    extra = conn.extra_dejson or {}

    os.environ["SNOWFLAKE_ACCOUNT"] = conn.host
    os.environ["SNOWFLAKE_USER"] = conn.login
    os.environ["SNOWFLAKE_PASSWORD"] = conn.password
    os.environ["SNOWFLAKE_DATABASE"] = extra.get("database", "FLIGHT_TRACKING")
    os.environ["SNOWFLAKE_SCHEMA"] = extra.get("schema", "RAW")
    os.environ["SNOWFLAKE_WAREHOUSE"] = extra.get("warehouse", "COMPUTE_WH")
    os.environ["SNOWFLAKE_ROLE"] = extra.get("role", "ACCOUNTADMIN")

    ctx = gx.get_context(context_root_dir=GE_PATH)
    result = ctx.run_checkpoint(checkpoint_name="checkpoint_facts")

    if not result["success"]:
        raise Exception(f"❌ Great Expectations checkpoint failed : {result}")

    print("✅ Great Expectations checkpoint passed")


with DAG(
    dag_id="dag_dbt_transform_main",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "snowflake", "flight_tracking"],
) as dag:

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"cd {DBT_PATH} && "
            "dbt source freshness "
            "--target prod"
        ),
        env=_snowflake_env(),
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PATH} && "
            "dbt run --select +marts "
            "--target prod --threads 4"
        ),
        env=_snowflake_env(),
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PATH} && "
            "dbt test --select +marts "
            "--target prod --threads 4"
        ),
        env=_snowflake_env(),
        append_env=True,
    )

    log_counts = PythonOperator(
        task_id="log_row_counts",
        python_callable=_log_row_counts,
    )

    ge_task = PythonOperator(
        task_id="ge_checkpoint",
        python_callable=_run_ge,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PATH} && "
            "dbt docs generate --target prod"
        ),
        env=_snowflake_env(),
        append_env=True,
    )

    dbt_source_freshness >> dbt_run >> dbt_test >> log_counts >> ge_task >> dbt_docs
