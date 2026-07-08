from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from common_dataset import SILVER_TRANSACTIONS_DS, SILVER_READY


DBT_PROJECT_DIR = "/opt/airflow/dbt/dbt_datalakehouse"
DBT_PROFILES_DIR = "/opt/airflow/dbt/dbt_datalakehouse"

with DAG(
    dag_id="silver_to_gold_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=[
        SILVER_READY
    ],
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "gold", "lakehouse"],
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    dbt_run_gold = BashOperator(
    task_id="dbt_run_gold",
    bash_command=f"""
    dbt run \
        --select gold \
        --project-dir {DBT_PROJECT_DIR} \
        --profiles-dir {DBT_PROFILES_DIR}
    """,
)

    dbt_test_gold = BashOperator(
    task_id="dbt_test_gold",
    bash_command=f"""
    dbt test \
        --select gold \
        --project-dir {DBT_PROJECT_DIR} \
        --profiles-dir {DBT_PROFILES_DIR}
    """,
)

    dbt_run_gold >> dbt_test_gold