from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from common_dataset import BRONZE_READY, SILVER_READY
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/dbt_datalakehouse"
DBT_PROFILES_DIR = "/opt/airflow/dbt/dbt_datalakehouse"

with DAG(
    dag_id="bronze_to_silver_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=[BRONZE_READY],
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "silver", "lakehouse"],
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        dbt deps \
            --project-dir {DBT_PROJECT_DIR} \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"""
        dbt run \
            --select silver \
            --project-dir {DBT_PROJECT_DIR} \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"""
        dbt test \
            --select silver \
            --project-dir {DBT_PROJECT_DIR} \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
        outlets=[SILVER_READY]
    )

    dbt_deps >> dbt_run_silver >> dbt_test_silver