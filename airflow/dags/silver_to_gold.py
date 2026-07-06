from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

DBT_CONTAINER = "dbt"
DBT_PROJECT_DIR = "/usr/app/dbt"

with DAG(
    dag_id="silver_to_gold_dbt",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=20),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "gold", "lakehouse"],
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    # gold, Spark-in yazdigi silver.sales.transactions-a ehtiyac duyur -
    # ona gore hemin DAG-in eyni execution_date-deki task-i bitene qeder gozleyir
    wait_for_silver_transactions = ExternalTaskSensor(
        task_id="wait_for_silver_transactions",
        external_dag_id="silver_sales_transactions",
        external_task_id="transform_transactions_to_silver",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=60 * 30,
        poke_interval=60,
        mode="reschedule",
    )

    # gold, dbt-nin qurdugu silver.sales.customers/products-a da ehtiyac duyur
    wait_for_dbt_silver = ExternalTaskSensor(
        task_id="wait_for_dbt_silver",
        external_dag_id="dbt_silver_customers_products",
        external_task_id="dbt_test_silver",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=60 * 30,
        poke_interval=60,
        mode="reschedule",
    )



    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"docker exec {DBT_CONTAINER} dbt run "
            f"--select gold --project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            f"docker exec {DBT_CONTAINER} dbt test "
            f"--select gold --project-dir {DBT_PROJECT_DIR}"
        ),
    )

    [wait_for_silver_transactions, wait_for_dbt_silver]  >> dbt_run_gold >> dbt_test_gold