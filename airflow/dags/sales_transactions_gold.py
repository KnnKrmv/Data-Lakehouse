from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from jobs.batch_ingest_job import get_spark_session, stop_spark
from jobs.gold_stream import run_gold_fact_stream

# =========================
# CONFIG
# =========================
SOURCE_TABLE = "silver.sales.transactions"
TARGET_TABLE = "gold.sales.transactions"

CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/gold/sales/transactions_fact"

PRIMARY_KEYS = ["transaction_id"]

# =========================
# SPARK INIT (GOLD CATALOG)
# =========================
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    gold_warehouse = Variable.get("GOLD_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")
    nessie_ref = Variable.get("NESSIE_BRANCH_GOLD", default_var="gold")

    return get_spark_session(
        app_name,
        {
            # 🔥 GOLD CATALOG
            "spark.sql.catalog.gold": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.gold.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.gold.uri": nessie_uri,
            "spark.sql.catalog.gold.ref": nessie_ref,
            "spark.sql.catalog.gold.warehouse": gold_warehouse,

            # S3 / MinIO
            "spark.sql.catalog.gold.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.gold.s3.endpoint": minio_endpoint,
            "spark.sql.catalog.gold.s3.path-style-access": "true",
            "spark.sql.catalog.gold.s3.access-key-id": minio_access_key,
            "spark.sql.catalog.gold.s3.secret-access-key": minio_secret_key,
        },
        spark_extensions=(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ),
    )


# =========================
# TASK
# =========================
def transform_transactions_to_gold():
    kafka_bootstrap = Variable.get("KAFKA_BOOTSTRAP_SERVERS")

    spark = get_spark("gold_sales_transactions")

    try:
        run_gold_fact_stream(
            spark=spark,
            source_table=SOURCE_TABLE,
            customers_table="silver.sales.customers",
            products_table="silver.sales.products",
            target_table=TARGET_TABLE,
            checkpoint_location=CHECKPOINT_LOCATION,
        )
    finally:
        stop_spark(spark)


# =========================
# DAG
# =========================
with DAG(
    dag_id="gold_sales_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=20),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "fact", "iceberg"],
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    transform = PythonOperator(
        task_id="transform_transactions_to_gold",
        python_callable=transform_transactions_to_gold,
        execution_timeout=timedelta(minutes=25),
    )