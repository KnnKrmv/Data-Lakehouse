from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from jobs.batch_ingest_job import get_spark_session, stop_spark
from jobs.silver_stream_job import run_silver_table_upsert_stream

from common_dataset import BRONZE_TRANSACTIONS_DS, SILVER_TRANSACTIONS_DS

# =========================
# CONFIG
# =========================
SOURCE_TABLE = "bronze.sales.transactions"
TARGET_TABLE = "silver.sales.transactions"
CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/silver/sales/transactions_from_bronze"

TARGET_SCHEMA_SQL = """
transaction_id BIGINT,
customer_id INT,
product_id INT,
quantity INT,
amount DECIMAL(10,2),
transaction_date TIMESTAMP,
status STRING,
net_amount DECIMAL(10,2)
""".strip()

PARTITION_BY = "days(transaction_date)"
PRIMARY_KEYS = ["transaction_id"]

# Bronze artıq düz sütunludur (src.* prefiksi yoxdur)
UPSERT_SELECT = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "amount",
    "transaction_date",
    "status",
    "CASE WHEN UPPER(status) = 'CANCELLED' "
    "THEN CAST(0 AS DECIMAL(10,2)) ELSE amount END AS net_amount",
]

DELETE_SELECT = ["transaction_id"]

QUALITY_FILTER = (
    "amount > 0 AND quantity > 0 AND status IS NOT NULL "
    "AND UPPER(status) IN ('COMPLETED', 'PENDING', 'CANCELLED', 'NEW')"
)


# =========================
# SPARK INIT (bronze + silver catalog lazımdır)
# =========================
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")
    silver_warehouse = Variable.get("SILVER_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")
    nessie_ref_bronze = Variable.get("NESSIE_BRANCH_BRONZE", default_var="bronze")
    nessie_ref_silver = Variable.get("NESSIE_BRANCH_SILVER", default_var="silver")

    return get_spark_session(
        app_name,
        {
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.bronze.uri": nessie_uri,
            "spark.sql.catalog.bronze.ref": nessie_ref_bronze,
            "spark.sql.catalog.bronze.warehouse": bronze_warehouse,
            "spark.sql.catalog.bronze.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.bronze.s3.endpoint": minio_endpoint,
            "spark.sql.catalog.bronze.s3.path-style-access": "true",
            "spark.sql.catalog.bronze.s3.access-key-id": minio_access_key,
            "spark.sql.catalog.bronze.s3.secret-access-key": minio_secret_key,

            "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.silver.uri": nessie_uri,
            "spark.sql.catalog.silver.ref": nessie_ref_silver,
            "spark.sql.catalog.silver.warehouse": silver_warehouse,
            "spark.sql.catalog.silver.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.silver.s3.endpoint": minio_endpoint,
            "spark.sql.catalog.silver.s3.path-style-access": "true",
            "spark.sql.catalog.silver.s3.access-key-id": minio_access_key,
            "spark.sql.catalog.silver.s3.secret-access-key": minio_secret_key,
        },
        spark_extensions=(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ),
    )


# =========================
# TASK
# =========================
def transform_transactions_to_silver():
    spark = get_spark("silver_sales_transactions")
    try:
        run_silver_table_upsert_stream(
            spark=spark,
            source_table=SOURCE_TABLE,
            checkpoint_location=CHECKPOINT_LOCATION,
            target_table=TARGET_TABLE,
            schema_sql=TARGET_SCHEMA_SQL,
            pk_cols=PRIMARY_KEYS,
            upsert_select=UPSERT_SELECT,
            delete_select=DELETE_SELECT,
            quality_filter_expr=QUALITY_FILTER,
            partition_by=PARTITION_BY,
            timeout_seconds=20 * 60,
        )
    finally:
        stop_spark(spark)


# =========================
# DAG
# =========================
with DAG(
    dag_id="silver_sales_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=[BRONZE_TRANSACTIONS_DS],  # Bronze bitəndə tetiklenir
    catchup=False,
    max_active_runs=1,
    tags=["silver", "cdc", "lakehouse"],
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    transform = PythonOperator(
        task_id="transform_transactions_to_silver",
        python_callable=transform_transactions_to_silver,
        execution_timeout=timedelta(minutes=10),
        outlets=[SILVER_TRANSACTIONS_DS],
        pool="spark_driver_pool",
    )