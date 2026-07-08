from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from jobs.batch_ingest_job import get_spark_session, stop_spark
from jobs.bronze_append_job import run_bronze_append_stream

from common_dataset import BRONZE_TRANSACTIONS_DS

# =========================
# CONFIG
# =========================
TARGET_TABLE = "bronze.sales.transactions"
KAFKA_TOPIC = "sales_server.sales.transactions"
CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/bronze/sales/transactions" 

TARGET_SCHEMA_SQL = """
transaction_id BIGINT,
customer_id INT,
product_id INT,
quantity INT,
amount DECIMAL(10,2),
transaction_date TIMESTAMP,
status STRING
""".strip()

PARTITION_BY = "days(transaction_date)"
PRIMARY_KEYS = ["transaction_id"]

ROW_SCHEMA = [
    {"name": "transaction_id", "type": "long"},
    {"name": "customer_id", "type": "int"},
    {"name": "product_id", "type": "int"},
    {"name": "quantity", "type": "int"},
    {"name": "amount", "type": "double"},
    {"name": "transaction_date", "type": "long"},
    {"name": "status", "type": "string"},
]

UPSERT_SELECT = [
    "CAST(src.transaction_id AS BIGINT) AS transaction_id",
    "CAST(src.customer_id AS INT) AS customer_id",
    "CAST(src.product_id AS INT) AS product_id",
    "CAST(src.quantity AS INT) AS quantity",
    "CAST(src.amount AS DECIMAL(10,2)) AS amount",
    "timestamp_micros(src.transaction_date) AS transaction_date",
    "src.status",
]

DELETE_SELECT = ["CAST(src.transaction_id AS BIGINT) AS transaction_id"]


# =========================
# SPARK INIT
# =========================
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")
    nessie_ref = Variable.get("NESSIE_BRANCH_BRONZE", default_var="bronze")

    return get_spark_session(
        app_name,
        {
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.bronze.uri": nessie_uri,
            "spark.sql.catalog.bronze.ref": nessie_ref,
            "spark.sql.catalog.bronze.warehouse": bronze_warehouse,
            "spark.sql.catalog.bronze.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.bronze.s3.endpoint": minio_endpoint,
            "spark.sql.catalog.bronze.s3.path-style-access": "true",
            "spark.sql.catalog.bronze.s3.access-key-id": minio_access_key,
            "spark.sql.catalog.bronze.s3.secret-access-key": minio_secret_key,
        },
        spark_extensions=(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ),
    )


# =========================
# TASK
# =========================
def ingest_transactions_cdc():
    kafka_bootstrap = Variable.get("KAFKA_BOOTSTRAP_SERVERS")

    spark = get_spark("cdc_sales_transactions")
    try:
        run_bronze_append_stream(
            spark=spark,
            kafka_bootstrap=kafka_bootstrap,
            kafka_topic=KAFKA_TOPIC,
            checkpoint_location=CHECKPOINT_LOCATION,
            target_table=TARGET_TABLE,
            schema_sql=TARGET_SCHEMA_SQL,
            row_schema_fields=ROW_SCHEMA,
            pk_cols=PRIMARY_KEYS,
            upsert_select=UPSERT_SELECT,
            delete_select=DELETE_SELECT,
            partition_by=PARTITION_BY,
            starting_offsets="earliest",
            timeout_seconds=20 * 60,
        )
    finally:
        stop_spark(spark)


# =========================
# DAG
# =========================
with DAG(
    dag_id="bronze_sales_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=2),  
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "cdc", "lakehouse"],
    default_args={
        "owner": "lakehouse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    ingest = PythonOperator(
        task_id="ingest_transactions_cdc",
        python_callable=ingest_transactions_cdc,
        execution_timeout=timedelta(minutes=10),
        outlets=[BRONZE_TRANSACTIONS_DS],
        pool="spark_driver_pool",
    )