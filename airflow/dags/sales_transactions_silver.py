from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from jobs.batch_ingest_job import get_spark_session, stop_spark
from jobs.silver_stream import run_silver_cdc_upsert_stream

# =========================
# CONFIG
# =========================
TARGET_TABLE = "silver.sales.transactions"
KAFKA_TOPIC = "sales_server.sales.transactions"   # bronze ilə EYNİ topic
CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/silver/sales/transactions_cdc"

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

# Debezium raw tipləri - bronze DAG-dakı ROW_SCHEMA ilə EYNİ olmalıdır
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
    "CASE WHEN UPPER(src.status) = 'CANCELLED' "
    "THEN CAST(0 AS DECIMAL(10,2)) ELSE CAST(src.amount AS DECIMAL(10,2)) END AS net_amount",
]

DELETE_SELECT = ["CAST(src.transaction_id AS BIGINT) AS transaction_id"]

QUALITY_FILTER = (
    "amount > 0 AND quantity > 0 AND status IS NOT NULL "
    "AND UPPER(status) IN ('COMPLETED', 'PENDING', 'CANCELLED', 'NEW')"
)


# =========================
# SPARK INIT (yalnız silver catalog lazımdır, bronze-a ehtiyac yoxdur)
# =========================
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    silver_warehouse = Variable.get("SILVER_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")
    nessie_ref = Variable.get("NESSIE_BRANCH_SILVER", default_var="silver")

    return get_spark_session(
        app_name,
        {
            "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.silver.uri": nessie_uri,
            "spark.sql.catalog.silver.ref": nessie_ref,
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
    kafka_bootstrap = Variable.get("KAFKA_BOOTSTRAP_SERVERS")

    spark = get_spark("silver_sales_transactions")
    try:
        run_silver_cdc_upsert_stream(
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
            quality_filter_expr=QUALITY_FILTER,
            partition_by=PARTITION_BY,
            starting_offsets="earliest",
            timeout_seconds=20 * 60,
        )
    finally:
        stop_spark(spark)


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="silver_sales_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=20),
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
        execution_timeout=timedelta(minutes=25),
    )