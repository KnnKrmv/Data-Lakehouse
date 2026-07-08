from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from jobs.batch_ingest_job import get_spark_session, stop_spark
from jobs.gold_stream_job import run_gold_fact_merge

from common_dataset import SILVER_TRANSACTIONS_DS, GOLD_TRANSACTIONS_DS

# =========================
# CONFIG
# =========================
SOURCE_TABLE = "silver.sales.transactions"
CUSTOMERS_TABLE = "silver.sales.customers"
PRODUCTS_TABLE = "silver.sales.products"
TARGET_TABLE = "gold.sales.transactions"

PRIMARY_KEYS = ["transaction_id"]

TARGET_SCHEMA_SQL = """
transaction_id BIGINT,
transaction_date TIMESTAMP,
customer_id INT,
customer_name STRING,
country STRING,
city STRING,
product_id INT,
product_name STRING,
category STRING,
brand STRING,
quantity INT,
amount DECIMAL(10,2),
net_amount DECIMAL(10,2),
status STRING
""".strip()

PARTITION_BY = "days(transaction_date)"


# =========================
# SPARK INIT
# =========================
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    silver_warehouse = Variable.get("SILVER_WAREHOUSE")
    gold_warehouse = "s3a://lakehouse/gold"
    nessie_uri = Variable.get("NESSIE_URI")
    nessie_ref_silver = Variable.get("NESSIE_BRANCH_SILVER", default_var="silver")
    nessie_ref_gold = Variable.get("NESSIE_BRANCH_GOLD", default_var="gold")

    return get_spark_session(
        app_name,
        {
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

            "spark.sql.catalog.gold": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.gold.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.gold.uri": nessie_uri,
            "spark.sql.catalog.gold.ref": nessie_ref_gold,
            "spark.sql.catalog.gold.warehouse": gold_warehouse,
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
    spark = get_spark("gold_sales_transactions")
    try:
        run_gold_fact_merge(
            spark=spark,
            source_table=SOURCE_TABLE,
            customers_table=CUSTOMERS_TABLE,
            products_table=PRODUCTS_TABLE,
            target_table=TARGET_TABLE,
            pk_cols=PRIMARY_KEYS,
            target_schema_sql=TARGET_SCHEMA_SQL,
            partition_by=PARTITION_BY,
        )
    finally:
        stop_spark(spark)


# =========================
# DAG
# =========================
with DAG(
    dag_id="gold_sales_transactions",
    start_date=datetime(2025, 1, 1),
    # QEYD: yalnız SILVER_TRANSACTIONS_DS-ə bağlıdır, çünki customers/products
    # üçün ayrı streaming DAG-ın olduğunu bilmirəm. Əgər onlar da ayrı DAG-larla
    # yenilənirsə, sadəcə siyahıya əlavə et:
    # schedule=[SILVER_TRANSACTIONS_DS, SILVER_CUSTOMERS_DS, SILVER_PRODUCTS_DS],
    schedule=[SILVER_TRANSACTIONS_DS],
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
        outlets=[GOLD_TRANSACTIONS_DS],
        pool="spark_driver_pool",
    )