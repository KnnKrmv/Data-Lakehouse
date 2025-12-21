from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
import os


# -------------------------------------------------------
# Spark Session Builder (reuse üçün)
# -------------------------------------------------------
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin123")
    nessie_uri = "http://lakehouse-nessie:19120/api/v1"

    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.1,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
            "software.amazon.awssdk:bundle:2.20.18,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bronze.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.bronze.uri", nessie_uri)
        .config("spark.sql.catalog.bronze.ref", "bronze")
        .config("spark.sql.catalog.bronze.warehouse", "s3a://warehouse/bronze")
        .config("spark.sql.catalog.bronze.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.bronze.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.bronze.s3.path-style-access", "true")
        .config("spark.sql.catalog.bronze.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.bronze.s3.secret-access-key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )


# -------------------------------------------------------
# Task 1: Create Iceberg Namespace
# -------------------------------------------------------
def create_iceberg_namespace():
    spark = get_spark("CreateBronzeNamespace")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze.sales_schema")
        spark.sql("SHOW NAMESPACES IN bronze").show()
        print("✅ bronze.sales_schema hazırdır")
    finally:
        spark.stop()


# -------------------------------------------------------
# Task 2: Postgres -> Bronze Iceberg
# -------------------------------------------------------
def source_bronze():
    spark = get_spark("PostgresToBronzeCustomers")

    try:
        pg_conn = BaseHook.get_connection("postgres_lakehouse")
        jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"

        source_table = Variable.get(
            "BRONZE_TABLE",
            default_var="lakehouse.customers"
        )

        df = spark.read.jdbc(
            url=jdbc_url,
            table=source_table,
            properties={
                "user": pg_conn.login,
                "password": pg_conn.password,
                "driver": "org.postgresql.Driver"
            }
        )

        target_table = "bronze.sales_schema.customers"

        if spark.catalog.tableExists(target_table):
            df.writeTo(target_table).append()
            print(f"➕ Append edildi: {target_table}")
        else:
            df.writeTo(target_table).using("iceberg").create()
            print(f"🆕 Create edildi: {target_table}")

        spark.table(target_table).show(5, truncate=False)

    finally:
        spark.stop()


# -------------------------------------------------------
# DAG
# -------------------------------------------------------
with DAG(
    dag_id="source_bronze_sales",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "bronze", "postgres"]
) as dag:

    create_namespace = PythonOperator(
        task_id="create_bronze_namespace",
        python_callable=create_iceberg_namespace
    )

    ingest_customers = PythonOperator(
        task_id="ingest_postgres_customers_to_bronze",
        python_callable=source_bronze
    )

    create_namespace >> ingest_customers
