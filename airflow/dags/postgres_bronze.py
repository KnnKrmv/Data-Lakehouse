from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import psycopg2
from airflow.operators.empty import EmptyOperator   
from common_dataset import BRONZE_READY
# Spark helpers (səndə jobs folderi varsa düzgün mount olunmalıdır)
from jobs.batch_ingest_job import (
    get_spark_session,
    stop_spark,
    write_iceberg_table
)

# =========================
# VARIABLES
# =========================
schema = Variable.get("PG_SOURCE_SCHEMA", default_var="sales")

tables_list = json.loads(
    Variable.get("POSTGRES_TABLES")
)

# =========================
# SPARK INIT
# =========================
def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    nessie_branch_bronze = Variable.get("NESSIE_BRANCH_BRONZE")
    bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")

    return get_spark_session(
        app_name,
        {
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.bronze.uri": nessie_uri,
            "spark.sql.catalog.bronze.ref": nessie_branch_bronze,
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
# TASK 1: CREATE NAMESPACE
# =========================
def create_namespace():
    spark = get_spark("create_namespace")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze.{schema}")
        print(f"[OK] Namespace created: bronze.{schema}")
    finally:
        stop_spark(spark)

# =========================
# TASK 2: INGEST TABLE
# =========================
def ingest_table(table_name: str):
    spark = get_spark(f"ingest_{table_name}")

    try:
        pg = BaseHook.get_connection("postgres_lakehouse")

        jdbc_url = f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.schema}"

        source = f"{schema}.{table_name}"
        target = f"bronze.{schema}.{table_name}"
        location = f"{Variable.get('BRONZE_WAREHOUSE')}/{schema}/{table_name}"

        print(f"[START] {source} -> {target}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=source,
            properties={
                "user": pg.login,
                "password": pg.password,
                "driver": "org.postgresql.Driver"
            }
        )

        write_iceberg_table(
            df=df,
            source=source,
            target=target,
            location=location,
            replace_with_empty=True,
        )

        print(f"[DONE] {table_name}")

    finally:
        stop_spark(spark)

# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="postgres_to_bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "postgres", "lakehouse"]
) as dag:

    create_ns = PythonOperator(
        task_id="create_namespace",
        python_callable=create_namespace
    )
    finish_bronze = EmptyOperator(
        task_id="finish_bronze",
        outlets=[BRONZE_READY]
    )
    for table in tables_list:
        task = PythonOperator(
            task_id=f"ingest_{table}",
            python_callable=ingest_table,
            op_kwargs={
                "table_name": table
            }
        )

        create_ns >> task >> finish_bronze