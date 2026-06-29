from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

from pyspark.sql.functions import col

from jobs.batch_ingest_job import (
    get_spark_session,
    stop_spark,
)

# =========================
# VARIABLES
# =========================

schema = Variable.get("PG_SOURCE_SCHEMA", default_var="sales")

tables_list = json.loads(
    Variable.get(
        "POSTGRES_TABLES",
        default_var='["sales_transactions"]'
    )
)

PARTITION_COLUMN = "product_name"

# =========================
# SPARK
# =========================

def get_spark(app_name: str):

    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")

    bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")

    return get_spark_session(
        app_name,
        {
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.bronze.uri": nessie_uri,
            "spark.sql.catalog.bronze.ref": "bronze",
            "spark.sql.catalog.bronze.warehouse": bronze_warehouse,
            "spark.sql.catalog.bronze.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.bronze.s3.endpoint": minio_endpoint,
            "spark.sql.catalog.bronze.s3.path-style-access": "true",
            "spark.sql.catalog.bronze.s3.access-key-id": minio_access_key,
            "spark.sql.catalog.bronze.s3.secret-access-key": minio_secret_key,
        },
    )

# =========================
# CREATE NAMESPACE
# =========================

def create_namespace():

    spark = get_spark("create_namespace")

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze.{schema}")
    finally:
        stop_spark(spark)

# =========================
# INGEST PARTITIONED TABLE
# =========================

def ingest_partitioned_table(table_name):

    spark = get_spark(f"partition_{table_name}")

    try:

        pg = BaseHook.get_connection("postgres_lakehouse")

        jdbc_url = (
            f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.schema}"
        )

        source = f"{schema}.{table_name}"

        target = f"bronze.{schema}.{table_name}_partitioned"

        location = (
            f"{Variable.get('BRONZE_WAREHOUSE')}/{schema}/{table_name}_partitioned"
        )

        print(f"Reading {source}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=source,
            properties={
                "user": pg.login,
                "password": pg.password,
                "driver": "org.postgresql.Driver",
            },
        )

        print(f"Writing {target}")

        (
            df.repartition(col(PARTITION_COLUMN))
            .writeTo(target)
            .using("iceberg")
            .tableProperty("location", location)
            .partitionedBy(col(PARTITION_COLUMN))
            .createOrReplace()
        )

        print(f"Finished {target}")

    finally:
        stop_spark(spark)

# =========================
# DAG
# =========================

with DAG(
    dag_id="postgres_to_partitioned_bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "partition", "iceberg"],
) as dag:

    create_ns = PythonOperator(
        task_id="create_namespace",
        python_callable=create_namespace,
    )

    for table in tables_list:

        task = PythonOperator(
            task_id=f"partition_{table}",
            python_callable=ingest_partitioned_table,
            op_kwargs={
                "table_name": table
            },
        )

        create_ns >> task