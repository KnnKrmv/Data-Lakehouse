from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
import os

def from_postgres_to_bronze():
    pg_conn = BaseHook.get_connection('postgres_lakehouse')
    jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"

    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin123")
    bronze_table = Variable.get("BRONZE_TABLE", default_var="project1.customers")
    nessie_uri = Variable.get("NESSIE_URI", default_var="http://nessie:19120/api/v1")

    table_name = bronze_table.split('.')[-1]
    schema_name = bronze_table.split('.')[0]

    # Maven paketləri ilə başladın (internet tələb edir, ilk dəfə yavaş ola bilər)
    spark = SparkSession.builder \
        .appName(f"PostgresToBronze_{table_name}") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.1,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
                "software.amazon.awssdk:bundle:2.20.18,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.bronze.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.bronze.uri", nessie_uri) \
        .config("spark.sql.catalog.bronze.ref", "bronze") \
        .config("spark.sql.catalog.bronze.warehouse", "s3a://warehouse/bronze") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .getOrCreate()

    try:
        print(f"Reading from PostgreSQL: {bronze_table}")
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", bronze_table) \
            .option("user", pg_conn.login) \
            .option("password", pg_conn.password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        row_count = df.count()
        print(f"Successfully read {row_count} rows from PostgreSQL")
        
        df.printSchema()
        if row_count > 0:
            df.show(5, truncate=False)

        full_table_name = f"bronze.{schema_name}.{table_name}"
        print(f"Writing to Iceberg table: {full_table_name}")
        
        df.writeTo(full_table_name) \
            .using("iceberg") \
            .createOrReplace()

        print(f"✅ Successfully written to {full_table_name}")
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

with DAG(
    dag_id="ssss",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['lakehouse', 'bronze', 'postgres', 'etl']
) as dag:

    ingest_table = PythonOperator(
        task_id='ss',
        python_callable=from_postgres_to_bronze,
        execution_timeout=None
    )