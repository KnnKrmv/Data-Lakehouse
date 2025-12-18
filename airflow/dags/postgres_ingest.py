from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkSession
import json

def from_postgres_to_bronze():
    pg_conn = BaseHook.get_connection("postgres_lakehouse")
    jdbc_url =  f'jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}'
    print(f"✅ PostgreSQL: {pg_conn.host}:{pg_conn.port}/{pg_conn.schema}")
    
    minio_conn = BaseHook.get_connection('minio_lakehouse')
    minio_extra = json.load(minio_conn) if minio_conn.extra else {}
    minio_endpoint = minio_extra.get('endpoint_url')
    minio_access_key = minio_extra.get('aws_access_key_id')
    minio_secret_key = minio_extra.get('aws_secret_access_key')
    print(f"✅ MinIO: {minio_endpoint}")


    bronze_table = Variable.get("BRONZE_TABLE", default_var="public.test_sales")
    bronze_path = Variable.get("BRONZE_PATH", default_var="s3a://warehouse/bronze/sales/")
    print(f"✅ Table: {bronze_table}")
    print(f"✅ Output: {bronze_path}")


    spark = SparkSession.builder \
        .appName("PostgresToBronze") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", bronze_table) \
        .option("user", pg_conn.login) \
        .option("password", pg_conn.password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    row_count = df.count()


    df.write \
        .mode("overwrite") \
        .parquet(bronze_path)
    
    spark.stop()
    print("🛑 Spark Session stopped")
    print("✅ Pipeline completed successfully!")


    with DAG(
        dag_id="Source_to_Bronze",
        start_date=datetime(2025,1,1),
        schedule=None,
        catchup=False,
        tags=['lakehouse', 'bronze', 'postgres', ''],
    ) as dag:
        
        ingest_table = PythonOperator(
            task_id='Postgres_transfer_Bronze',
            python_callable=from_postgres_to_bronze
        )