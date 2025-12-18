from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession

def from_postgres_to_bronze():
    spark = SparkSession.builder \
        .appName("PostgresToBronze") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # localhost əvəzinə servis adı: postgres
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow" 
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.test_sales") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df.write \
        .mode("overwrite") \
        .parquet("s3a://warehouse/bronze/bmis/")
    
    spark.stop()

with DAG(
    dag_id = "Source_Bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    ingest_table = PythonOperator(
        task_id ='Postgres_transfer_Bronze',
        python_callable=from_postgres_to_bronze
    )