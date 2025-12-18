from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os

def init_lakehouse_catalogs():
    nessie_url = "http://lakehouse-nessie:19120/api/v1"
    minio_endpoint = "http://lakehouse-minio:9000"
    
    spark = SparkSession.builder \
        .appName("LakehouseCatalogSetup") \
        .config("spark.jars", "/opt/spark/jars-extra/*.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.bronze.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.bronze.uri", nessie_url) \
        .config("spark.sql.catalog.bronze.warehouse", "s3a://warehouse/bronze/") \
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.silver.uri", nessie_url) \
        .config("spark.sql.catalog.silver.warehouse", "s3a://warehouse/silver/") \
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.gold.uri", nessie_url) \
        .config("spark.sql.catalog.gold.warehouse", "s3a://warehouse/gold/") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sql("SHOW CATALOGS").show()
    spark.stop()

with DAG(
    dag_id='nessie_catalog_creator',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['nessie', 'catalogs', 'iceberg']
) as dag:
    
    catalog_creators = PythonOperator(
        task_id='nessie_catalog_task',
        python_callable=init_lakehouse_catalogs
    )