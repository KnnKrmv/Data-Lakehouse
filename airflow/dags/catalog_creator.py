from pyspark.sql import SparkSession
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def init_lakehouse_catalogs():
    # Sənin Docker mühitinə uyğun endpointlər
    nessie_url = "http://lakehouse-nessie:19120/api/v1"
    minio_endpoint = "http://lakehouse-minio:9000"
    minio_access_key = "minioadmin"
    minio_secret_key = "minioadmin123"

    print("Starting Spark Session with Multi-Catalog configuration...")

    spark = SparkSession.builder \
        .appName("LakehouseCatalogSetup") \
        .config("spark.jars", "/opt/spark/jars-extra/*.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.bronze.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.bronze.uri", nessie_url) \
        .config("spark.sql.catalog.bronze.warehouse", "s3a://warehouse/bronze/") \
        .config("spark.sql.catalog.bronze.ref", "main") \
        .config("spark.sql.catalog.bronze.authentication.type", "NONE") \
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.silver.uri", nessie_url) \
        .config("spark.sql.catalog.silver.warehouse", "s3a://warehouse/silver/") \
        .config("spark.sql.catalog.silver.ref", "main") \
        .config("spark.sql.catalog.silver.authentication.type", "NONE") \
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.gold.uri", nessie_url) \
        .config("spark.sql.catalog.gold.warehouse", "s3a://warehouse/gold/") \
        .config("spark.sql.catalog.gold.ref", "main") \
        .config("spark.sql.catalog.gold.authentication.type", "NONE") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("✅ Catalogs initialized: 'bronze', 'silver', and 'gold' are ready.")
    
    # Kataloqların yarandığını terminalda görmək üçün:
    spark.sql("SHOW CATALOGS").show()

    return spark

if __name__ == "__main__":
    spark_session = init_lakehouse_catalogs()
    # spark_session.stop()

with DAG(
    dag_id='nessie_catalog_creator',
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=['nessie', 'catalogs', 'iceberg']
) as dag:
    catalog_creators=PythonOperator(
        task_id = 'nessie_catalog',
        python_callable = init_lakehouse_catalogs
    )