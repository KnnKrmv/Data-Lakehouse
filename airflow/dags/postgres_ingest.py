from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession

def from_postgres_to_bronze():
    """
    PostgreSQL-dən Bronze layer-ə data transfer et.
    """
    
    # ===== POSTGRESQL CREDENTIALS (Connection-dan) =====
    print("📡 Getting PostgreSQL credentials from Connection...")
    pg_conn = BaseHook.get_connection('postgres_lakehouse')
    jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    print(f"✅ PostgreSQL: {pg_conn.host}:{pg_conn.port}/{pg_conn.schema}")
    
    # ===== MINIO CREDENTIALS (Variables-dan) =====
    print("📡 Getting MinIO credentials from Variables...")
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin123")
    print(f"✅ MinIO: {minio_endpoint}")
    
    # ===== CONFIG (Variables-dan) =====
    print("⚙️  Getting configuration from Variables...")
    bronze_table = Variable.get("BRONZE_TABLE", default_var="public.test_sales")
    bronze_base_path = Variable.get("BRONZE_BASE_PATH", default_var="s3a://warehouse/bronze/bmis/")
    
    # Table adını path-ə əlavə et (public.test_sales -> test_sales)
    table_name = bronze_table.split('.')[-1] if '.' in bronze_table else bronze_table
    bronze_path = f"{bronze_base_path}{table_name}/"
    
    print(f"✅ Table: {bronze_table}")
    print(f"✅ Output: {bronze_path}")
    
    # ===== SPARK SESSION =====
    print("🔥 Creating Spark Session...")
    spark = SparkSession.builder \
        .appName("PostgresToBronze") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print("✅ Spark Session created")
    
    # ===== READ FROM POSTGRESQL =====
    print(f"📖 Reading from PostgreSQL: {bronze_table}")
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", bronze_table) \
        .option("user", pg_conn.login) \
        .option("password", pg_conn.password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    row_count = df.count()
    print(f"✅ Read {row_count} rows")
    
    # Sample data
    print("📊 Sample data:")
    df.show(5, truncate=False)
    
    # ===== WRITE TO MINIO =====
    print(f"💾 Writing to MinIO: {bronze_path}")
    df.write \
        .mode("overwrite") \
        .parquet(bronze_path)
    
    print(f"✅ Successfully written {row_count} rows to Bronze layer")
    
    spark.stop()
    print("🛑 Spark stopped")
    print("✅ Pipeline completed!")

with DAG(
    dag_id="Source_Bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['lakehouse', 'bronze', 'postgres', 'etl'],
    description='PostgreSQL to Bronze layer - Using Airflow Connections & Variables'
) as dag:
    
    ingest_table = PythonOperator(
        task_id='Postgres_transfer_Bronze',
        python_callable=from_postgres_to_bronze
    )