from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import pyodbc

# --- SPARK SESSİYASI (Iceberg və Nessie Tənzimləmələri ilə) ---
def get_spark(app_name: str):
    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    except Exception:
        pass

    # Variable-ları götürürük
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    nessie_uri = Variable.get("NESSIE_URI")
    warehouse_path = Variable.get("BRONZE_WAREHOUSE")

    os.environ["AWS_REGION"] = "us-east-1"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        # Kataloq konfiqurasiyası (Xətanın həlli buradadır)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bronze.type", "nessie")
        .config("spark.sql.catalog.bronze.uri", nessie_uri)
        .config("spark.sql.catalog.bronze.warehouse", warehouse_path)
        .config("spark.sql.catalog.bronze.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.bronze.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.bronze.s3.path-style-access", "true")
        # S3A konfiqurasiyası
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Network konfiqurasiyası
        .config("spark.driver.host", "lakehouse-airflow")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )
    return spark

# --- TAPŞIRIQ FUNKSİYALARI ---

def create_namespace(schema):
    spark = get_spark(f"CreateNamespace_{schema}")
    try:
        # Kataloq artıq təyin olunduğu üçün Spark bunu başa düşəcək
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze.mssql_{schema}")
        print(f"✅ Namespace bronze.mssql_{schema} yaradıldı")
    finally:
        spark.stop()

def ingest_table(schema, table):
    spark = get_spark(f"Ingest_{schema}_{table}")
    ms = BaseHook.get_connection("mssql_lakehouse")
    warehouse = Variable.get("BRONZE_WAREHOUSE")
    try:
        jdbc_url = f"jdbc:sqlserver://{ms.host}:{ms.port};databaseName={ms.schema}"
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"{schema}.{table}",
            properties={
                "user": ms.login,
                "password": ms.password,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }
        )
        # Iceberg cədvəlinə yazırıq
        df.writeTo(f"bronze.mssql_{schema}.{table}") \
            .using("iceberg") \
            .tableProperty("location", f"{warehouse}/mssql_{schema}/{table}") \
            .createOrReplace()
        print(f"✅ Table {table} ingested successfully")
    finally:
        spark.stop()

def get_mssql_tables(schema):
    """DAG parse olunanda cədvəl adlarını dinamik çəkir"""
    try:
        ms = BaseHook.get_connection("mssql_lakehouse")
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={ms.host},{ms.port};"
            f"DATABASE={ms.schema};"
            f"UID={ms.login};"
            f"PWD={ms.password};"
            "TrustServerCertificate=yes;"
        )
        with conn.cursor() as cur:
            cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=?", (schema,))
            tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        print(f"⚠️ Cədvəllər oxunarkən xəta: {e}")
        return []

# --- DAG TƏRİFİ ---

with DAG(
    dag_id="mssql_to_bronze_lakehouse",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "lakehouse", "mssql"]
) as dag:

    target_schema = "sales"

    # 1. Namespace Yaradılması
    create_ns_task = PythonOperator(
        task_id="create_bronze_namespace",
        python_callable=create_namespace,
        op_kwargs={"schema": target_schema}
    )

    # 2. MSSQL-dən cədvəllərin siyahısını alırıq
    tables = get_mssql_tables(target_schema)

    # 3. Hər cədvəl üçün ayrı kvadrat (task) yaradan döngü
    for table_name in tables:
        ingest_task = PythonOperator(
            task_id=f"ingest_{table_name.lower()}",
            python_callable=ingest_table,
            op_kwargs={"schema": target_schema, "table": table_name}
        )

        # Asılılıq: Namespace -> Ingest (Şəkildəki kimi)
        create_ns_task >> ingest_task