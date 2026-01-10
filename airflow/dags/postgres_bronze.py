from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
import psycopg2
import os

# =====================================================
# SPARK SESSION - DÜZƏLDİLMİŞ
# =====================================================

def get_spark(app_name: str):
    # ✅ Bütün mövcud Spark referanslarını təmizlə
    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    except Exception:
        pass
    
    try:
        SparkContext._active_spark_context = None
        SparkContext._gateway = None
    except Exception:
        pass
    
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")

    os.environ["AWS_REGION"] = "us-east-1"

    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        
        .config("spark.jars", 
                "/opt/spark/jars-extra/postgresql-42.7.1.jar,"
                "/opt/spark/jars-extra/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,"
                "/opt/spark/jars-extra/nessie-spark-extensions-3.5_2.12-0.77.1.jar,"
                "/opt/spark/jars-extra/bundle-2.20.18.jar,"
                "/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,"
                "/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar")
        
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "")
        .config("spark.driver.memory", "2g")
        
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "16")
        
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # ✅ AWS Region konfiqurasiyası - executor-lar üçün
        .config("spark.executorEnv.AWS_REGION", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bronze.catalog-impl", 
                "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.bronze.uri", nessie_uri)
        .config("spark.sql.catalog.bronze.ref", "bronze")
        .config("spark.sql.catalog.bronze.warehouse", bronze_warehouse)
        .config("spark.sql.catalog.bronze.io-impl", 
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.bronze.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.bronze.s3.path-style-access", "true")
        .config("spark.sql.catalog.bronze.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.bronze.s3.secret-access-key", minio_secret_key)
        
        .getOrCreate()
    )

def create_namespace():
    spark = get_spark("CreateNamespace")
    try:
        schema = Variable.get("BRONZE_SCHEMA")
        
        print(f"✅ Spark Master: {spark.sparkContext.master}")
        print(f"✅ App Name: {spark.sparkContext.appName}")
        
        # ✅ Catalog adı olmadan sadəcə schema
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze.{schema}")
    finally:
        spark.stop()

def get_postgres_tables():
    pg = BaseHook.get_connection("postgres_lakehouse")

    conn = psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.schema,
        user=pg.login,
        password=pg.password
    )

    source_schema = Variable.get("PG_SOURCE_SCHEMA")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
              AND table_schema=%s
            """,
            (source_schema,)
        )
        tables = cur.fetchall()

    conn.close()
    return tables

def ingest_table(table_schema, table_name):
    spark = get_spark(f"Ingest_{table_name}")

    try:
        pg = BaseHook.get_connection("postgres_lakehouse")
        jdbc_url = f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.schema}"

        catalog_1 = Variable.get("CATALOG_1")
        bronze_schema = Variable.get("BRONZE_SCHEMA")
        bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")

        source = f"{table_schema}.{table_name}"
        target = f"{catalog_1}.{bronze_schema}.{table_name}"
        location = f"{bronze_warehouse}/{bronze_schema}/{table_name}"

        print(f"✅ Processing: {source} -> {target}")
        print(f"✅ Spark running on: {spark.sparkContext.master}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=source,
            properties={
                "user": pg.login,
                "password": pg.password,
                "driver": "org.postgresql.Driver"
            }
        )

        df.writeTo(target) \
            .using("iceberg") \
            .tableProperty("location", location) \
            .createOrReplace()

        print(f"✅ Successfully ingested {table_name}")

    finally:
        spark.stop()

# =====================================================
# DAG
# =====================================================

with DAG(
    dag_id="postgres_to_bronze_lakehouse",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["bronze", "lakehouse", "postgres"]
) as dag:

    create_ns = PythonOperator(
        task_id="create_bronze_namespace",
        python_callable=create_namespace
    )

    # ✅ Variable-dan cədvəl siyahısını oxu
    import json
    tables_list = json.loads(Variable.get("POSTGRES_TABLES"))
    source_schema = Variable.get("PG_SOURCE_SCHEMA")
    
    for table in tables_list:
        task = PythonOperator(
            task_id=f"ingest_{table}",
            python_callable=ingest_table,
            op_kwargs={
                "table_schema": source_schema,
                "table_name": table
            }
        )
        create_ns >> task