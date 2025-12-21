from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
import os

def create_iceberg_namespace():
    """Iceberg namespace/schema yaradır"""
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin123")
    nessie_uri = Variable.get("NESSIE_URI", default_var="http://lakehouse-nessie:19120/api/v1")  # ✅ v1

    spark = SparkSession.builder \
        .appName("CreateNamespace") \
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
        .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.test.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.test.uri", nessie_uri) \
        .config("spark.sql.catalog.test.ref", "test") \
        .config("spark.sql.catalog.test.warehouse", "s3a://warehouse/test") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        # Namespace yarat (əgər yoxdursa)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS test.test1")
        print("✅ Namespace 'test.test1' created/verified")
        
        # Mövcud namespace-ləri göstər
        spark.sql("SHOW NAMESPACES IN test").show()
        
    except Exception as e:
        print(f"❌ Error creating namespace: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


def test_source_bronze():
    # PostgreSQL bağlantısı
    pg_conn = BaseHook.get_connection('postgres_lakehouse')
    jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"

    # MinIO / Nessie parametrləri
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin123")
    bronze_table = Variable.get("BRONZE_TABLE", default_var="lakehouse.customers")
    nessie_uri = Variable.get("NESSIE_URI", default_var="http://lakehouse-nessie:19120/api/v1")  # ✅ v1

    # Spark session
    spark = SparkSession.builder \
        .appName(f"PostgresToTest_{bronze_table}") \
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
        .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.test.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.test.uri", nessie_uri) \
        .config("spark.sql.catalog.test.ref", "test") \
        .config("spark.sql.catalog.test.warehouse", "s3a://warehouse/test") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        print(f"Reading from PostgreSQL table: {bronze_table}")
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", bronze_table) \
            .option("user", pg_conn.login) \
            .option("password", pg_conn.password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        row_count = df.count()
        print(f"Read {row_count} rows from PostgreSQL")
        df.printSchema()
        if row_count > 0:
            df.show(5, truncate=False)

        # Test catalog / schema / table
        full_table_name = "test.test1.test_table"
        print(f"Writing to Iceberg table: {full_table_name}")
        
        # createOrReplace istifadə et
        df.writeTo(full_table_name) \
            .using("iceberg") \
            .createOrReplace()

        print(f"✅ Successfully written to {full_table_name} in MinIO")
        
        # Yoxlama: table-dan oxu
        result_df = spark.table(full_table_name)
        print(f"✅ Verification: Table has {result_df.count()} rows")
        result_df.show(5, truncate=False)

    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


with DAG(
    dag_id="test_ingest_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['lakehouse', 'test', 'postgres', 'etl']
) as dag:

    create_namespace_task = PythonOperator(
        task_id='create_namespace',
        python_callable=create_iceberg_namespace,
        execution_timeout=None
    )

    ingest_table = PythonOperator(
        task_id='ingest_postgres_to_test',
        python_callable=test_source_bronze,
        execution_timeout=None
    )
    
    create_namespace_task >> ingest_table