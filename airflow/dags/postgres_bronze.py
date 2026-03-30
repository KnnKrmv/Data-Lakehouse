from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from spark.jobs.batch_ingest_job import get_spark_session, stop_spark, write_iceberg_table
from datetime import datetime
import psycopg2


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
        spark_extensions=(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ),
    )


def create_namespace():
    spark = get_spark("CreateNamespace")
    try:
        schema = Variable.get("BRONZE_SCHEMA_PG")

        print(f"[postgres_bronze] action=create_namespace schema={schema} master={spark.sparkContext.master} app={spark.sparkContext.appName}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze.{schema}")
    finally:
        stop_spark(spark)


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
        bronze_schema = Variable.get("BRONZE_SCHEMA_PG")
        bronze_warehouse = Variable.get("BRONZE_WAREHOUSE")

        source = f"{table_schema}.{table_name}"
        target = f"{catalog_1}.{bronze_schema}.{table_name}"
        location = f"{bronze_warehouse}/{bronze_schema}/{table_name}"

        print(f"[postgres_bronze] action=ingest_start source={source} target={target} location={location} spark={spark.sparkContext.master}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=source,
            column='transaction_id',
            lowerBound=0,
            upperBound=100000000,
            numPartitions=10,
            properties={
                "user": pg.login,
                "password": pg.password,
                "fetchsize": "10000",
                "driver": "org.postgresql.Driver"
            }
        )

        write_iceberg_table(
            df=df,
            source=source,
            target=target,
            location=location,
            replace_with_empty=True,
        )

    finally:
        stop_spark(spark)


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
