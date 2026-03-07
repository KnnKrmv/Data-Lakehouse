from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from spark.jobs.batch_ingest_job import get_spark_session, stop_spark, write_iceberg_table
from datetime import datetime
import pyodbc


def _log_event(event: str, **fields):
    details = ", ".join(f"{key}={value}" for key, value in fields.items())
    print(f"[mssql_bronze] {event} | {details}")


def get_spark(app_name: str):
    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    warehouse = Variable.get("BRONZE_WAREHOUSE")
    nessie_uri = Variable.get("NESSIE_URI")

    return get_spark_session(
        app_name,
        {
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.type": "nessie",
            "spark.sql.catalog.bronze.uri": nessie_uri,
            "spark.sql.catalog.bronze.warehouse": warehouse,
            "spark.sql.catalog.bronze.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.bronze.s3.endpoint": minio_endpoint,
            "spark.sql.catalog.bronze.s3.path-style-access": "true",
            "spark.sql.catalog.bronze.s3.access-key-id": minio_access_key,
            "spark.sql.catalog.bronze.s3.secret-access-key": minio_secret_key,
        },
        spark_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )


def create_namespace(schema):
    namespace = f"bronze.mssql_{schema}"
    app_name = f"CreateNamespace_{schema}"
    _log_event("action", step="create_namespace", namespace=namespace, app=app_name)

    spark = get_spark(app_name)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        _log_event("result", step="create_namespace", namespace=namespace, status="ok")
    finally:
        stop_spark(spark)


def ingest_table(schema, table):
    spark = get_spark(f"Ingest_{schema}_{table}")
    ms = BaseHook.get_connection("mssql_lakehouse")
    warehouse = Variable.get("BRONZE_WAREHOUSE")
    source = f"{schema}.{table}"
    target = f"bronze.mssql_{schema}.{table}"
    location = f"{warehouse}/mssql_{schema}/{table}"

    _log_event(
        "action",
        step="ingest_start",
        source=source,
        target=target,
        location=location,
        spark="spark://spark-master:7077",
    )

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
        _log_event(
            "result",
            step="jdbc_read",
            source=source,
            target=target,
            partitions=df.rdd.getNumPartitions(),
        )

        write_iceberg_table(
            df=df,
            source=source,
            target=target,
            location=location,
            replace_with_empty=True,
        )
        _log_event("result", step="ingest_success", source=source, target=target)
    except Exception as e:
        _log_event("result", step="ingest_error", source=source, target=target, error=repr(e))
        raise
    finally:
        stop_spark(spark)


def get_mssql_tables(schema):
    _log_event("action", step="list_tables_start", schema=schema)
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
        _log_event("result", step="list_tables", schema=schema, count=len(tables))
        return tables
    except Exception as e:
        _log_event("result", step="list_tables_error", schema=schema, error=repr(e))
        return []


with DAG(
    dag_id="mssql_to_bronze_lakehouse",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "lakehouse", "mssql"]
) as dag:

    target_schema = "sales"

    create_ns_task = PythonOperator(
        task_id="create_bronze_namespace",
        python_callable=create_namespace,
        op_kwargs={"schema": target_schema}
    )

    tables = get_mssql_tables(target_schema)

    for table_name in tables:
        ingest_task = PythonOperator(
            task_id=f"ingest_{table_name.lower()}",
            python_callable=ingest_table,
            op_kwargs={"schema": target_schema, "table": table_name}
        )
        create_ns_task >> ingest_task
