import json
import base64
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Base64-ə çevirmə funksiyası
def encode_schema(schema_dict):
    json_str = json.dumps(schema_dict)
    return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

KAFKA_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367",
])

MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin123")

AFTER_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "transaction_id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "customer_id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "customer_name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "product_id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "product_name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "product_category", "type": "string", "nullable": True, "metadata": {}},
        {"name": "quantity", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "unit_price", "type": "binary", "nullable": True, "metadata": {}},
        {"name": "total_amount", "type": "binary", "nullable": True, "metadata": {}},
        {"name": "currency", "type": "string", "nullable": True, "metadata": {}},
        {"name": "payment_method", "type": "string", "nullable": True, "metadata": {}},
        {"name": "transaction_status", "type": "string", "nullable": True, "metadata": {}},
        {"name": "transaction_date", "type": "long", "nullable": True, "metadata": {}},
    ]
}

SALES_TRANSACTIONS_DDL = "CREATE TABLE IF NOT EXISTS bronze.sales_v2.sales_transactions (transaction_id BIGINT, customer_id BIGINT, customer_name STRING, product_id BIGINT, product_name STRING, product_category STRING, quantity INT, unit_price DECIMAL(12, 2), total_amount DECIMAL(12, 2), currency STRING, payment_method STRING, transaction_status STRING, transaction_date TIMESTAMP, cdc_op STRING, ingested_at TIMESTAMP) USING iceberg LOCATION 's3a://lakehouse/bronze/sales_v2/sales_transactions'"

with DAG(
    dag_id="bronze_sales_transactions_v2",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    SparkSubmitOperator(
        task_id="bronze_sales_transactions",
        application="/opt/airflow/dags/jobs/stream_spark_job.py",
        conn_id="spark_default",
        packages=KAFKA_PACKAGES,
        conf={
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            # Iceberg + Nessie catalog configuration
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.bronze.uri": Variable.get("NESSIE_URI", default_var="http://nessie:19120/api/v1"),
            "spark.sql.catalog.bronze.ref": "bronze",
            "spark.sql.catalog.bronze.warehouse": Variable.get("BRONZE_WAREHOUSE", default_var="s3a://lakehouse/bronze"),
            "spark.sql.catalog.bronze.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.bronze.s3.endpoint": MINIO_ENDPOINT,
            "spark.sql.catalog.bronze.s3.path-style-access": "true",
            "spark.sql.catalog.bronze.s3.access-key-id": MINIO_ACCESS_KEY,
            "spark.sql.catalog.bronze.s3.secret-access-key": MINIO_SECRET_KEY,
            "spark.sql.catalog.bronze.client.region": "us-east-1",
        },
        application_args=[
            "--topic", "lakehouse.sales.sales_transactions",
            "--target-table", "bronze.sales_v2.sales_transactions",
            "--checkpoint-path", "s3a://lakehouse/checkpoints/sales_transactions/",
            "--create-table-ddl", SALES_TRANSACTIONS_DDL,
            "--after-schema-base64", encode_schema(AFTER_SCHEMA), # Base64 ilə ötürürük
        ],
    )