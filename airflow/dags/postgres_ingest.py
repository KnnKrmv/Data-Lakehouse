from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="nessie_catalog_creator",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    init_catalogs = SparkSubmitOperator(
        task_id="init_nessie_catalogs",
        application="/opt/spark-apps/init_catalogs.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),
            "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.bronze.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.bronze.warehouse": "s3a://warehouse/bronze/",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )
