from airflow.models import Variable
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import os
from typing import Dict, Optional


def _reset_active_context():
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            try:
                active.stop()
            except Exception:
                pass
    except Exception:
        pass

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


def get_spark_session(
    app_name: str,
    catalog_configs: Dict[str, str],
    *,
    spark_extensions: str = (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    ),
):
    _reset_active_context()

    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")

    os.environ["AWS_REGION"] = "us-east-1"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.extraClassPath", "/opt/spark/jars-extra/*")
        .config("spark.executor.extraClassPath", "/opt/spark/jars-extra/*")
        .config("spark.executor.memory", "6g")
        .config("spark.executor.cores", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "lakehouse-airflow")
        .config("spark.driver.port", "7078")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.executorEnv.AWS_REGION", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
        .config("spark.sql.extensions", spark_extensions)
    )

    for key, value in catalog_configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def _log_event(event: str, **fields):
    details = ", ".join(f"{key}={value}" for key, value in fields.items())
    print(f"[batch_ingest] {event} | {details}")


def write_iceberg_table(
    df: DataFrame,
    source: str,
    target: str,
    location: str,
    *,
    replace_with_empty: bool = True,
):
    _log_event(
        "action",
        source=source,
        target=target,
        location=location,
        action="create_or_replace",
        metrics="disabled",
    )
    df.writeTo(target).using("iceberg").tableProperty("location", location).createOrReplace()


def stop_spark(spark: Optional[SparkSession] = None):
    if spark is not None:
        spark.stop()
    else:
        _reset_active_context()
