"""
Reusable Kafka(Debezium CDC) -> Iceberg (Nessie) upsert helper.
jobs/batch_ingest_job.py-dəki get_spark_session/stop_spark ilə birlikdə istifadə olunur.
"""
from __future__ import annotations 
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType, 
    StructField,
    StructType,
)


def _spark_type(type_name: str):
    mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
    }
    return mapping.get(type_name.lower(), StringType())


def build_row_schema(fields: list) -> StructType:
    return StructType([
        StructField(f["name"], _spark_type(f["type"]), True)
        for f in fields
    ])


def ensure_iceberg_table(spark, target_table: str, schema_sql: str, partition_by: str = "") -> None:
    parts = target_table.split(".")
    namespace = f"{parts[0]}.{parts[1]}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    partition_sql = f" PARTITIONED BY ({partition_by})" if partition_by else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} ({schema_sql}) USING iceberg{partition_sql}")


def read_cdc_stream(spark, kafka_bootstrap: str, kafka_topic: str, row_schema: StructType, starting_offsets: str = "earliest"):
    # schemas.enable=false olduğu üçün mesaj birbaşa (payload wrapper-siz) belədir:
    # {"before": {...}|null, "after": {...}|null, "op": "c|u|d|r", ...}
    envelope_schema = StructType([
        StructField("before", row_schema),
        StructField("after", row_schema),
        StructField("op", StringType()),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), envelope_schema).alias("e"))
        .select(
            col("e.before").alias("before"),
            col("e.after").alias("after"),
            col("e.op").alias("op"),
        )
        .filter(col("op").isNotNull())
    )

def make_upsert_batch_fn(target_table: str, pk_cols: list, upsert_select: list, delete_select: list):
    def process_batch(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        session = batch_df.sparkSession

        # Upserts (create / update / read-snapshot)
        upserts = (
            batch_df.filter(col("op").isin("c", "u", "r"))
            .select(col("after").alias("src"))
            .selectExpr(*upsert_select)
            .filter(col(pk_cols[0]).isNotNull())
            .dropDuplicates(pk_cols)
        )

        upsert_count = upserts.count()
        if upsert_count > 0:
            upserts.createOrReplaceTempView("_src_upserts")

            on_clause = " AND ".join(f"t.{k}=s.{k}" for k in pk_cols)
            cols = upserts.columns
            non_key = [c for c in cols if c not in pk_cols]
            set_clause = ", ".join(f"t.{c}=s.{c}" for c in (non_key or pk_cols))
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join(f"s.{c}" for c in cols)

            session.sql(f"""
                MERGE INTO {target_table} t
                USING _src_upserts s ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
            print(f"[OK] batch={batch_id} upsert={upsert_count} -> {target_table}")

        # Deletes
        deletes = (
            batch_df.filter(col("op") == "d")
            .select(col("before").alias("src"))
            .selectExpr(*delete_select)
            .filter(col(pk_cols[0]).isNotNull())
            .dropDuplicates(pk_cols)
        )

        delete_count = deletes.count()
        if delete_count > 0:
            deletes.createOrReplaceTempView("_src_deletes")
            on_clause = " AND ".join(f"t.{k}=s.{k}" for k in pk_cols)
            session.sql(f"""
                MERGE INTO {target_table} t
                USING _src_deletes s ON {on_clause}
                WHEN MATCHED THEN DELETE
            """)
            print(f"[OK] batch={batch_id} delete={delete_count} -> {target_table}")

    return process_batch


def run_cdc_upsert_stream(
    spark,
    kafka_bootstrap: str,
    kafka_topic: str,
    checkpoint_location: str,
    target_table: str,
    schema_sql: str,
    row_schema_fields: list,
    pk_cols: list,
    upsert_select: list,
    delete_select: list,
    partition_by: str = "",
    starting_offsets: str = "earliest",
    timeout_seconds: int | None = None,
):
    """
    target table-i yaradır (yoxdursa), Kafka-dan CDC stream-i oxuyur,
    hər mikro-batch-i MERGE INTO ilə upsert/delete edir.

    timeout_seconds verilibsə: query həmin müddətdən sonra səliqəylə dayandırılır
    (Airflow PythonOperator-un sonsuz bloklanmaması üçün, checkpoint-dən davam edəcək).
    timeout_seconds verilməyibsə: query bitənə qədər gözlənilir (adətən bitmir).
    """
    ensure_iceberg_table(spark, target_table, schema_sql, partition_by)
    row_schema = build_row_schema(row_schema_fields)
    parsed = read_cdc_stream(spark, kafka_bootstrap, kafka_topic, row_schema, starting_offsets)
    process_batch = make_upsert_batch_fn(target_table, pk_cols, upsert_select, delete_select)

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    if timeout_seconds:
        query.awaitTermination(timeout_seconds)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()

    return query