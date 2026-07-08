"""
jobs/bronze_append_job.py

Bronze-u "immutable CDC log" kimi yazır: hər hadisə (c/u/d/r) ayrı sətir
kimi APPEND olunur, heç bir MERGE yoxdur. Bu, Iceberg-in yalnız "append"
snapshot yaratmasını təmin edir ki, Silver bunu streaming ilə təhlükəsiz
oxuya bilsin (overwrite/rewrite snapshot-larla bağlı məlum məhdudiyyəti
aradan qaldırır).
"""
from __future__ import annotations

from pyspark.sql.functions import col, current_timestamp, lit
from jobs.cdc_kafka_job import build_row_schema, ensure_iceberg_table, read_cdc_stream


def ensure_bronze_log_table(spark, target_table: str, schema_sql: str, partition_by: str = "") -> None:
    full_schema_sql = f"{schema_sql}, _op STRING, _ingested_at TIMESTAMP"
    ensure_iceberg_table(spark, target_table, full_schema_sql, partition_by)


def make_bronze_append_batch_fn(target_table: str, upsert_select: list, delete_select: list, pk_cols: list):
    def process_batch(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        changes = (
            batch_df.filter(col("op").isin("c", "u", "r"))
            .select(col("after").alias("src"), col("op"))
            .selectExpr(*upsert_select, "op AS _op")
            .filter(col(pk_cols[0]).isNotNull())
        )

        deletes = (
            batch_df.filter(col("op") == "d")
            .select(col("before").alias("src"), col("op"))
            .selectExpr(*delete_select, "op AS _op")
            .filter(col(pk_cols[0]).isNotNull())
        )

        missing_cols = [c for c in changes.columns if c not in deletes.columns]
        for c in missing_cols:
            deletes = deletes.withColumn(c, lit(None))
        deletes = deletes.select(*changes.columns)

        combined = changes.unionByName(deletes).withColumn("_ingested_at", current_timestamp())

        row_count = combined.count()
        if row_count > 0:
            combined.writeTo(target_table).append()
            print(f"[OK] bronze append batch={batch_id} rows={row_count} -> {target_table}")

    return process_batch


def run_bronze_append_stream(
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
    ensure_bronze_log_table(spark, target_table, schema_sql, partition_by)
    row_schema = build_row_schema(row_schema_fields)
    parsed = read_cdc_stream(spark, kafka_bootstrap, kafka_topic, row_schema, starting_offsets)
    process_batch = make_bronze_append_batch_fn(target_table, upsert_select, delete_select, pk_cols)

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