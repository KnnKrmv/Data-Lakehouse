"""
jobs/silver_cdc_job.py

Bronze ilə eyni Kafka(Debezium CDC) topic-ini AYRI checkpoint ilə oxuyub,
silver Iceberg table-a upsert/delete edir. jobs/cdc_kafka_job.py-dəki
ensure_iceberg_table / build_row_schema / read_cdc_stream funksiyalarını
təkrar istifadə edir — yalnız batch-processing hissəsinə keyfiyyət filtri
əlavə olunub.

Niyə bronze-u Iceberg stream kimi oxumuruq?
Bronze-a yazı MERGE (copy-on-write) ilə olduğu üçün demək olar ki, bütün
snapshot-lar "overwrite" tipindədir. Iceberg-in streaming reader-i overwrite
snapshot-ları emal edə bilmir (ya səhv verir, ya da onları atlayıb heç bir
data görmür). Bunun əvəzinə silver də bronze kimi birbaşa Kafka-dan oxuyur —
eyni mənbə, iki müstəqil consumer, iki ayrı checkpoint.
"""
from __future__ import annotations

from pyspark.sql.functions import col

from jobs.cdc_kafka_job import (
    build_row_schema,
    ensure_iceberg_table,
    read_cdc_stream,
)


def make_silver_upsert_batch_fn(
    target_table: str,
    pk_cols: list,
    upsert_select: list,
    delete_select: list,
    quality_filter_expr: str = "true",
):
    def process_batch(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        session = batch_df.sparkSession

        # Upserts (create / update / read-snapshot) + keyfiyyət filtri
        upserts = (
            batch_df.filter(col("op").isin("c", "u", "r"))
            .select(col("after").alias("src"))
            .selectExpr(*upsert_select)
            .filter(quality_filter_expr)
            .filter(col(pk_cols[0]).isNotNull())
            .dropDuplicates(pk_cols)
        )

        upsert_count = upserts.count()
        if upsert_count > 0:
            upserts.createOrReplaceTempView("_silver_src_upserts")

            on_clause = " AND ".join(f"t.{k}=s.{k}" for k in pk_cols)
            cols = upserts.columns
            non_key = [c for c in cols if c not in pk_cols]
            set_clause = ", ".join(f"t.{c}=s.{c}" for c in (non_key or pk_cols))
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join(f"s.{c}" for c in cols)

            session.sql(f"""
                MERGE INTO {target_table} t
                USING _silver_src_upserts s ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
            print(f"[OK] silver batch={batch_id} upsert={upsert_count} -> {target_table}")

        # Deletes (keyfiyyət filtri tətbiq edilmir - silinmə həmişə keçərlidir)
        deletes = (
            batch_df.filter(col("op") == "d")
            .select(col("before").alias("src"))
            .selectExpr(*delete_select)
            .filter(col(pk_cols[0]).isNotNull())
            .dropDuplicates(pk_cols)
        )

        delete_count = deletes.count()
        if delete_count > 0:
            deletes.createOrReplaceTempView("_silver_src_deletes")
            on_clause = " AND ".join(f"t.{k}=s.{k}" for k in pk_cols)
            session.sql(f"""
                MERGE INTO {target_table} t
                USING _silver_src_deletes s ON {on_clause}
                WHEN MATCHED THEN DELETE
            """)
            print(f"[OK] silver batch={batch_id} delete={delete_count} -> {target_table}")

    return process_batch


def run_silver_cdc_upsert_stream(
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
    quality_filter_expr: str = "true",
    partition_by: str = "",
    starting_offsets: str = "earliest",
    timeout_seconds: int | None = None,
):
    """
    Bronze-dakı run_cdc_upsert_stream ilə eyni imza/davranış, üstünə
    quality_filter_expr əlavə olunub. Eyni kafka_topic-i bronze-dan tamamilə
    müstəqil (ayrı checkpoint_location) oxuyur.
    """
    ensure_iceberg_table(spark, target_table, schema_sql, partition_by)
    row_schema = build_row_schema(row_schema_fields)
    parsed = read_cdc_stream(spark, kafka_bootstrap, kafka_topic, row_schema, starting_offsets)
    process_batch = make_silver_upsert_batch_fn(
        target_table, pk_cols, upsert_select, delete_select, quality_filter_expr
    )

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