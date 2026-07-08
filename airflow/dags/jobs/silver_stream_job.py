"""
jobs/silver_stream.py

Bronze append-log-unu Iceberg üzərindən streaming oxuyur. Hər mikro-batch
daxilində eyni PK üçün birdən çox hadisə ola biləcəyi üçün _ingested_at-a
görə YALNIZ ən sonuncu versiyanı seçir (dropDuplicates yerinə deterministik
row_number/window istifadə olunur), keyfiyyət filtri tətbiq edir, silver
Iceberg table-a MERGE (upsert/delete) edir.
"""
from __future__ import annotations

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from jobs.cdc_kafka_job import ensure_iceberg_table


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

        window = Window.partitionBy(*pk_cols).orderBy(col("_ingested_at").desc())
        latest_per_pk = (
            batch_df.withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # Upserts (_op = c/u/r)
        upserts = (
            latest_per_pk.filter(col("_op").isin("c", "u", "r"))
            .selectExpr(*upsert_select)
            .filter(quality_filter_expr)
            .filter(col(pk_cols[0]).isNotNull())
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

        # Deletes (_op = d)
        deletes = (
            latest_per_pk.filter(col("_op") == "d")
            .selectExpr(*delete_select)
            .filter(col(pk_cols[0]).isNotNull())
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


def run_silver_table_upsert_stream(
    spark,
    source_table: str,
    checkpoint_location: str,
    target_table: str,
    schema_sql: str,
    pk_cols: list,
    upsert_select: list,
    delete_select: list,
    quality_filter_expr: str = "true",
    partition_by: str = "",
    starting_offsets: str = "earliest",  # imza uyğunluğu üçün saxlanılıb, Iceberg source-da nəzərə alınmır
    timeout_seconds: int | None = None,
):
    ensure_iceberg_table(spark, target_table, schema_sql, partition_by)

    parsed = spark.readStream.format("iceberg").load(source_table)
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