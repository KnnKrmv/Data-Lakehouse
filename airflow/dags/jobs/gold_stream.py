from __future__ import annotations

from pyspark.sql.functions import col
from jobs.cdc_kafka_job import ensure_iceberg_table


def run_gold_fact_merge(
    spark,
    source_table: str,
    customers_table: str,
    products_table: str,
    target_table: str,
    pk_cols: list,
    target_schema_sql: str,
    partition_by: str = "",
):
    # =========================
    # CREATE TABLE (Silver ilə eyni helper)
    # =========================
    ensure_iceberg_table(
        spark=spark,
        target_table=target_table,
        schema_sql=target_schema_sql,
        partition_by=partition_by,
    )

    # =========================
    # READ SILVER
    # =========================
    transactions = spark.read.table(source_table)
    customers = spark.read.table(customers_table)
    products = spark.read.table(products_table)

    # =========================
    # BUILD FACT
    # =========================
    fact = (
        transactions
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .select(
            col("transaction_id"),
            col("transaction_date"),
            col("customer_id"),
            col("full_name").alias("customer_name"),
            col("country"),
            col("city"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("brand"),
            col("quantity"),
            col("amount"),
            col("net_amount"),
            col("status"),
        )
        .filter(col(pk_cols[0]).isNotNull())
        .dropDuplicates(pk_cols)
    )

    if fact.limit(1).count() == 0:
        print(f"[SKIP] {target_table} boşdur.")
        return

    fact.createOrReplaceTempView("_gold_src_fact")

    cols = fact.columns
    non_key = [c for c in cols if c not in pk_cols]

    on_clause = " AND ".join(f"t.{c}=s.{c}" for c in pk_cols)
    set_clause = ", ".join(f"t.{c}=s.{c}" for c in non_key)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING _gold_src_fact s
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
    """)

    print(f"[OK] Gold merge -> {target_table}")