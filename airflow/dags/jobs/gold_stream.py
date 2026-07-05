from pyspark.sql.functions import col


def run_gold_fact_stream(
    spark,
    source_table: str,
    customers_table: str,
    products_table: str,
    target_table: str,
    checkpoint_location: str,
):
    # =========================
    # 1. STREAM SOURCE (SILVER)
    # =========================
    transactions = spark.readStream.table(source_table)

    # =========================
    # 2. BATCH DIMENSIONS
    # =========================
    customers = spark.read.table(customers_table)
    products = spark.read.table(products_table)

    # cache (performance)
    customers.cache()
    products.cache()

    # =========================
    # 3. JOIN LOGIC (FACT TABLE)
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
    )

    # =========================
    # 4. WRITE TO ICEBERG (STREAM)
    # =========================
    query = (
        fact.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .toTable(target_table)
    )

    query.awaitTermination()