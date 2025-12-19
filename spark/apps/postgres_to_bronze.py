import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--table", required=True)
parser.add_argument("--warehouse", required=True)
parser.add_argument("--nessie-ref", required=True)
parser.add_argument("--nessie-uri", required=True)
args = parser.parse_args()

table_full = args.table  # ex: sales.customers
warehouse = args.warehouse
nessie_ref = args.nessie_ref
nessie_uri = args.nessie_uri

# Split table into schema and table name
if '.' in table_full:
    schema, table = table_full.split('.')
else:
    schema = 'default'
    table = table_full

output_path = f"{warehouse}{schema}/{table}/"

spark = SparkSession.builder \
    .appName(f"PostgresToBronze_{table}") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config(f"spark.sql.catalog.spark_catalog.warehouse", warehouse) \
    .config(f"spark.sql.catalog.{nessie_ref}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{nessie_ref}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{nessie_ref}.uri", nessie_uri) \
    .config(f"spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config(f"spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config(f"spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config(f"spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# READ FROM POSTGRESQL
pg_jdbc_url = "jdbc:postgresql://postgres:5432/mydb"
pg_user = "postgres"
pg_pass = "postgres"

df = spark.read \
    .format("jdbc") \
    .option("url", pg_jdbc_url) \
    .option("dbtable", table_full) \
    .option("user", pg_user) \
    .option("password", pg_pass) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Read {df.count()} rows from {table_full}")
df.show(5, truncate=False)

# WRITE TO ICEBERG (MinIO)
df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save(f"{nessie_ref}.{table_full}")

print(f"Saved table {table_full} to Iceberg (Nessie ref {nessie_ref}) at {output_path}")

spark.stop()
