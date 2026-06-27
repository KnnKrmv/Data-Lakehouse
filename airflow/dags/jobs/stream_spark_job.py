import argparse
import json
import os
import decimal
import base64
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, udf
from pyspark.sql.types import StructType, BinaryType, LongType, TimestampType, DecimalType

# 1. Arqumentlərin qəbulu
parser = argparse.ArgumentParser()
parser.add_argument("--topic", required=True)
parser.add_argument("--target-table", required=True)
parser.add_argument("--checkpoint-path", required=True)
parser.add_argument("--create-table-ddl", required=True)
parser.add_argument("--after-schema-base64", required=True) # JSON əvəzinə Base64
parser.add_argument("--kafka-bootstrap-servers", default="kafka:9092")
# ... (digər argumentlər olduğu kimi qalır) ...
args = parser.parse_args()

# 2. Spark Session
spark = SparkSession.builder.appName(f"stream-bronze-{args.topic}").getOrCreate()

# 3. Schema-nın Base64-dən dekodlanması
decoded_json = base64.b64decode(args.after_schema_base64).decode('utf-8')
after_schema_dict = json.loads(decoded_json)
after_schema = StructType.fromJson(after_schema_dict)

# --- UDF-lər: Debezium tip çevrilmələri ---

# Debezium DECIMAL sahələrini binary (big-endian, signed) olaraq göndərir
def binary_to_decimal(b, scale=2):
    if b is None: return None
    unscaled = int.from_bytes(b, byteorder='big', signed=True)
    return decimal.Decimal(unscaled) / (10 ** scale)

binary_to_decimal_udf = udf(lambda b: binary_to_decimal(b, scale=2), DecimalType(12, 2))

# Debezium TIMESTAMP sahələrini epoch microseconds (long) olaraq göndərir
def long_to_timestamp(us):
    if us is None: return None
    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc)

long_to_timestamp_udf = udf(long_to_timestamp, TimestampType())

# Timestamp olması gözlənilən sahələr (Iceberg DDL-dən)
TIMESTAMP_FIELD_NAMES = {"transaction_date"}

# 4. Stream Oxunuşu
source_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.kafka_bootstrap_servers).option("subscribe", args.topic).load()

# 5. Parsing
outer_schema = StructType.fromJson({
    "type": "struct",
    "fields": [
        {"name": "payload", "type": {
            "type": "struct",
            "fields": [
                {"name": "after", "type": after_schema_dict, "nullable": True, "metadata": {}},
                {"name": "op", "type": "string", "nullable": True, "metadata": {}}
            ]
        }, "nullable": True, "metadata": {}}
    ]
})

parsed_df = source_df.select(from_json(col("value").cast("string"), outer_schema).alias("msg")) \
                     .select("msg.payload.after", "msg.payload.op", current_timestamp().alias("ingested_at")) \
                     .filter(col("after").isNotNull())

# Sahələrin dinamik seçilməsi (Binary→Decimal və Long→Timestamp çevrilmələri)
def _resolve_field(field):
    c = col(f"after.{field.name}")
    if isinstance(field.dataType, BinaryType):
        return binary_to_decimal_udf(c).alias(field.name)
    if isinstance(field.dataType, LongType) and field.name in TIMESTAMP_FIELD_NAMES:
        return long_to_timestamp_udf(c).alias(field.name)
    return c.alias(field.name)

after_fields = [_resolve_field(field) for field in after_schema.fields]

bronze_df = parsed_df.select(*after_fields, col("op").alias("cdc_op"), col("ingested_at"))

# 6. Yazılma
spark.sql(args.create_table_ddl)
query = bronze_df.writeStream \
                 .format("iceberg") \
                 .outputMode("append") \
                 .trigger(availableNow=True) \
                 .option("checkpointLocation", args.checkpoint_path) \
                 .toTable(args.target_table)
query.awaitTermination()