# Full PySpark Streaming ETL for telemetry_data topic
# Reads from Kafka, cleans data, writes to Delta Lake on HDFS, and prints console output

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import from_json, from_unixtime, col
from delta.tables import DeltaTable


# -----------------------------
# Spark Session with Delta support
# -----------------------------
spark = SparkSession.builder \
    .appName("TelemetryETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Telemetry schema
# -----------------------------
schema = StructType([
    StructField("timestamp", DoubleType()),       # epoch time in seconds
    StructField("speed", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("fuel", DoubleType())
])

# -----------------------------
# Read from Kafka topic
# -----------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telemetry_data") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json")

telemetry_df = kafka_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
telemetry_df = telemetry_df.withColumn("timestamp", from_unixtime(col("timestamp")))

# -----------------------------
# Filter invalid data
# -----------------------------
telemetry_df_clean = telemetry_df.filter(
    (col("speed") >= 0) &
    (col("fuel") >= 0) &
    (col("temperature") >= -50)
)

# -----------------------------
# HDFS Delta Lake paths
# -----------------------------
delta_path = "hdfs://localhost:9000/user/telemetry/delta_car_data"
checkpoint_path = "hdfs://localhost:9000/user/telemetry/checkpoints"

# -----------------------------
# Write to console and Delta Lake
# -----------------------------
query = telemetry_df_clean.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()


# Keep streaming alive
query.awaitTermination()
