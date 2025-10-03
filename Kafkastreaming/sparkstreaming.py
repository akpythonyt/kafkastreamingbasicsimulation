from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import from_json, from_unixtime, col
from cassandra.cluster import Cluster
import uuid

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("TelemetryETLToCassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Telemetry schema
# -----------------------------
schema = StructType([
    StructField("timestamp", DoubleType()),
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
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 200) \
    .load() \
    .selectExpr("CAST(value AS STRING) as json")

# -----------------------------
# Parse JSON and convert timestamp
# -----------------------------
telemetry_df = kafka_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
telemetry_df = telemetry_df.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

# -----------------------------
# Filter invalid data
# -----------------------------
telemetry_df_clean = telemetry_df.filter(
    (col("speed") >= 0) &
    (col("fuel") >= 0) &
    (col("temperature") >= -50)
)

# -----------------------------
# Function to write batch to Cassandra
# -----------------------------
def write_to_cassandra(batch_df, batch_id):
    insert_query = """
        INSERT INTO telemetry_data (id, timestamp, speed, temperature, fuel)
        VALUES (%s, %s, %s, %s, %s)
    """

    def insert_partition(rows):
        # Create a Cassandra session per partition
        cluster = Cluster(['127.0.0.1'], connect_timeout=30)
        session = cluster.connect('telemetry_keyspace')

        # Use async inserts for speed
        futures = []
        for row in rows:
            fut = session.execute_async(insert_query, (
                uuid.uuid4(), row.timestamp, row.speed, row.temperature, row.fuel
            ))
            futures.append(fut)

        # Wait for all inserts in this partition to complete
        for fut in futures:
            fut.result()

        # Close session
        session.shutdown()
        cluster.shutdown()

    # Process each partition in parallel
    batch_df.foreachPartition(insert_partition)

# -----------------------------
# Write stream to Cassandra with checkpointing
# -----------------------------
query = telemetry_df_clean.writeStream \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", "/tmp/checkpoints/telemetry") \
    .outputMode("update") \
    .start()

query.awaitTermination()
