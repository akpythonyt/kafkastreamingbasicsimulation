import subprocess
import time
import sys
from config import (
    KAFKA_PATH, ZOOKEEPER_CONFIG, KAFKA_CONFIG,
    PRODUCER_SCRIPT, SPARK_SCRIPT
)

# -----------------------------
# Start Zookeeper
# -----------------------------
def start_zookeeper():
    print("Starting Zookeeper...")
    return subprocess.Popen(
        ["bin/zookeeper-server-start.sh", ZOOKEEPER_CONFIG],
        cwd=KAFKA_PATH
    )

# -----------------------------
# Start Kafka Broker
# -----------------------------
def start_kafka():
    print("Starting Kafka broker...")
    return subprocess.Popen(
        ["bin/kafka-server-start.sh", KAFKA_CONFIG],
        cwd=KAFKA_PATH
    )

# -----------------------------
# Start Kafka Producer
# -----------------------------
def start_kafka_producer():
    print("Starting Kafka producer...")
    return subprocess.Popen(
        ["python3", PRODUCER_SCRIPT],
        cwd=KAFKA_PATH
    )

# -----------------------------
# Start Spark Streaming Job
# -----------------------------
def start_spark_streaming():
    print("Starting Spark streaming job...")
    return subprocess.Popen(
        ["spark-submit", "--packages",
         "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
         SPARK_SCRIPT],
        cwd=KAFKA_PATH
    )

# -----------------------------
# Main Function
# -----------------------------
if __name__ == "__main__":
    try:
        zk_process = start_zookeeper()
        time.sleep(5)  # wait for Zookeeper
        kafka_process = start_kafka()
        time.sleep(10)  # wait for Kafka broker to stabilize
        producer_process = start_kafka_producer()
        time.sleep(5)
        spark_process = start_spark_streaming()
        print("\n✅ All services started successfully! Press Ctrl+C to stop.\n")

        # Keep running until interrupted
        while True:
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n🛑 Stopping all services...")
        for p in [spark_process, producer_process, kafka_process, zk_process]:
            try:
                p.terminate()
            except Exception:
                pass
        sys.exit(0)
