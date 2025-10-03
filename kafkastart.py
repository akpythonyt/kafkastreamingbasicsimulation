import subprocess
import time
import sys
from config import KAFKA_PATH, ZOOKEEPER_CONFIG, KAFKA_CONFIG

def start_zookeeper():
    print("Starting Zookeeper...")
    return subprocess.Popen(
        ["bin/zookeeper-server-start.sh", ZOOKEEPER_CONFIG],
        cwd=KAFKA_PATH,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

def start_kafka():
    print("Starting Kafka broker...")
    return subprocess.Popen(
        ["bin/kafka-server-start.sh", KAFKA_CONFIG],
        cwd=KAFKA_PATH,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

if __name__ == "__main__":
    try:
        zk_process = start_zookeeper()
        time.sleep(5)  # Give Zookeeper time to start
        kafka_process = start_kafka()
        print("Kafka and Zookeeper are starting... Press Ctrl+C to stop.")

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Kafka and Zookeeper...")
        kafka_process.terminate()
        zk_process.terminate()
        sys.exit(0)
