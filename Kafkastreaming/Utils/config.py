# config.py
# Absolute paths to Kafka and scripts

# Kafka installation path
KAFKA_PATH = "/Users/arun/Downloads/kafka_2.13-3.7.0"

# Project path (where producer and sparkstreaming scripts are located)
PROJECT_PATH = "/Users/arun/Desktop/Dataingestion/Kafkastreaming"

# Config files
ZOOKEEPER_CONFIG = f"{KAFKA_PATH}/config/zookeeper.properties"
KAFKA_CONFIG = f"{KAFKA_PATH}/config/server.properties"

# Script paths
PRODUCER_SCRIPT = f"{PROJECT_PATH}/producer.py"
SPARK_SCRIPT = f"{PROJECT_PATH}/sparkstreaming.py"
