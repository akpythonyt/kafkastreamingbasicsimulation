from kafka import KafkaConsumer
import json

TOPIC = "telemetry_data"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="telemetry_group"
)

print("📥 Listening for telemetry data...")

for message in consumer:
    print(f"Received: {message.value}")
