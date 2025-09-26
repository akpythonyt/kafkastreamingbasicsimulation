import time, random, json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
TOPIC = "telemetry_data"

def generate_telemetry():
    return {
        "timestamp": time.time(),
        "speed": round(random.uniform(0, 150), 2),
        "temperature": round(random.uniform(70, 120), 2),
        "fuel": round(random.uniform(5, 100), 2),
    }

print("🚀 Producing telemetry data to Kafka...")
while True:
    data = generate_telemetry()
    producer.send(TOPIC, data)
    producer.flush()
    print(f"Sent: {data}")
    time.sleep(1)
