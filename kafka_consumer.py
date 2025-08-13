# kafka_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'supply_chain_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest', # Start from the beginning of the topic
    enable_auto_commit=True,
    group_id='my-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting Kafka consumer...")
for message in consumer:
    print(f"Received: {message.value}")