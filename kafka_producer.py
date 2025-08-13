# kafka_producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

def generate_shipment_data(shipment_id):
    # Simulate normal shipment data with occasional delays
    status_options = ["in_transit", "delayed", "delivered"]
    status = random.choices(status_options, weights=[0.8, 0.1, 0.1], k=1)[0]
    delay_hours = 0
    if status == "delayed":
        delay_hours = random.randint(1, 24) # Simulate 1 to 24 hour delay

    return {
        "shipment_id": shipment_id,
        "timestamp": datetime.now().isoformat(),
        "location": f"Lat:{random.uniform(30, 40):.2f},Lon:{random.uniform(-100, -80):.2f}",
        "status": status,
        "estimated_arrival": (datetime.now() + timedelta(days=random.randint(1, 5))).isoformat(),
        "actual_arrival": None if status != "delivered" else (datetime.now() + timedelta(days=random.randint(0, 4))).isoformat(),
        "delay_hours": delay_hours
    }

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print("Starting Kafka producer...")
for i in range(1, 101): # Prodduces 100 messages
    data = generate_shipment_data(f"SHIP{i:03d}")
    producer.send('supply_chain_events', value=data)
    print(f"Sent: {data['shipment_id']} - Status: {data['status']}")
    time.sleep(1) # Send one message per second

producer.flush()
print("Finished sending messages.")