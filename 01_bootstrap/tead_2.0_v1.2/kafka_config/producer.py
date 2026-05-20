import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

TOPIC = 'web_events'
BROKER = 'localhost:19092'

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event_types = ['view_item', 'add_to_cart', 'purchase']
item_ids = [f"PROD_{i:03d}" for i in range(1, 20)]

print(f"Producer started. Sending events to {TOPIC}...")

try:
    while True:
        customer_id = f"USER_{random.randint(100, 150)}"
        event_type = random.choice(event_types)
        item_id = random.choice(item_ids)
        event_id = f"EVT_{random.getrandbits(32)}"
        event_time = datetime.utcnow().isoformat()

        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "customer_id": customer_id,
            "item_id": item_id,
            "amount": round(random.uniform(5.0, 150.0), 2) if event_type == 'purchase' else 0.0,
            "event_time": event_time
        }

        # 10% chance of duplicate (simulates real-world late/retry)
        if random.random() < 0.10:
            producer.send(TOPIC, payload)
            print(f"  [DUP] Resending event {event_id}")

        producer.send(TOPIC, payload)
        print(f"Sent: {event_type} | User: {customer_id} | Time: {event_time}")

        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()
    producer.close()
