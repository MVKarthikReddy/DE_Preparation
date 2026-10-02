import json
import time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'order-checkout-service',
    'acks': 'all',  # Wait for full broker ISR persistence
    'retries': 3,
    'linger.ms': 10  # Micro-batching for higher throughput
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered order key={msg.key().decode('utf-8')} to "
              f"[{msg.topic()}] Partition={msg.partition()} Offset={msg.offset()}")

# Simulated customer orders
sample_orders = [
    {"order_id": "ORD-101", "user_id": "user_alpha", "item": "Mechanical Keyboard", "amount": 120.0},
    {"order_id": "ORD-102", "user_id": "user_beta", "item": "USB-C Dock", "amount": 85.0},
    {"order_id": "ORD-103", "user_id": "user_alpha", "item": "Keycaps Set", "amount": 35.0},  # Same user!
    {"order_id": "ORD-104", "user_id": "user_gamma", "item": "Gaming Mouse", "amount": 60.0},
    {"order_id": "ORD-105", "user_id": "user_beta", "item": "Mousepad XL", "amount": 25.0},   # Same user!
]
while True:
    for order in sample_orders:
        key = order["user_id"]
        payload = json.dumps(order)
        
        # Asynchronously produce message
        producer.produce(
            topic='orders',
            key=key.encode('utf-8'),
            value=payload.encode('utf-8'),
            callback=delivery_report
        )
        # Serve delivery callbacks from previous produce calls
        producer.poll(0)
        time.sleep(0.5)

# Wait for any outstanding messages to be delivered
producer.flush()
print("All orders produced successfully.")