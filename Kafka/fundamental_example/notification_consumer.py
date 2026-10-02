import json
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'notification-service-group',  # Different group = Independent offsets!
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])

print("Notification Service started. Listening for customer alerts...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        order_data = json.loads(msg.value().decode('utf-8'))
        print(f"[Alert] Sending receipt to {order_data['user_id']} for "
              f"{order_data['item']} (${order_data['amount']})")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()