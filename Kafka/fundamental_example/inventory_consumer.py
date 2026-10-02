import json
import time
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'inventory-service-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Explicit manual commit for reliability
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])

print("Inventory Service started. Listening for orders...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        order_data = json.loads(msg.value().decode('utf-8'))
        partition = msg.partition()
        offset = msg.offset()
        
        print(f"[Inventory] Reserving stock for Order {order_data['order_id']} "
              f"| Item: {order_data['item']} | (P={partition}, Offset={offset})")
        
        # Simulate inventory database update
        time.sleep(0.2)

        # Commit offset after successful business logic execution
        consumer.commit(message=msg, asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()