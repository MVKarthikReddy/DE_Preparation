## 3. Project Architecture

We are building an **E-Commerce Real-Time Order Processing System**:

```text
                     [ Producer: order_service ]
                                  │
                   (Key: user_id ──► murmur2 hash)
                                  │
                                  ▼
                     Topic: 'orders' (3 Partitions)
           ┌──────────────────────┼──────────────────────┐
           ▼                      ▼                      ▼
     Partition 0            Partition 1            Partition 2
     [Off 0, 1, 2...]       [Off 0, 1, 2...]       [Off 0, 1, 2...]
           │                      │                      │
           ├──────────────────────┼──────────────────────┤
           │                      │                      │
           ▼                      ▼                      ▼
    Consumer Group 1:                     Consumer Group 2:
    'inventory-service-group'             'notification-service-group'
    - Reserves stock in DB                - Sends email receipts
    - Manual offset commit (`acks`)       - Auto-commit enabled

```

---

## 4. Setup & Installation

### Prerequisites

* [Docker Desktop](https://www.docker.com/) installed and running.
* Python 3.9+ installed.

### Step 1: Create Project Folder & Files

```bash
mkdir kafka-starter-project && cd kafka-starter-project

```

### Step 2: Create `docker-compose.yml`

Save this file to run a modern single-node Kafka broker using KRaft mode:

```yaml
version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka-kraft
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'

```

### Step 3: Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate       # On Windows: venv\Scripts\activate
pip install confluent-kafka

```

---

## 5. Application Code

### File 1: `producer.py`

Publishes orders using `user_id` as the partition key.

```python
import json
import time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'order-checkout-service',
    'acks': 'all',  # Wait for leader & followers to write to disk
    'retries': 3,
    'linger.ms': 10
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Published: Key={msg.key().decode('utf-8')} -> "
              f"[{msg.topic()}] Partition={msg.partition()} Offset={msg.offset()}")

sample_orders = [
    {"order_id": "ORD-101", "user_id": "user_alpha", "item": "Mechanical Keyboard", "amount": 120.0},
    {"order_id": "ORD-102", "user_id": "user_beta", "item": "USB-C Dock", "amount": 85.0},
    {"order_id": "ORD-103", "user_id": "user_alpha", "item": "Custom Keycaps", "amount": 35.0},  # Same key as 101
    {"order_id": "ORD-104", "user_id": "user_gamma", "item": "Gaming Mouse", "amount": 60.0},
    {"order_id": "ORD-105", "user_id": "user_beta", "item": "Desk Mat", "amount": 25.0},         # Same key as 102
]

for order in sample_orders:
    key = order["user_id"]
    payload = json.dumps(order)
    
    producer.produce(
        topic='orders',
        key=key.encode('utf-8'),
        value=payload.encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(0.4)

producer.flush()
print("All sample orders sent.")

```

### File 2: `inventory_consumer.py`

Represents the Inventory Service. It manually commits offsets after business logic executes to prevent data loss.

```python
import json
import time
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'inventory-service-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Manual commit ensures processing before advancing offset
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
            print(f"Consumer error: {msg.error()}")
            break

        order_data = json.loads(msg.value().decode('utf-8'))
        print(f"[Inventory] Reserving stock for {order_data['item']} "
              f"(Order: {order_data['order_id']}, Partition: {msg.partition()}, Offset: {msg.offset()})")
        
        time.sleep(0.2)  # Simulate database write
        
        # Explicitly save progress to Kafka
        consumer.commit(message=msg, asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

```

### File 3: `notification_consumer.py`

Represents an independent Notification Service with its own group ID and independent progress.

```python
import json
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'notification-service-group',  # Separate group = Independent offsets
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
        print(f"[Alert] Email receipt sent to {order_data['user_id']} for ${order_data['amount']}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

```

---

## 6. How to Run & Test

### Step 1: Start the Kafka Container

```bash
docker compose up -d

```

Verify the container is healthy:

```bash
docker ps

```

### Step 2: Create the Partitioned Topic

Create the `orders` topic with **3 partitions**:

```bash
docker exec -it kafka-kraft kafka-topics \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

```

### Step 3: Run the Consumers

Open two separate terminal windows:

* **Terminal 1:**
```bash
python inventory_consumer.py

```


* **Terminal 2:**
```bash
python notification_consumer.py

```



### Step 4: Run the Producer

Open a third terminal window:

```bash
python producer.py

```

---

## 7. Key Observations to Verify

1. **Deterministic Partition Hashing:**
In Terminal 3, check `ORD-101` and `ORD-103`. Because both use `key="user_alpha"`, both messages land on the **exact same partition** in order.
2. **Fan-Out (Multiple Groups):**
Both `inventory_consumer.py` and `notification_consumer.py` process all 5 orders without competing with each other.
3. **Partition Balancing (Scaling):**
* Keep running the producer.
* Open a fourth terminal and run a second instance of `inventory_consumer.py`.
* Kafka automatically performs a **rebalance**, splitting the 3 partitions between the two workers.



---

## 8. Teardown

To stop consumers, press `Ctrl+C` in each terminal.

To stop and remove the Kafka broker and its storage volume:

```bash
docker compose down -v

```

```

<FollowUp label="Want me to show how to inspect internal Kafka consumer lag and partition offsets using CLI commands?" query="Show me the exact kafka-consumer-groups CLI commands to inspect consumer lag, active member IDs, and partition assignments."/>

```
