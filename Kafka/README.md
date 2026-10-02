Kafka Fundamentals
---

### 1. Record (Event)

* **Definition:** The fundamental, atomic unit of data in Kafka. A record represents an immutable state change or domain event stored as raw bytes. It consists of a **Key** (optional), a **Value** (payload), a **Timestamp** (creation or ingestion time), and optional **Headers** (key-value metadata).
* **Example:**
```json
{
  "key": "cust_849201",
  "value": {
    "transaction_id": "tx_99812",
    "amount": 250.00,
    "currency": "USD",
    "type": "DEBIT"
  },
  "timestamp": 1775143200000,
  "headers": { "trace_id": "c4b9-8e21-44df" }
}

```



---

### 2. Topic

* **Definition:** A named, logical category, stream, or channel to which records are published and from which records are consumed. Topics in Kafka are multi-producer and multi-subscriber, and records written to them are append-only and durable.
* **Example:**
* `payment.settlements`: Holds financial ledger events.
* `fleet.telemetry.gps`: Holds raw device sensor coordinates.



---

### 3. Partition

* **Definition:** The physical subdivision and fundamental unit of scalability and concurrency within a topic. A partition is an ordered, immutable, append-only sequence of records continually maintained on broker disk segments. Kafka guarantees strict FIFO ordering **only within a single partition**, not across distinct partitions of the same topic.
* **Example:**
* Topic `payment.settlements` is split into **4 physical partitions**:
```
Topic: payment.settlements
├── Partition 0: [Offset 0, Offset 1, Offset 2, ...]
├── Partition 1: [Offset 0, Offset 1, Offset 2, ...]
├── Partition 2: [Offset 0, Offset 1, Offset 2, ...]
└── Partition 3: [Offset 0, Offset 1, Offset 2, ...]

```





---

### 4. Partition Key & Hashing

* **Definition:** An optional identifier supplied with a record that controls deterministic routing to a specific partition. By default, Kafka applies the `murmur2` hashing algorithm on the serialized key bytes:

$$\text{Target Partition} = \left(\text{murmur2}(\text{serializedKey}) \ \& \ \text{0x7fffffff}\right) \pmod{\text{numPartitions}}$$



Records sharing the same non-null key are guaranteed to land on the identical partition in strict chronological order.
* **Example:**
* Record A: `key = "cust_849201"` $\to$ `murmur2("cust_849201") % 4` $\to$ **Partition 2**
* Record B: `key = "cust_849201"` $\to$ `murmur2("cust_849201") % 4` $\to$ **Partition 2**
* Record C: `key = "cust_110943"` $\to$ `murmur2("cust_110943") % 4` $\to$ **Partition 0**



---

### 5. Offset

* **Definition:** A sequential, monotonically increasing 64-bit integer assigned by the broker to each record upon append within a specific partition. Offsets act as immutable record identifiers and track consumer progress through consumer commit positions stored in the internal `__consumer_offsets` topic.
* **Example:**
* A consumer reading Partition 2 retrieves records with offsets `104`, `105`, and `106`.
* After processing, the consumer commits offset `107` (the next expected record).



---

### 6. Producer

* **Definition:** A client application that serializes, batches, and transmits records over TCP to Kafka cluster partition leaders. The producer controls durability constraints via the `acks` configuration parameter (`0`, `1`, or `all`/`-1`).
* **Example:**
* A Go or Python microservice serving checkout requests batches and sends payment payloads:
```python
producer.produce(
    topic="payment.settlements",
    key=b"cust_849201",
    value=b'{"amount": 250.00}',
    callback=on_delivery
)

```





---

### 7. Consumer & Consumer Group

* **Definition:**
* **Consumer:** A client application that pulls records in batches by polling one or more topic partitions using assigned offsets.
* **Consumer Group:** A set of cooperating consumers sharing the same `group.id` that divide the partitions of subscribed topics among themselves so that each partition is assigned to **at most one consumer** in the group.


* **Example:**
* Topic `payment.settlements` has **4 partitions** (`P0`, `P1`, `P2`, `P3`).
* Consumer Group `fraud-detection-service` has **2 instances**:
* Consumer A is assigned `P0` and `P1`.
* Consumer B is assigned `P2` and `P3`.


* Consumer Group `ledger-service` has **4 instances**:
* Consumers 1, 2, 3, and 4 each get exactly 1 partition.





---

### 8. Broker & Cluster

* **Definition:**
* **Broker:** A server instance running the Kafka process that stores log segments on disk, serves client read/write network requests, and coordinates replication.
* **Cluster:** A collection of multiple Kafka brokers operating collectively to provide distributed storage, horizontal scalability, and fault tolerance.


* **Example:**
* A cluster consisting of 3 nodes: `broker-101.corp:9092`, `broker-102.corp:9092`, and `broker-103.corp:9092`.



---

### 9. Replication: Leader, Follower & ISR

* **Definition:**
* **Replication Factor (RF):** The total number of copies maintained for a partition across distinct brokers.
* **Leader:** The broker replica responsible for handling all producer writes and standard consumer reads for a given partition.
* **Follower:** Replicas that act as consumers to fetch and replicate log entries from the partition leader.
* **In-Sync Replicas (ISR):** The subset of follower replicas that are fully caught up with the leader's log end offset (LEO) within the configured time boundary (`replica.lag.time.max.ms`).


* **Example:**
* Partition `P0` with $\text{RF} = 3$:
* Leader: Broker 101
* Followers: Broker 102, Broker 103
* ISR List: `[101, 102, 103]`


* If Broker 101 crashes, KRaft automatically promotes an in-sync replica (e.g., Broker 102) to become the new Leader without data loss.



---

### 10. KRaft (Kafka Raft Metadata Mode)

* **Definition:** Kafka's built-in event-driven consensus mechanism based on the Raft protocol that manages cluster metadata, controller quorum elections, and broker registrations. KRaft eliminates the external Apache ZooKeeper coordination dependency and stores metadata as an internal, replicated Kafka log topic (`@metadata`).
* **Example:**
* Instead of maintaining a separate ZooKeeper ensemble, a subset of brokers run with `process.roles=controller,broker` to vote, form a quorum, and maintain cluster state consensus directly.
