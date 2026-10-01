# E-Commerce Kafka Streaming Project (Assignment 2)

## Files
| File | Purpose |
|---|---|
| `docker-compose.yml` | 3-broker Kafka cluster (KRaft, no ZooKeeper) |
| `config.py` | Brokers, topic names, partitions, replication factor |
| `create_topics.py` | Creates the 6 topics (3 partitions, RF=3) |
| `producer.py` | Generates related sample events and streams them to Kafka |
| `consumer.py` | Reads the topics back to verify delivery |

## Run (Docker + Python 3.9+)
```bash
# 0. Install dependency
pip install -r requirements.txt

# 1. Start the Kafka cluster (wait ~20s)
docker compose up -d
docker compose ps

# 2. Create topics
python create_topics.py

# 3. Terminal A: start the consumer
python consumer.py --latest

# 4. Terminal B: start the producer
python producer.py --events 200

# Stop everything
docker compose down -v
```

## No Docker? Test the generator only
```bash
python producer.py --dry-run --events 20
```

## Inspect with Kafka CLI tools (inside the container)
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe --topic order-events

docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 --topic order-events \
  --from-beginning --property print.key=true
```

## Partition keys
| Topic | Key |
|---|---|
| order-events, payment-events, shipment-events | `order_id` |
| catalogue-events | `product_id` |
| inventory-events | `warehouse_id:product_id` |
| customer-service-events | `ticket_id` |
