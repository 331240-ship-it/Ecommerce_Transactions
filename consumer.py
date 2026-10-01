"""
Kafka -> MongoDB consumer.

Reads all six topics and stores every event in MongoDB:
    database   : ecommerce
    collections: one per topic (order-events, payment-events, ...)

Each document = the original event + Kafka metadata + parsed event_time.
A unique index on (kafka_partition, kafka_offset) prevents duplicates if a
message is re-delivered. Offsets are committed only AFTER the write succeeds.

Usage:
    pip install pymongo
    python consumer_mongo.py                 # from beginning
    python consumer_mongo.py --latest        # only new events
    python consumer_mongo.py --mongo-uri mongodb://localhost:27017
"""
import argparse
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError

import config

# Natural-id field of each topic -> indexed for fast lookups / joins
LOOKUP_FIELD = {
    "order-events": "order_id",
    "payment-events": "order_id",
    "shipment-events": "order_id",
    "catalogue-events": "product_id",
    "inventory-events": "product_id",
    "customer-service-events": "ticket_id",
}


def setup_indexes(db, topics):
    for t in topics:
        col = db[t]
        col.create_index([("kafka_partition", ASCENDING), ("kafka_offset", ASCENDING)],
                         unique=True, name="uniq_kafka_position")
        col.create_index([("event_time", ASCENDING)], name="event_time")
        field = LOOKUP_FIELD.get(t)
        if field:
            col.create_index([(field, ASCENDING)], name=field)


def to_document(msg):
    doc = dict(msg.value)                      # original event fields
    doc["kafka_key"] = msg.key
    doc["kafka_partition"] = msg.partition
    doc["kafka_offset"] = msg.offset
    doc["ingested_at"] = datetime.now(timezone.utc)
    try:                                       # real datetime for time queries
        doc["event_time"] = datetime.strptime(
            msg.value["timestamp"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
    except Exception:
        doc["event_time"] = doc["ingested_at"]
    return doc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", nargs="+", default=config.TOPICS)
    ap.add_argument("--latest", action="store_true", help="start at the end")
    ap.add_argument("--group", default="mongo-writer-group")
    ap.add_argument("--mongo-uri", default="mongodb+srv://mongoadmin:mongo240@cluster0.zfpw84m.mongodb.net/")
    ap.add_argument("--db", default="ecommerce")
    args = ap.parse_args()

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")               # fail fast if Mongo is down
    db = client[args.db]
    setup_indexes(db, args.topic)
    print(f"Connected to MongoDB: {args.mongo_uri} / db '{args.db}'")

    consumer = KafkaConsumer(
        *args.topic,
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset="latest" if args.latest else "earliest",
        enable_auto_commit=False,              # commit manually after DB write
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    saved, dupes = 0, 0
    print(f"Listening on: {', '.join(args.topic)}  (Ctrl+C to stop)\n")

    try:
        for msg in consumer:
            try:
                db[msg.topic].insert_one(to_document(msg))
                saved += 1
            except DuplicateKeyError:
                dupes += 1                     # already stored earlier
            consumer.commit()

            print(f"saved -> {msg.topic:<24} p{msg.partition} off={msg.offset} "
                  f"{msg.value.get('event_type')}")
            if (saved + dupes) % 50 == 0:
                print(f"--- total saved: {saved}, duplicates skipped: {dupes} ---")
    except KeyboardInterrupt:
        print(f"\nStopped. Saved {saved} events, skipped {dupes} duplicates.")
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()