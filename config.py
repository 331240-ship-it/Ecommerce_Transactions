"""Shared configuration for the producer, consumer and topic setup."""

# Host-side addresses of the 3 brokers started by docker-compose.yml
BOOTSTRAP_SERVERS = ["localhost:9092"]

PARTITIONS = 3
REPLICATION_FACTOR = 3

TOPICS = [
    "order-events",
    "catalogue-events",
    "inventory-events",
    "shipment-events",
    "payment-events",
    "customer-service-events",
]
