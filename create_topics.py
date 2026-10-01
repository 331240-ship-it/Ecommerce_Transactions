"""Create the six Kafka topics (3 partitions, replication factor 3)."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import config


def main():
    admin = KafkaAdminClient(bootstrap_servers=config.BOOTSTRAP_SERVERS)
    existing = set(admin.list_topics())

    for name in config.TOPICS:
        if name in existing:
            print(f"[skip]    {name} already exists")
            continue
        try:
            admin.create_topics([
                NewTopic(name=name,
                         num_partitions=config.PARTITIONS,
                         replication_factor=config.REPLICATION_FACTOR)
            ])
            print(f"[created] {name}")
        except TopicAlreadyExistsError:
            print(f"[skip]    {name} already exists")

    print("\nTopics now on the cluster:")
    for t in sorted(t for t in admin.list_topics() if not t.startswith("__")):
        print("  -", t)
    admin.close()


if __name__ == "__main__":
    main()
