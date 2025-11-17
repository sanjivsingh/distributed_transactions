import time
import json
from confluent_kafka import Producer, Consumer, KafkaException, OFFSET_BEGINNING
from confluent_kafka.admin import (
    AdminClient,
    NewPartitions,
    NewTopic,
)  # ADDED NewTopic IMPORT
from threading import Thread


def ensure_topic_config(topic_name: str, num_partitions: int, num_replicas: int):
    """
    Ensures the topic exists and has the required number of partitions.
    Creates the topic if it's missing or increases partitions if necessary.
    """
    from . import config, constants

    admin_conf = {"bootstrap.servers": config.configurations[constants.KAFKA_BROKER]}

    admin_client = AdminClient(admin_conf)
    # 1. List topics to check for existence
    try:
        # metadata = admin_client.list_topics(topic=topic_name, timeout=5)
        # Use a general list to detect if the topic is created by the producer
        metadata = admin_client.list_topics(timeout=5)

        if topic_name not in metadata.topics:
            # --- TOPIC DOES NOT EXIST: CREATE IT ---
            print(
                f"   Topic '{topic_name}' not found. Attempting to CREATE with {num_partitions} partitions..."
            )

            new_topic = NewTopic(topic_name, num_partitions, num_replicas)
            futures = admin_client.create_topics([new_topic])

            for topic, future in futures.items():
                future.result()  # Blocking call to wait for completion
                print(
                    f"   ✅ Successfully CREATED topic '{topic}' with {num_partitions} partitions."
                )

        else:
            # --- TOPIC EXISTS: CHECK PARTITIONS ---
            current_partitions = len(metadata.topics[topic_name].partitions)
            print(
                f"   Topic '{topic_name}' current partition count: {current_partitions}"
            )

            if current_partitions < num_partitions:
                # --- ALTER: INCREASE PARTITIONS ---
                print(
                    f"   Attempting to INCREASE partitions from {current_partitions} to {num_partitions}..."
                )

                new_partitions = [NewPartitions(topic_name, num_partitions)]
                futures = admin_client.create_partitions(new_partitions)

                for topic, future in futures.items():
                    future.result()
                    print(
                        f"   ✅ Successfully INCREASED partitions for topic '{topic}' to {num_partitions}."
                    )
            elif current_partitions > num_partitions:
                print(
                    f"   Topic has {current_partitions} partitions, which is MORE than the desired {num_partitions}. Continuing."
                )
            else:
                print(
                    f"   Topic already has the required {num_partitions} partitions. Skipping alter."
                )

    except Exception as e:
        # Catch exceptions (e.g., broker unreachable, topic already exists/in progress)
        print(f"   ❌ Failed to configure topic '{topic_name}'. Reason: {e}")
        print(
            "   -> Execution will proceed, but may fail if the topic config is wrong."
        )


if __name__ == "__main__":
    from . import constants
    from . import config

    ensure_topic_config(
        config.configurations[constants.CONVERSION_TOPIC],
        num_partitions=config.configurations[constants.NUMBER_OF_PARTITIONS],
        num_replicas=config.configurations[constants.NUMBER_OF_REPLICAS],
    )
