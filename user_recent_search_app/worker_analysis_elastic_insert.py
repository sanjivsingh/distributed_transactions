# archival_worker_insert.py - Inserts search events into analysis Elasticsearch
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json

from setup.elasticsearch_setup import (
    config as elasticsearch_config,
    constants as elasticsearch_constants,
)
from setup.kafka_setup import config as kafka_config, constants as kafka_constants

es = Elasticsearch(
    [
        {
            "host": elasticsearch_config.configurations[
                elasticsearch_constants.ES_HOST
            ],
            "port": elasticsearch_config.configurations[
                elasticsearch_constants.ES_PORT
            ],
            "scheme": "http",
        }
    ]
)

consumer_conf = {
    "bootstrap.servers": kafka_config.configurations[kafka_constants.KAFKA_BROKER],
    "group.id": "insert_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(consumer_conf)
consumer.subscribe(["user_searches"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            break

    data = json.loads(msg.value().decode("utf-8"))
    # Store in analysis Elasticsearch
    es.index(index="search_analysis", body=data)
    print(f"Inserted search event into analysis ES: {data}")

    # Commit offset
    consumer.commit(msg)