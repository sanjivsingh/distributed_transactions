# archival_worker_insert.py - Inserts search events into analysis Elasticsearch
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json
import redis
from fastapi import HTTPException

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


# Redis connection
from setup.redis_setup import config as redis_config, constants as redis_constants
try:
    redis_client = redis.Redis(
        host=redis_config.configurations[redis_constants.REDIS_SERVER],
        port=redis_config.configurations[redis_constants.REDIS_PORT],
        decode_responses=True
    )
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Redis connection failed: {str(e)}")



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

    # Save to Redis (user recent) - use Lua script for atomic operation
    key = f"user:{data['username']}:searches"
    # Lua script for updating recent searches
    update_recent_searches_script = """
    local key = KEYS[1]
    local query = ARGV[1]
    redis.call('LREM', key, 0, query)
    redis.call('LPUSH', key, query)
    redis.call('LTRIM', key, 0, 9)
    """
    redis_client.eval(update_recent_searches_script, 1, key, data['query'])
    
    # Update global popular
    redis_client.zincrby("global:popular_searches", 1, data['query'])

    # Commit offset
    consumer.commit(msg)