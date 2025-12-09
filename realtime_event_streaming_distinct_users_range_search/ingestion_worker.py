from confluent_kafka import Consumer
import time
import redis

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
import json
from setup.redis_setup import config as redis_config, constants as redis_constants


class FastPathWorker:
    def __init__(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": config.KAFKA_BROKER,
                "group.id": "fast-path",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False
            }
        )
        self.consumer.subscribe([config.USER_ACTIVITY_TOPIC])
        self.redis_client = redis.Redis(
            host=config.INGESTION_REDIS_SERVER,
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            decode_responses=True,
        )
        self.__current_bucket = None

    def _current_bucket(self)   :
        #minute_ts = int(time.time() // config.DISTINCT_USERS_BUCKET_SECONDS)
        minute_ts = time.strftime("%Y%m%d%H%M")
        new_bucket = f"{config.DISTINCT_USERS_MINUTE_HYPERLOGLOG_KEY}:{minute_ts}"
        if self.__current_bucket is None:
            self.__current_bucket = new_bucket

        if self.__current_bucket == new_bucket:
            return self.__current_bucket , self.__current_bucket

        print("Switching to new bucket:", new_bucket)
        temp = self.__current_bucket
        self.__current_bucket = new_bucket
        return temp, new_bucket

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            
            event = json.loads(msg.value().decode())
            print(time.time(), event["user_id"])
            old_bucket, bucket = self._current_bucket()
            self.redis_client.pfadd(bucket, event["user_id"])
            self.consumer.commit(msg)
            print(time.time(),f"Processed message: {msg.offset()} with user_id: {event['user_id']}",old_bucket, bucket)
            
            # publish to Redis Stream for CDC to MongoDB        
            if old_bucket != bucket:
                self.redis_client.xadd(
                    config.REDIS_STREAM_NAME,
                    fields={"bucket": old_bucket}
                )
                print(f"""Published bucket {old_bucket} to CDC Redis Stream {config.REDIS_STREAM_NAME}""")

if __name__ == "__main__":
    try:
        worker = FastPathWorker()
        worker.run()
    except KeyboardInterrupt:
        print("Shutting down worker...")    
    finally:
        worker.consumer.close()
