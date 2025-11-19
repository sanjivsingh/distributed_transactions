from collections import defaultdict
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
import pymongo
import json
import re
import threading
import time
from commons import logger
import concurrent.futures

log = logger.setup_logger(__name__)


dbtask_lock = threading.Lock()

post_count_threshold = 10
post_count = 0

pending_map = defaultdict(int)
inprogress_map = defaultdict(int)
pending_msgs = []
inprogress_msgs = []

# MongoDB connection
from setup.mongodb_setup import config as mongo_config, constants as mongo_constants

mongo_client = pymongo.MongoClient(
    mongo_config.configurations[mongo_constants.SERVER],
    mongo_config.configurations[mongo_constants.PORT],
)
mongo_db = mongo_client["instagram_db"]
hashtag_collection = mongo_db["hashtags"]

from setup.kafka_setup import config as kafka_config, constants as kafka_constants

def update_db_hashtag_postcounts(consumer):
    global inprogress_map, inprogress_msgs, dbtask_lock
    log.info(f"Updating hashtag post counts in MongoDB: {dict(inprogress_map)}")
    with dbtask_lock:
        try:
            time.sleep(10)  # Simulate some delay
            log.info(f"Updating hashtag post counts rwady to run")
            for tag, post_count in inprogress_map.items():
                hashtag_collection.update_one(
                    {"_id": tag},
                    {"$inc": {"number_of_posts": post_count}},
                    upsert=True,
                )
                log.info(f"Updated hashtag: {tag} with post count increment: {post_count}")
            for msg in inprogress_msgs:
                consumer.commit(message=msg)
                log.info(f"Committed offset for message: {msg.offset()}")
            inprogress_map.clear()
            inprogress_msgs.clear()
            log.info("Completed background task to update hashtag post counts")
        except Exception as e:
            log.error(f"Error in updating hashtag post counts: ", e)

class HashtagWorker:
    def __init__(self):
        consumer_conf = {
            "bootstrap.servers": kafka_config.configurations[
                kafka_constants.KAFKA_BROKER
            ],
            "group.id": "hashtag_group",
            "auto.offset.reset": "earliest",  # Start from the earliest if no committed offset is found
            "enable.auto.commit": False,  # Disable auto-commit for manual control
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe(["instagram_posts"])

    def run(self):
        global post_count, pending_map, inprogress_map, pending_msgs, inprogress_msgs, dbtask_lock

        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event - not an error
                    log.warning(
                        f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}"
                    )
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                post_data = json.loads(msg.value().decode("utf-8"))
                post_id = post_data["post_id"]
                caption = post_data["caption"]
                # image_ids = post_data["image_ids"]

                # Extract hashtags
                hashtags = re.findall(r"#\w+", caption)
                log.info(f"post_count : {post_count} post_id : {post_id} Extracted hashtags: {hashtags}")
                for tag in hashtags:
                    tag = tag.lower()  # Normalize to lowercase
                    pending_map[tag] += 1
                pending_msgs.append(msg)
                post_count += 1
            except Exception as e:
                # retry or push it to dead letter queue
                log.error(f"Error processing message : {msg}", e)

            if post_count >= post_count_threshold:
                if dbtask_lock.acquire(blocking=False):
                    try:
                        self.update_hashtag_postcounts(self.consumer)
                    finally:
                        dbtask_lock.release()
                # If lock not acquired, skip and continue to next message
                log.info("Waiting for next batch of posts...")  

    def update_hashtag_postcounts(self, consumer):
        global post_count, pending_map, inprogress_map, pending_msgs, inprogress_msgs, dbtask_lock
        log.info(
            f"Received {post_count} posts. Current hashtag post counts: {dict(pending_map)}"
        )
        inprogress_map, pending_map = pending_map, inprogress_map
        inprogress_msgs, pending_msgs = pending_msgs, inprogress_msgs

        # Update MongoDB asynchronously
        task = concurrent.futures.ThreadPoolExecutor().submit(
            update_db_hashtag_postcounts, self.consumer
        )
        log.info("Started background task to update hashtag post counts")
        post_count = 0
        return task


# Start worker in a thread
if __name__ == "__main__":
    worker = HashtagWorker()
    try:
        worker.run()
    except KeyboardInterrupt as e:
        log.error("Shutting down worker...", e)
    finally:
        log.info("exiting....")
        if post_count > 0:
            task = worker.update_hashtag_postcounts(worker.consumer)
            task.result()  # Wait for the background task to complete
        if worker.consumer:
            worker.consumer.close()
        if mongo_client:
            mongo_client.close()
