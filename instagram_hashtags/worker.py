from confluent_kafka import Consumer
import pymongo
import json
import re

# MongoDB connection
from mongodb_setup import config as mongo_config, constants as mongo_constants
mongo_client = pymongo.MongoClient(
    mongo_config.configurations[mongo_constants.SERVER],
    mongo_config.configurations[mongo_constants.PORT]
)
mongo_db = mongo_client["instagram_db"]
hashtag_collection = mongo_db["hashtags"]


from kafka_setup import config as kafka_config, constants as kafka_constants


class HashtagWorker:
    def __init__(self):
        consumer_conf = {
            "bootstrap.servers": kafka_config.configurations[
                kafka_constants.KAFKA_BROKER
            ],
            "group.id": "hashtag_group",
            "auto.offset.reset": "earliest",
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe(["posts"])

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            post_data = json.loads(msg.value().decode("utf-8"))
            post_id = post_data["post_id"]
            caption = post_data["caption"]
            image_ids = post_data["image_ids"]

            # Extract hashtags
            hashtags = re.findall(r"#\w+", caption)
            for tag in hashtags:
                tag = tag.lower()  # Normalize to lowercase
                # Update MongoDB
                hashtag_collection.update_one(
                    {"_id": tag},
                    {
                        "$inc": {"number_of_posts": 1},
                        "$push": {
                            "top_hundred_posts": {
                                "post_id": post_id,
                                "image_id": image_ids[0] if image_ids else None,
                            }
                        },
                    },
                    upsert=True,
                )
                # Keep only top 100 (assuming recent, but for simplicity, limit to 100)
                hashtag_collection.update_one(
                    {"_id": tag},
                    {"$push": {"top_hundred_posts": {"$each": [], "$slice": -100}}},
                )


# Start worker in a thread
worker = HashtagWorker()
worker.run()
