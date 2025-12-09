import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


import redis
import pymongo
import json
import time
import config
from bson import Binary
from setup.redis_setup import config as redis_config, constants as redis_constants
from setup.mongodb_setup import config as mongo_config, constants as mongo_constants

class CDCRRedisSync:

    def __init__(self):
    
        self.redis_client = redis.Redis(
            host=config.INGESTION_REDIS_SERVER,
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            decode_responses=True,
        )
        
        # MongoDB connection
        self.mongo_client = pymongo.MongoClient(
            mongo_config.configurations[mongo_constants.SERVER],
            mongo_config.configurations[mongo_constants.PORT],
        )
        self.db = self.mongo_client[config.MONGO_DATABASE]
        self.collection = self.db[config.MONGO_COLLECTION]
        
        # Stream configuration
        self.stream_name = config.REDIS_STREAM_NAME
        self.consumer_group = config.REDIS_CONSUMER_GROUP
        self.consumer_name = f"consumer-{int(time.time())}"

        # Create consumer group if it doesn't exist
        try:
            self.redis_client.xgroup_create(self.stream_name, self.consumer_group, id='0', mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

    def run(self):
        print(f"Starting Redis stream consumer: {self.consumer_name}")
        
        while True:
            try:
                # Read from stream using consumer group
                messages = self.redis_client.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.stream_name: '>'},
                    count=10,  # Process 10 messages at a time
                    block=1000  # Block for 1 second
                )
                
                for stream, msgs in messages:
                    for msg_id, fields in msgs:
                        try:
                            # Process the message
                            document = self.process_message(msg_id, fields)
                            
                            # Insert into MongoDB
                            self.collection.insert_one(document)
                            print(f"Inserted document: {msg_id} into MongoDB fields: {fields}")
                            
                            # Acknowledge the message
                            self.redis_client.xack(self.stream_name, self.consumer_group, msg_id)
                            
                        except Exception as e:
                            print(f"Error processing message {msg_id}: {e}")
                            # TODO : Handle failed messages (could implement dead letter queue)
                            continue
                            
            except Exception as e:
                print(f"Error reading from stream: {e}")
                time.sleep(5)  # Wait before retrying

    def process_message(self, msg_id, fields):
        """Process Redis stream message and convert to MongoDB document"""
        bucket = fields.get('bucket')
        # get hyperloglog data from Redis
        bucket_data = self.redis_client.dump(bucket)  # Ensure the bucket exists

        # create MongoDB document
        document = {
            '_id': bucket,
            'data': { "bucket": bucket, "bucket_data" : bucket_data}
        }
        return document

    def close(self):
        """Clean up connections"""
        if self.redis_client:
            self.redis_client.close()
        if self.mongo_client:
            self.mongo_client.close()

if __name__ == "__main__":
    redis_sync = CDCRRedisSync()
    try:
        redis_sync.run()
    except KeyboardInterrupt:
        print("Shutting down...")
        redis_sync.close()