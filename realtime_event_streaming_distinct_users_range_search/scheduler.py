import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import datetime
import time
import redis
import config
import pymongo
from bson import Binary
from setup.mongodb_setup import config as mongo_config, constants as mongo_constants
from setup.redis_setup import config as redis_config, constants as redis_constants

def merge_hyperloglogs(source_start_key, source_end_key, target_key):
    redis_client = redis.Redis(
        host=config.BATCH_REDIS_SERVER,
        port=redis_config.configurations[redis_constants.REDIS_PORT],
        # decode_responses=True,
        # keeping it binary safe for hyperloglog data
    )
    # MongoDB connection
    mongo_client = pymongo.MongoClient(
        mongo_config.configurations[mongo_constants.SERVER],
        mongo_config.configurations[mongo_constants.PORT],
    )
    db = mongo_client[config.MONGO_DATABASE]
    collection = db[config.MONGO_COLLECTION]

    # range search logic for hourly distinct users 
    # Perform the range query
    query = {"_id": {"$gte": source_start_key, "$lte": source_end_key}}
    results = collection.find(query)

    # Iterate and print the results
    print(f"Documents with _id between {source_start_key} and {source_end_key}:")
    hll_keys = []
    for doc in results:
        if 'data' in doc:   
            print(doc['_id'], doc['data'])
            bucket_data = doc['data']['bucket_data']
            # Handle Binary type from MongoDB
            if isinstance(bucket_data, Binary):
                bucket_data = bucket_data.value
            bucket = doc["data"]["bucket"]
            
            # delete if exists to avoid BUSYKEY error
            redis_client.delete(bucket)
            # restore the HyperLogLog from dumped data
            redis_client.restore(bucket, 0, bucket_data)
            hll_keys.append(bucket)

    # The first argument must be the destination key. Extend the list with all the source keys found
    command_args = [target_key] 
    command_args.extend(hll_keys)
    redis_client.pfmerge(*command_args)

    # store in mongoDB using dump
    dumped_data = redis_client.dump(target_key)
    document = {
        '_id': target_key,
        'data': { "bucket": target_key, "bucket_data" : dumped_data}
    }
    collection.insert_one(document)

    # delete temporary hourly hyperloglog
    redis_client.delete(target_key)

    return hll_keys

class Scheduler:
    def __init__(self):
        self.redis_client_batch = redis.Redis(
            host=config.BATCH_REDIS_SERVER,
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            # decode_responses=True,
            # keeping it binary safe for hyperloglog data
        )

        self.redis_client_ingestion = redis.Redis(
            host=config.INGESTION_REDIS_SERVER,
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            # decode_responses=True,
            # keeping it binary safe for hyperloglog data
        )

    def hourly_scheduler(self):
        # range search logic for hourly distinct users 
        # Perform the range query
        now = datetime.datetime.now()
        start_time = (now - datetime.timedelta(hours=1)).strftime("%Y%m%d%H00")
        start_key = f"{config.DISTINCT_USERS_MINUTE_HYPERLOGLOG_KEY}:{start_time}"
        end_time = (now - datetime.timedelta(hours=1)).strftime("%Y%m%d%H59")
        end_key = f"{config.DISTINCT_USERS_MINUTE_HYPERLOGLOG_KEY}:{end_time}"
        hourly_time = now.strftime("%Y%m%d%H")
        hourly_bucket = f"{config.DISTINCT_USERS_HOUR_HYPERLOGLOG_KEY}:{hourly_time}"
        min_hll_keys = merge_hyperloglogs(start_key, end_key, hourly_bucket)

        print(f"Hourly distinct users hyperloglog created for hour {hourly_bucket} by merging {min_hll_keys}")
        for key in min_hll_keys:
            # delete temporary keys
            self.redis_client_batch.delete(key)
            # delete from ingestion redis as well
            self.redis_client_ingestion.delete(key)
            print(f"Deleted key: {key}")

    def daily_scheduler(self):
        # Perform the range query
        now = datetime.datetime.now()
        start_time = (now - datetime.timedelta(days=1)).strftime("%Y%m%d00")
        start_key = f"{config.DISTINCT_USERS_HOUR_HYPERLOGLOG_KEY}:{start_time}"
        end_time = (now - datetime.timedelta(days=1)).strftime("%Y%m%d23")
        end_key = f"{config.DISTINCT_USERS_HOUR_HYPERLOGLOG_KEY}:{end_time}"
        daily_time = now.strftime("%Y%m%d")
        daily_bucket = f"{config.DISTINCT_USERS_DAY_HYPERLOGLOG_KEY}:{daily_time}"
        hour_hll_keys = merge_hyperloglogs(start_key, end_key, daily_bucket) 
        print(f"Daily distinct users hyperloglog created for day {daily_bucket} by merging {hour_hll_keys}")

    def monthly_scheduler(self):
        # Perform the range query
        now = datetime.datetime.now()
        start_time = (now - datetime.timedelta(days=30)).strftime("%Y%m01")
        start_key = f"{config.DISTINCT_USERS_DAY_HYPERLOGLOG_KEY}:{start_time}"
        end_time = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
        end_key = f"{config.DISTINCT_USERS_DAY_HYPERLOGLOG_KEY}:{end_time}"
        monthly_time = now.strftime("%Y%m")
        monthly_bucket = f"{config.DISTINCT_USERS_MONTH_HYPERLOGLOG_KEY}:{monthly_time}" 
        daily_hll_keys = merge_hyperloglogs(start_key, end_key, monthly_bucket) 
        print(f"Monthly distinct users hyperloglog created for month {monthly_bucket} by merging {daily_hll_keys}")

    def yearly_scheduler(self):
        # Perform the range query
        now = datetime.datetime.now()
        start_time = (now - datetime.timedelta(days=365)).strftime("%Y01")
        start_key = f"{config.DISTINCT_USERS_MONTH_HYPERLOGLOG_KEY}:{start_time}"
        end_time = (now - datetime.timedelta(days=365)).strftime("%Y12")
        end_key = f"{config.DISTINCT_USERS_MONTH_HYPERLOGLOG_KEY}:{end_time}"
        yearly_time = now.strftime("%Y")
        yearly_bucket = f"{config.DISTINCT_USERS_YEAR_HYPERLOGLOG_KEY}:{yearly_time}"
        monthly_hll_keys = merge_hyperloglogs(start_key, end_key, yearly_bucket) 
        print(f"Yearly distinct users hyperloglog created for year {yearly_bucket} by merging {monthly_hll_keys}")

    def run(self):
        last_hourly = time.time()
        last_daily = time.time()
        last_monthly = time.time()
        last_yearly = time.time()
        while True:
            now = time.time()
            # Run hourly every 3600 seconds (1 hour)
            if now - last_hourly >= 3600:
                self.hourly_scheduler()
                last_hourly = now
            # Run daily every 86400 seconds (1 day)
            if now - last_daily >= 86400:
                self.daily_scheduler()
                last_daily = now
            # Run monthly approximately every 2592000 seconds (30 days)
            if now - last_monthly >= 2592000:
                self.monthly_scheduler()
                last_monthly = now
            # Run yearly approximately every 31536000 seconds (365 days)
            if now - last_yearly >= 31536000:
                self.yearly_scheduler()
                last_yearly = now
            time.sleep(60)  # Check every minute

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.run()