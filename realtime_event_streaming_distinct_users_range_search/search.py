import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import redis
import pymongo
import time
import config
from bson import Binary
from setup.redis_setup import config as redis_config, constants as redis_constants
from setup.mongodb_setup import config as mongo_config, constants as mongo_constants


class SearchService:

    def __init__(self):
        # Redis connection
        self.search_redis_client = redis.Redis(
            host=config.SEARCH_REDIS_SERVER,
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            decode_responses=True,
        )

        self.ingestion_redis_client = redis.Redis(
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

    def generate_minute_windows(self, start_timestamp, end_timestamp) -> list[str]:

        from datetime import datetime, timedelta
        start = datetime.strptime(start_timestamp, '%Y/%m/%d %H:%M')
        end = datetime.strptime(end_timestamp, '%Y/%m/%d %H:%M')
        if start > end:
            return []

        minutes = [(start.year,start.month ,start.day,start.hour,m) for m  in range(start.minute, end.minute+1)]
        windows =[]
        for minute in minutes:
            minute_str = f"{config.DISTINCT_USERS_MINUTE_HYPERLOGLOG_KEY}:{minute[0]:04d}{minute[1]:02d}{minute[2]:02d}{minute[3]:02d}{minute[4]:02d}"
            windows.append(minute_str)
        return windows

    def generate_hours_windows(self, start_timestamp, end_timestamp) -> list[str]:
        from datetime import datetime, timedelta
        start = datetime.strptime(start_timestamp, '%Y/%m/%d %H:%M')
        end = datetime.strptime(end_timestamp, '%Y/%m/%d %H:%M')
        if start > end:
            return []
        hours = [(start.year,start.month ,start.day,h) for h  in range(start.hour + (1 if start.minute > 0 else 0), end.hour+(1 if end.minute=="59" else 0))]
        if hours:
            min_hour = min(hours)
            max_hour = max(hours)
            minutes =  self.generate_minute_windows(start_timestamp,self.__sub_date_parts( f"{min_hour[0]:04d}/{min_hour[1]:02d}/{min_hour[2]:02d} {min_hour[3]:02d}:59" , "hour",-1))
            minutes.extend(self.generate_minute_windows(self.__sub_date_parts(f"{max_hour[0]:04d}/{max_hour[1]:02d}/{max_hour[2]:02d} {max_hour[3]:02d}:00", "hour",1), end_timestamp))
        else:
            end_temp = end.replace(minute=0)
            end_temp -= timedelta(minutes=1)
            minutes =  self.generate_minute_windows(start_timestamp,end_temp.strftime('%Y/%m/%d %H:%M'))
            minutes.extend(self.generate_minute_windows(end.replace(minute=0).strftime('%Y/%m/%d %H:%M'), end_timestamp))

        windows =[]
        for hour in hours:
            hour_str = f"{config.DISTINCT_USERS_HOUR_HYPERLOGLOG_KEY}:{hour[0]:04d}{hour[1]:02d}{hour[2]:02d}{hour[3]:02d}"
            windows.append(hour_str)
        windows.extend(minutes) 
        return windows

    def generate_days_windows(self, start_timestamp, end_timestamp) -> list[str]:

        from datetime import datetime, timedelta
        start = datetime.strptime(start_timestamp, '%Y/%m/%d %H:%M')
        end = datetime.strptime(end_timestamp, '%Y/%m/%d %H:%M')
        if start > end:
            return []
        days = [(start.year,start.month ,d) for d  in range(start.day + (1 if start.hour > 0 or start.minute > 0 else 0), end.day +(1 if end.hour == 23 and end.minute=="59" else 0))]
        if days:
            min_day = min(days)
            max_day = max(days)
            hours =  self.generate_hours_windows(start_timestamp, self.__sub_date_parts(f"{min_day[0]:04d}/{min_day[1]:02d}/{min_day[2]:02d} 23:59", "day",-1))
            hours.extend(self.generate_hours_windows(self.__sub_date_parts(f"{max_day[0]:04d}/{max_day[1]:02d}/{max_day[2]:02d} 00:00", "day",1), end_timestamp))
        else:
            if start.day != end.day:
                end_temp = end.replace(hour=0).replace(minute=0)
                end_temp -= timedelta(hours=1)
                hours  = self.generate_hours_windows(start_timestamp, end_temp.strftime('%Y/%m/%d %H:%M'))
                hours.extend(self.generate_minute_windows(end.replace(hour=0).replace(minute=0).strftime('%Y/%m/%d %H:%M'), end_timestamp))  
            else:
                hours  = self.generate_hours_windows(start_timestamp, end_timestamp)
        windows =[]

        for day in days:
            day_str = f"{config.DISTINCT_USERS_DAY_HYPERLOGLOG_KEY}:{day[0]:04d}{day[1]:02d}{day[2]:02d}"
            windows.append(day_str)
        windows.extend(hours) 
        return windows

    def generate_month_windows(self, start_timestamp, end_timestamp) -> list[str]:

        from datetime import datetime, timedelta
        start = datetime.strptime(start_timestamp, '%Y/%m/%d %H:%M')
        end = datetime.strptime(end_timestamp, '%Y/%m/%d %H:%M')
        if start > end:
            return []

        months = [(start.year,m) for m  in range(start.month + (1 if start.day > 1 or start.hour > 0 or start.minute > 0 else 0), end.month + (1 if end_timestamp.endswith("31 23:59") or end_timestamp.endswith("30 23:59") else 0))]
        if months:
            min_month = min(months)
            max_month = max(months)
            days =  self.generate_days_windows(start_timestamp, self.__sub_date_parts(f"{min_month[0]:04d}/{min_month[1]:02d}/31 23:59", "month",-1))
            days.extend(self.generate_days_windows(self.__sub_date_parts(f"{max_month[0]:04d}/{max_month[1]:02d}/01 00:00", "month",1), end_timestamp))
        else:
            days  = self.generate_days_windows(start_timestamp, end_timestamp)

        windows =[]
        for month in months:
            month_str = f"{config.DISTINCT_USERS_MONTH_HYPERLOGLOG_KEY}:{month[0]:04d}{month[1]:02d}"
            windows.append(month_str)

        windows.extend(days) 
        return windows

    def generate_windows(self, start_timestamp, end_timestamp):
        from datetime import datetime, timedelta
        start = datetime.strptime(start_timestamp, '%Y/%m/%d %H:%M')
        end = datetime.strptime(end_timestamp, '%Y/%m/%d %H:%M')
        if start > end:
            return []
        # Years: full years within the range
        years = [y for y in range(start.year + (1 if start.month > 1 or start.day > 1 or start.hour > 0 or start.minute > 0 else 0), end.year + ( 1 if end_timestamp.endswith("12/31 23:59") else 0 ))]
        if years:
            min_year = min(years)
            max_year = max(years)
            months =  self.generate_month_windows(start_timestamp, self.__sub_date_parts(f"{min_year:04d}/12/31 23:59", "year",-1))
            months.extend(self.generate_month_windows(self.__sub_date_parts(f"{max_year:04d}/01/01 00:00", "year",1), end_timestamp))
        else:
            months  = self.generate_month_windows(start_timestamp, end_timestamp)

        windows = []
        for year in years:
            year_str = f"{config.DISTINCT_USERS_YEAR_HYPERLOGLOG_KEY}:{year}"
            windows.append(year_str)
        windows.extend(months) 
        return windows

    def __sub_date_parts(self, date_str: str, parts: str, num: int) -> str:
        from datetime import datetime, timedelta
        date = datetime.strptime(date_str, '%Y/%m/%d %H:%M')
        if parts == 'minute':  # Subtract minute
            date += timedelta(minutes=num)
        elif parts == 'hour':  # Subtract hour
            date += timedelta(hours=num)
        elif parts == 'day':  # Subtract day
            date += timedelta(days=num)
        elif parts == 'month':  # Subtract month
            if num == -1:
                month = date.month + num if date.month > 1 else 12
                year = date.year if date.month > 1 else date.year - 1
                day = min(date.day, [31,
                    29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                    31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
                date = date.replace(year=year, month=month, day=day)
            elif num  ==1:
                month = date.month + num if date.month < 12 else 1
                year = date.year if date.month < 12 else date.year + 1
                day = min(date.day, [31,
                    29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                    31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
                date = date.replace(year=year, month=month, day=day)
        elif parts == 'year':  # Subtract year
            date  = date.replace(year=date.year + num)
        return date.strftime('%Y/%m/%d %H:%M')


    def search_distinct_users(self, start_ts: str, end_ts: str) -> int:
        search_key = f"distinct_users_range:{start_ts}_{end_ts}"
        if self.search_redis_client.exists(search_key):
            print("Cache hit for search_key :", search_key)
            #return self.search_redis_client.pfcount(search_key)  # warm up

        split_keys = self.generate_windows(start_ts, end_ts)
        print(start_ts, end_ts, "Generated split_keys :", split_keys)       
        for split_key in split_keys:

            if self.search_redis_client.exists(split_key):
                print("Cache hit for split_key:", split_key)
                # continue

            query = {"_id": split_key}
            document = self.collection.find_one(query)
            if document:
                bucket_data = document["data"]["bucket_data"]
                # delete if exists to avoid BUSYKEY error
                self.search_redis_client.delete(split_key)
                # restore the HyperLogLog from dumped data
                self.search_redis_client.restore(split_key, 3600, bucket_data)
            else:
                print("Restored split_key from MongoDB:", split_key)
                if self.ingestion_redis_client.exists(split_key):
                    print(
                        "Falling back to ingestion Redis for split_key:", split_key
                    )
                    bucket_data = self.ingestion_redis_client.dump(split_key)

                    # delete if exists to avoid BUSYKEY error
                    self.search_redis_client.delete(split_key)
                    # restore the HyperLogLog from dumped data
                    self.search_redis_client.restore(split_key, 3600, bucket_data)

        # The first argument must be the destination key. Extend the list with all the source keys found
        command_args = [search_key]
        command_args.extend(split_keys)
        self.search_redis_client.pfmerge(*command_args)
        print(f"Merged split_keys into search_key: {search_key}")
        return self.search_redis_client.pfcount(search_key)  # warm up


if __name__ == "__main__":
    search_service = SearchService()
    start_timestamp = "2025/12/08 19:46"
    end_timestamp = "2025/12/08 20:06"
    distinct_users_count = search_service.search_distinct_users(start_timestamp, end_timestamp)
    print(f"Distinct users count from {start_timestamp} to {end_timestamp}: {distinct_users_count}")