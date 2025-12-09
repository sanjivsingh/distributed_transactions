from setup.kafka_setup import config as kafka_config, constants as kafka_constants
from setup.redis_setup import config as redis_config, constants as redis_constants

# ---------- Config ----------
KAFKA_BROKER = kafka_config.configurations[kafka_constants.KAFKA_BROKER]
USER_ACTIVITY_TOPIC = "user_activity"

INGESTION_REDIS_SERVER = redis_config.configurations[redis_constants.REDIS_SERVER]
BATCH_REDIS_SERVER = redis_config.configurations[redis_constants.REDIS_SERVER]
SEARCH_REDIS_SERVER = redis_config.configurations[redis_constants.REDIS_SERVER]
REDIS_STREAM_NAME=  "user_activity_stream"
REDIS_CONSUMER_GROUP=   "user_activity_group"


DISTINCT_USERS_MINUTE_HYPERLOGLOG_KEY = "hyperloglog:distinct_users_minute"
DISTINCT_USERS_HOUR_HYPERLOGLOG_KEY = "hyperloglog:distinct_users_hour"
DISTINCT_USERS_DAY_HYPERLOGLOG_KEY = "hyperloglog:distinct_users_day"
DISTINCT_USERS_MONTH_HYPERLOGLOG_KEY = "hyperloglog:distinct_users_month"
DISTINCT_USERS_YEAR_HYPERLOGLOG_KEY = "hyperloglog:distinct_users_year"
DISTINCT_USERS_BUCKET_SECONDS = 60

MONGO_DATABASE= "distinct_users_db"
MONGO_COLLECTION= "distinct_users_collection"