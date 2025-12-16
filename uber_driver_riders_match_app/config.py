# redis pubsub and service discovery configuration
from setup.redis_setup import config as redis_config , constants as redis_constants
REDIS_SERVER = redis_config.configurations[redis_constants.REDIS_SERVER]
REDIS_PORT = redis_config.configurations[redis_constants.REDIS_PORT]

DEFAULT_REDIS_CONFIG = {
    'host': REDIS_SERVER,
    'port': REDIS_PORT,
}

REDIS_CITY_SHARDS = {
    'ashburn': {'host': REDIS_SERVER, 'port': REDIS_PORT, 'weight': 100},
    'seattle': {'host': REDIS_SERVER, 'port': REDIS_PORT, 'weight': 100},
    'newyork': {'host': REDIS_SERVER, 'port': REDIS_PORT, 'weight': 100},
    'losangeles': {'host': REDIS_SERVER, 'port': REDIS_PORT, 'weight': 100},
    'chicago': {'host': REDIS_SERVER, 'port': REDIS_PORT, 'weight': 100}
}


DRIVER_LOCATIONS_KEY = "driver_locations"
RIDE_REQUESTS_KEY = "ride_requests"
PUBSUB_CHANNEL = "ride_offers"
DRIVER_METADATA_KEY = "driver_metadata"

# Service Discovery Configuration
SERVICE_DISCOVERY_HOST = "localhost"
SERVICE_DISCOVERY_PORT = 8500
SERVICE_DISCOVERY_URL = f"http://{SERVICE_DISCOVERY_HOST}:{SERVICE_DISCOVERY_PORT}"

#zookeeper config
SERVICE_REGISTRY_PATH= "uber_ride_match/service_registry"
REDIS_SHARDS_PATH = "uber_ride_match/redis_shards"
HEALTH_CHECK_PATH = "uber_ride_match/health_check"
from setup.zookeeper_setup import config as zk_config , constants as zk_constants
ZOOKEEPER_HOSTS = zk_config.configurations[zk_constants.ZOOKEEPER_CONN]


# location update service configuration
LOCATION_TTL = 30  # seconds
LOCATION_SERVICE_NAME = "location_service"
LOCATION_SERVICE_PORT = 8001

# match service configuration
MATCH_SERVICE_NAME = "match_service"
MATCH_SERVICE_PORT = 8002

# ride estimate service configuration
RIDE_ESTIMATE_SERVICE_NAME = "ride_estimate_service"
RIDE_ESTIMATE_SERVICE_PORT = 8003

# websocket gateway service configuration
WEBSOCKET_GATEWAY_SERVICE_NAME = "websocket_gateway_service"
WEBSOCKET_GATEWAY_SERVICE_PORT = 8004

from setup.mysql_setup import config as mysql_config, constants as mysql_constants
# Database Configuration
DB_CONFIG = {
    'host': mysql_config.configurations[mysql_constants.HOST],
    'user': mysql_config.configurations[mysql_constants.USER],
    'password': mysql_config.configurations[mysql_constants.PASSWORD],
    'port': mysql_config.configurations[mysql_constants.PORT],
    'database': 'ride_service_db'
}