SERVER_URL = "http://localhost:8000"
CHUNK_SIZE = 1024 * 5  # 5KB

# Configurations
from setup.mongodb_setup import config as mongodb_config, constants as mongodb_constants
MONGO_SERVER =     mongodb_config.configurations[mongodb_constants.SERVER]
MONGO_PORT=    mongodb_config.configurations[mongodb_constants.PORT]

DB_NAME = "dropbox_sync"
SERVER_CHUNK_PREFIX = "dropbox_filesync_app/data/server"
S3_BUCKET = "your-s3-bucket"
is_local = True