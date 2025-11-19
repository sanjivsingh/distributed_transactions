from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
import pymongo
import json
from commons import logger
import pandas as pd
import signal
import threading
import time
import importlib

log = logger.setup_logger(__name__)
import config

# MongoDB connection
from setup.mongodb_setup import config as mongo_config, constants as mongo_constants

mongo_client = pymongo.MongoClient(
    mongo_config.configurations[mongo_constants.SERVER],
    mongo_config.configurations[mongo_constants.PORT],
)
mongo_db = mongo_client[config.mongo_instagram_db]
login_events_collection = mongo_db[config.mongo_login_events_collection]

from setup.kafka_setup import config as kafka_config, constants as kafka_constants

class LoginEventWorker:
    def __init__(self):
        self.consumer_conf = {
            "bootstrap.servers": kafka_config.configurations[
                kafka_constants.KAFKA_BROKER
            ],
            "group.id": config.login_event_group,
            "auto.offset.reset": "earliest",  # Start from the earliest if no committed offset is found
            "enable.auto.commit": False,  # Disable auto-commit for manual control
        }
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe([config.stream_event_files_topic])
        self.running = True
        self.config_last_modified = self.get_config_mtime()

        # Start config refresh thread
        self.config_refresh_thread = threading.Thread(target=self.refresh_config_periodically, daemon=True)
        self.config_refresh_thread.start()

    def get_config_mtime(self):
        """Get last modified time of config module."""
        import os
        config_path = config.__file__
        return os.path.getmtime(config_path)

    def refresh_config(self):
        """Reload config module if changed."""
        current_mtime = self.get_config_mtime()
        if current_mtime > self.config_last_modified:
            log.info("Config file changed, reloading...")
            importlib.reload(config)
            self.config_last_modified = current_mtime
            # Reinitialize consumer with new config if needed
            self.consumer_conf["group.id"] = config.login_event_group
            # Note: Consumer might need restart for some configs; here we just update group.id
            log.info("Config reloaded successfully.")

    def refresh_config_periodically(self):
        """Check for config changes every 60 seconds."""
        while self.running:
            self.refresh_config()
            time.sleep(60)

    def run(self):
        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event - not an error
                    log.warning( f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                # TODO : add logic for retry and moving to dead letter queue after certain retries

                login_event_data = json.loads(msg.value().decode("utf-8"))
                file_id = login_event_data["file_id"]
                creation_time = login_event_data["creation_time"]
                cloud_file_path = login_event_data["cloud_file_path"]
                log.info( f"Processing file_id: {file_id}, creation_time: {creation_time}, cloud_file_path: {cloud_file_path}")

                event_df = pd.read_csv(cloud_file_path)
                event_df['event_type'] = event_df['Event'].str.split(' ').str[1]
                event_type_counts = event_df['event_type'].value_counts().to_dict()
                for event_type, event_count in event_type_counts.items():
                    login_events_collection.update_one(
                        {"_id": event_type},
                        {"$inc": {"event_count": event_count}},
                        upsert=True,
                    )
                    log.info(f"Updated event_type: {event_type} with event count increment: {event_count}")

                #  Commit offset after successful processing
                self.consumer.commit(msg)

            except Exception as e:
                # retry or push it to dead letter queue
                log.error(f"Error processing message : {msg}", e)

    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()
        if mongo_client:
            mongo_client.close()

# Signal handler for graceful shutdown
def signal_handler(signum, frame):
    log.info("Received signal, shutting down...")
    worker.stop()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Start worker
if __name__ == "__main__":
    worker = LoginEventWorker()
    try:
        worker.run()
    except Exception as e:
        log.error("Worker error", e)
    finally:
        worker.stop()
