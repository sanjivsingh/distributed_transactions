from confluent_kafka import Producer
import os
from commons.uniqueid import SnowflakeIDGenerator
import json
import datetime
import sys
from commons import logger
log = logger.setup_logger(__name__)
import config
class MoveEventToCloudWorker:

    def __init__(self, worker_id = 1):
        # Snowflake ID Generator
        self.id_generator = SnowflakeIDGenerator(worker_id)

        # Kafka producer
        from setup.kafka_setup import config as kafka_config, constants as kafka_constants
        producer_conf = {
            "bootstrap.servers": kafka_config.configurations[kafka_constants.KAFKA_BROKER]
        }
        self.producer = Producer(producer_conf)

        # directories
        self.local_dir = os.path.join(os.getcwd(), config.local_directory)
        # TODO : simulate cloud storage directory, replace with S3 or GCS bucket
        self.cloud_storage_dir = os.path.join(os.getcwd(), config.cloud_storage_directory)
        if not os.path.exists(self.cloud_storage_dir):
            os.makedirs(self.cloud_storage_dir)

    ...
    def start(self):
        while True:
            local_files = os.listdir(self.local_dir)
            if local_files:
                list.sort(local_files)
                # skipping last file as it may be still being written
                for local_file in local_files[0:len(local_files)-1]:
                    local_file_path = os.path.join(self.local_dir, local_file)
                    cloud_file_path = os.path.join(self.cloud_storage_dir, local_file)

                    # TODO : replace with S3 or GCS upload code and after successful upload delete local file
                    os.rename(local_file_path, cloud_file_path)
                    log.info(f"Moved file from {local_file_path} to cloud storage at {cloud_file_path}")

                    # Send file info to Kafka
                    message = {"file_id": self.id_generator.generate_id(), "creation_time": datetime.datetime.now().isoformat(), "cloud_file_path": cloud_file_path}
                    self.producer.produce(config.stream_event_files_topic, json.dumps(message).encode("utf-8"))
                    self.producer.flush()
                    log.info(f"Produced message to Kafka: {message}")

            log.info("No local files to process. Sleeping for 10 seconds.")
            import time
            time.sleep(10)
    def __del__(self):
        self.close()

    def close(self):
        if self.producer:
            self.producer.flush()
            log.info("Producer flushed and closed.")

if __name__ == "__main__":


    worker_id = sys.argv[1] if sys.argv and len(sys.argv) > 1 else 1
    worker = MoveEventToCloudWorker()
    try:
        worker.start()
    except KeyboardInterrupt as e:
        log.error("Shutting down worker...", e)
    finally:
        worker.close()