from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
import json
from abc import ABC
import time

from kafka_setup import  constants, config
from commons import logger
from file_convertor import driver
from redis_setup import redis_duplicate
# setup database
from file_convertor_webapp import mysql_database

# redis configuration
from redis_setup import constants as redis_constants
from redis_setup import config as redis_config
from file_convertor_webapp.models import ConversionRequest

class ConversionConsumer(ABC):

    def __init__(self, configurations) -> None:
        super().__init__()

        # Kafka broker configuration
        self.conf = {
            'bootstrap.servers': configurations[constants.KAFKA_BROKER], # Replace with your Kafka broker address
            'group.id': configurations[constants.CONVERSION_CONSUMER_GROUP] ,       # Unique consumer group ID
            'auto.offset.reset': 'latest',       # Start from the beginning if no committed offset is found
            'enable.auto.commit': False            # Disable auto-commit for manual control
        }
        # topic to consumer msg
        self.topic = configurations[constants.CONVERSION_TOPIC]
        self.check_duplicate = configurations[constants.DUPLICATE_CHECK]

        # Setup logger
        self.log = logger.setup_logger(__name__)
        self.dbConn =  mysql_database.MysqlDatabaseConnection(reset=False)

    def close(self):
        pass

    def start(self):

        # Create Consumer instance
        consumer = Consumer(self.conf)
        duplicate_store = redis_duplicate.RedisDuplicate(host=redis_config.configurations[redis_constants.REDIS_SERVER],port=redis_config.configurations[redis_constants.REDIS_PORT], set_name = redis_config.configurations[redis_constants.SET_NAME])
        try:
            consumer.subscribe([self.topic])

            while True:
                self.log.info("waiting 10 seconds...")
                #time.sleep(10) # Pause for 60 seconds
                msg = consumer.poll(timeout=1.0) # Poll for messages with a timeout

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event - not an error
                        self.log.warning(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    # 3. Process the message
                    unique_id = f"{msg.topic()}-{msg.partition()}-{msg.offset()}"

                    if self.check_duplicate and duplicate_store.is_present(unique_id):
                        # This is a duplicate (it was processed, but the offset wasn't committed)
                        self.log.info(f"Duplicate message detected: ID={unique_id}")
                        # Still commit the offset to move past it safely.
                        consumer.commit(message=msg, asynchronous=False) 
                        continue 

                    key = msg.key().decode('utf-8')
                    value = msg.value().decode('utf-8')
                    # Process message
                    self.log.info("----------------------------------------------")
                    self.log.info(f"Received message: Key={key}, Value={value}, "
                        f"Topic={msg.topic()}, Partition={msg.partition()}, Offset={msg.offset()}")

                    req_dict = self.dbConn.get_record(key)
                    if req_dict is None:
                        self.log.warning(f"conversion id: {key} doesn't exist")
                        consumer.commit(message=msg, asynchronous=False)
                        continue
                    
                    try :

                        req = ConversionRequest()
                        req.id = req_dict['id']
                        req.status = req_dict['status']
                        req.input_format = req_dict['input_format']
                        req.output_format = req_dict['output_format']
                        req.upload_path = req_dict['upload_path']
                        req.converted_path = req_dict['converted_path']
                        req.details = req_dict['details']

                        req.status = "running"
                        self.dbConn.update_status(request_id = req.id, status = req.status, details = req.details)
                        #time.sleep(10 + random.randint(0,10))

                        req_dict = json.loads(value)
                        conversion_response = None
                        try:   
                            self.log.info(f"conversion started: {req.id}")
                            conversion_response = driver.Driver.convert(input_type=req.input_format , input_path = req.upload_path ,output_type = req.output_format , output_path=req.converted_path)
                            self.log.info(f"conversion response: {conversion_response}" )
                            req.status = "completed"
                            req.details = conversion_response.message
                        except Exception as e:
                            self.log.exception(f"conversion response: {e}")
                            req.status = "failed"
                            req.details = f"Error in conversion : {e}"

                        self.dbConn.update_status(request_id = req.id, status = req.status, details = req.details)
                        # Artificial time
                        #time.sleep(10 + random.randint(0,10))
                        # Manually commit the offset after successful processing

                        if self.check_duplicate:
                            # 3. Atomically Record ID and Commit Offset
                            # This is the tricky part. For true Exactly-Once, the message processing 
                            # and the offset commit must happen transactionally, usually by saving 
                            # the offset in the same database transaction as the business logic.
                            # For simplicity with the confluent-kafka client, we record the ID first:
                            duplicate_store.set_key(unique_id) 

                        consumer.commit(message=msg, asynchronous=False)
                    except Exception as e:
                        self.log.info(f" error in processing request : {req_dict} : {e}")

        except KeyboardInterrupt:
            pass
        finally:
            # Close down consumer to commit final offsets.
            if consumer:
                consumer.close()
            self.log.info("Kafka Consumer closed.")
            if duplicate_store:
                duplicate_store.__del__()
                self.log.info("duplicate_store connection closed.")


if __name__ == "__main__":

    consumer  = ConversionConsumer(config.configurations)
    consumer.start()