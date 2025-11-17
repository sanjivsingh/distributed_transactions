from confluent_kafka import Producer
import json
import time
from abc import ABC

from . import constants
from commons import logger
from file_convertor_webapp import  models

class ConversionProducer(ABC):

    def __init__(self, configurations) -> None:

        super().__init__()
        # Setup logger
        self.logger = logger.setup_logger(__name__)

        # Kafka broker configuration
        self.producer_config = {
            'bootstrap.servers':  configurations[constants.KAFKA_BROKER],
            'enable.idempotence': True # Guarantees exactly-once me
        }

        # Topic to produce messages to
        self.topic  = configurations[constants.CONVERSION_TOPIC]

        # Create Producer instance
        self.producer = Producer(self.producer_config)

    def __del__(self):
        if self.producer:
            self.producer.flush()

    def push(self,  req: dict[str,str]) -> bool:

        # Produce message
        message_key = str(req["id"]).encode('utf-8')
        message_value = json.dumps(req).encode('utf-8')
        try:
            self.producer.produce(self.topic, key=message_key, value=message_value, callback=self.delivery_report)
        except BufferError:
            self.logger.warning("Local producer queue full, flushing...")
            self.producer.poll(0) # Serve delivery reports
            self.producer.flush() # Wait for outstanding messages
            self.producer.produce(self.topic, key=message_key, value=message_value, callback=self.delivery_report)

        # Wait for any outstanding messages to be delivered
        self.producer.flush()
        return True

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to topic {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

