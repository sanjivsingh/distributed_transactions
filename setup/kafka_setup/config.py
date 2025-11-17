from setup.kafka_setup import constants

configurations = {
    constants.CONVERSION_TOPIC: "conversion_requests",
    constants.KAFKA_BROKER: "localhost:9092",
    constants.CONVERSION_CONSUMER_GROUP: "conversion_consumer_group",
    constants.NUMBER_OF_PARTITIONS: 3,
    constants.NUMBER_OF_REPLICAS: 3,
    constants.DUPLICATE_CHECK: True,
    constants.DB_NULL_LIMIT: 10,
}
