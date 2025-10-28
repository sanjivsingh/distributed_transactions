- Install

brew install kafka 

-- start server:

/usr/local/opt/kafka/bin/kafka-server-start /usr/local/etc/kafka/server.properties


 .venv/bin/python -m kafka_setup.conversion_producer

 .venv/bin/python -m kafka_setup.conversion_consumer

 .venv/bin/python -m kafka_setup.setup


# list consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# check status of consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my_consumer_group