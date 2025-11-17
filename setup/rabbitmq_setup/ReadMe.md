# Install RabbitMQ

brew install rabbitmq


# Start the RabbitMQ server
Use the following command to start the server in the background as a service:

   brew services start rabbitmq

# Enable the management console (optional)
Run this command to enable the web-based management UI:

   rabbitmq-plugins enable rabbitmq_management

You can then access the console in your browser at http://localhost:15672. 
Managing RabbitMQ


To start the server: 

   brew services start rabbitmq

To stop the server: 

   brew services stop rabbitmq

To check the server status: 

   rabbitmqctl status

To view server logs: 

   brew services list

To access the management UI: Open http://localhost:15672 in your web browser and log in with the default credentials guest/guest. 