import pika
import json
from airflow.hooks.base_hook import BaseHook


def send_message_to_rabbitmq(rabbit_connection, exchange, routing_key, message):
    # Get RabbitMQ connection details from Airflow connection
    connection = BaseHook.get_connection(rabbit_connection)

    credentials = pika.PlainCredentials(connection.login, connection.password)
    parameters = pika.ConnectionParameters(
        host=connection.host,
        port=connection.port,
        virtual_host=connection.schema,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(message)
    )

    connection.close()

# Example usage:
# send_message_to_rabbitmq('my_exchange', 'my_routing_key', {'key': 'value'})
