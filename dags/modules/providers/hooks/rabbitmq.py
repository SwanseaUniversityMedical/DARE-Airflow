from functools import cached_property

import aio_pika
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook


class RabbitMQHookAsync(RabbitMQHook):
    """
    RabbitMQ async interaction hook.

    :param rabbitmq_conn_id: Airflow conn ID to be used to
        configure this hook, defaults to rabbitmq_default
    """

    @cached_property
    async def async_conn(self) -> aio_pika.Connection:
        """
        Returns an async connection to RabbitMQ using aio_pika.

        :return: Connection to RabbitMQ server
        """

        conn = self.get_connection(self.rabbitmq_conn_id)

        if not conn.schema:
            conn.schema = "/"

        connection = await aio_pika.connect_robust(
            host=conn.host,
            port=conn.port,
            login=conn.login,
            password=conn.password,
            virtualhost=conn.schema,

        )
        return connection

    async def pull_async(self, queue_name: str) -> str:
        """
        Pull and acknowledge a message from the queue asynchronously.

        :param queue_name: The queue to pull messages from
        :return: The message
        """
        connection: aio_pika.Connection = await self.async_conn

        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    return message.body.decode()
