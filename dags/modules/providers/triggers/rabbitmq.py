import traceback
from functools import cached_property
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent
from ..hooks.rabbitmq import RabbitMQHookAsync


class RabbitMQTrigger(BaseTrigger):
    """
    RabbitMQTrigger is fired as deferred class with params to run the task in trigger worker.

    :param queue_name: Queue of the RabbitMQ service.
    :param rabbitmq_conn_id: Airflow conn ID for RabbitMQ
    """

    def __init__(
        self,
        queue_name: str,
        rabbitmq_conn_id: str = "rabbitmq_default",
    ):
        super().__init__()
        self.queue_name = queue_name
        self.rabbitmq_conn_id = rabbitmq_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize RabbitMQTrigger arguments and classpath.
        """
        return (
            "modules.providers.triggers.rabbitmq.RabbitMQTrigger",
            {
                "queue_name": self.queue_name,
                "rabbitmq_conn_id": self.rabbitmq_conn_id,
            },
        )

    @cached_property
    def hook(self) -> RabbitMQHookAsync:
        return RabbitMQHookAsync(rabbitmq_conn_id=self.rabbitmq_conn_id)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Make an asynchronous connection and message pull using RabbitMQHookAsync.
        """
        try:
            message: str = await self.hook.pull_async(self.queue_name)
            yield TriggerEvent({"status": "success", "message": message})

        except Exception as ex:
            trace = traceback.format_exc()
            yield TriggerEvent({"status": "error", "message": str(ex), "trace": trace})
