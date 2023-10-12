from datetime import timedelta
from functools import cached_property
from typing import Any, Sequence

from airflow.configuration import conf

from airflow.exceptions import AirflowException

from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor
from ..hooks.rabbitmq import RabbitMQHookAsync
from ..triggers.rabbitmq import RabbitMQTrigger


class RabbitMQSensorAsync(RabbitMQSensor):
    """
    Waits for a message to appear on a RabbitMQ channel.

    :param channel: Channel of the RabbitMQ.
    :param rabbitmq_conn_id: Airflow conn ID for RabbitMQ
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = ("queue_name",)

    def __init__(
        self,
        *,
        queue_name: str,
        rabbitmq_conn_id: str = "rabbitmq_default",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(queue_name=queue_name, rabbitmq_conn_id=rabbitmq_conn_id, **kwargs)
        self.deferrable = deferrable

    def execute(self, context) -> str | None:
        """
        Airflow runs this method on the worker and defers using the trigger.

        Uses base class' blocking implementation of self.poke before trying
        to defer to return faster when there's already a message waiting.
        """
        if not self.deferrable:
            # invokes self.poke
            super().execute(context)
        else:
            if not self.poke(context=context):
                # only defer if there wasn't something on the queue waiting
                self._defer()

        # Overriden to return the message taken off the queue
        return self._return_value

    def _defer(self) -> None:
        """
        Check for a message on a RabbitMQ channel and defers using the triggerer.
        """
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=RabbitMQTrigger(
                queue_name=self.queue_name,
                rabbitmq_conn_id=self.rabbitmq_conn_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event: dict[str, Any]) -> str | None:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "success":
            self._return_value = event["message"]

        if event["status"] == "error":
            raise AirflowException(event["trace"])

        return self._return_value

    @cached_property
    def hook(self) -> RabbitMQHookAsync:
        return RabbitMQHookAsync(rabbitmq_conn_id=self.rabbitmq_conn_id)
