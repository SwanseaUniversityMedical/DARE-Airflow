import datetime
import logging
import time
from functools import cached_property
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils import timezone

from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook
from ..triggers.rabbitmq import RabbitMQTrigger


class RabbitMQPythonOperator(BaseOperator):

    def __init__(self,
                 func,
                 queue_name: str,
                 rabbitmq_conn_id: str = "rabbitmq_default",
                 deferrable: datetime.timedelta | None | bool = True,
                 timeout: datetime.timedelta | None = None,
                 poke_interval: datetime.timedelta | None = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.func = func
        self.timeout = timeout
        self.poke_interval = poke_interval

        self.deferrable = deferrable
        if isinstance(self.deferrable, bool):
            self.deferrable = datetime.timedelta(seconds=60) if self.deferrable else None

    def _defer(self) -> None:
        """
        Check for a message on a RabbitMQ channel and defers using the triggerer.
        """
        self.defer(
            timeout=self.timeout,
            trigger=RabbitMQTrigger(
                queue_name=self.queue_name,
                rabbitmq_conn_id=self.rabbitmq_conn_id,
            ),
            method_name="execute",
        )

    def execute(self, context, event: dict[str, Any] | None = None):
        logger = logging.getLogger(__name__)
        last_message_utc = timezone.utcnow()

        # Consume an event if we are resuming from a deferred state and process it with the functor
        if event is not None:
            logger.info("Consuming deferred event...")

            if event["status"] == "success":
                logger.info("Consumed message!")
                logger.info(f"message={event['message']}")

                # Process the deferred message!
                last_message_utc = timezone.utcnow()
                self.func(event["message"])

                # Block for the timeout period before polling the queue again
                if self.poke_interval is not None:
                    time.sleep(max(0., self.poke_interval.total_seconds()))

            elif event["status"] == "error":
                raise AirflowException(event["trace"])
            else:
                raise AirflowException("Unknown deferred event type!", str(event))

        while True:
            # Consume messages directly off the queue in a blocking loop and process them with the functor
            message = self.hook.pull(self.queue_name)
            if message is not None:
                logger.info("Consumed message!")
                logger.info(f"message={message}")

                # Process the message!
                last_message_utc = timezone.utcnow()
                self.func(message)

            else:
                # If the queue is empty, and we've been waiting then defer
                if (self.deferrable is not None) and \
                   ((timezone.utcnow() - last_message_utc) > self.deferrable):

                    logger.info("Deferring!")
                    self._defer()

            # Block for the timeout period before polling the queue again
            # If deferred above this will never be reached
            if self.poke_interval is not None:
                time.sleep(max(0., self.poke_interval.total_seconds()))

    @cached_property
    def hook(self) -> RabbitMQHook:
        return RabbitMQHook(rabbitmq_conn_id=self.rabbitmq_conn_id)
