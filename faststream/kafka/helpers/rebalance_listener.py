import asyncio
import logging
from typing import TYPE_CHECKING, Any, Optional

from aiokafka import ConsumerRebalanceListener

from faststream._internal.utils.functions import call_or_await

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, TopicPartition

    from faststream._internal.basic_types import LoggerProto


def make_logging_listener(
    *,
    consumer: "AIOKafkaConsumer",
    logger: Optional["LoggerProto"],
    log_extra: dict[str, Any],
    listener: Optional["ConsumerRebalanceListener"],
) -> Optional["ConsumerRebalanceListener"]:
    if logger is None:
        return listener

    logging_listener = _LoggingListener(
        consumer=consumer,
        logger=logger,
        log_extra=log_extra,
    )
    if listener is None:
        return logging_listener

    return _LoggingListenerFacade(
        logging_listener=logging_listener,
        listener=listener,
    )


class _LoggingListener(ConsumerRebalanceListener):  # type: ignore[misc]
    _log_unassigned_consumer_delay_seconds = 60 * 2
    _log_unassigned_consumer_task: asyncio.Task[None] | None

    def __init__(
        self,
        *,
        consumer: "AIOKafkaConsumer",
        logger: "LoggerProto",
        log_extra: dict[str, Any],
    ) -> None:
        self.consumer = consumer
        self.logger = logger
        self.log_extra = log_extra
        self._log_unassigned_consumer_task = None

    async def on_partitions_revoked(self, revoked: set["TopicPartition"]) -> None:
        pass

    async def log_unassigned_consumer(self) -> None:
        await asyncio.sleep(self._log_unassigned_consumer_delay_seconds)
        self.logger.log(
            logging.WARNING,
            f"Consumer in group {self.consumer._group_id} has had no partition "
            f"assignments for {self._log_unassigned_consumer_delay_seconds} seconds: "
            f"topics {self.consumer._subscription.topics} may have fewer partitions "
            f"than consumers.",
            extra=self.log_extra,
        )

    async def on_partitions_assigned(self, assigned: set["TopicPartition"]) -> None:
        self.logger.log(
            logging.INFO,
            f"Consumer {self.consumer._coordinator.member_id} assigned to partitions: "
            f"({', '.join(str(tp) for tp in assigned)})",
            extra=self.log_extra,
        )

        if not assigned:
            self.logger.log(
                logging.WARNING,
                f"Consumer in group {self.consumer._group_id} has no partition assignments - this "
                f"could be temporary, e.g. during a rolling update. A separate warning will be logged if "
                f"this condition persists for {self._log_unassigned_consumer_delay_seconds} seconds.",
                extra=self.log_extra,
            )

            self._log_unassigned_consumer_task: asyncio.Task[None] | None = (
                asyncio.create_task(self.log_unassigned_consumer())
            )

        elif self._log_unassigned_consumer_task:
            self._log_unassigned_consumer_task.cancel()
            self._log_unassigned_consumer_task = None


class _LoggingListenerFacade(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(
        self,
        *,
        logging_listener: _LoggingListener,
        listener: ConsumerRebalanceListener,
    ) -> None:
        self.logging_listener = logging_listener
        self.listener = listener

    async def on_partitions_revoked(self, revoked: set["TopicPartition"]) -> None:
        await call_or_await(self.listener.on_partitions_revoked, revoked)

    async def on_partitions_assigned(self, assigned: set["TopicPartition"]) -> None:
        await self.logging_listener.on_partitions_revoked(assigned)
        await call_or_await(self.listener.on_partitions_assigned, assigned)
