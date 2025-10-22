import asyncio
import logging
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from time import time
from typing import TYPE_CHECKING, Any

import anyio
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer

from faststream._internal.utils.functions import call_or_await, run_in_executor
from faststream.confluent.schemas import TopicPartition
from faststream.exceptions import SetupError

from . import config as config_module

if TYPE_CHECKING:
    from typing_extensions import NotRequired, TypedDict

    from faststream._internal.logger import LoggerState

    from .admin import AdminService

    class _SendKwargs(TypedDict):
        value: bytes | str | None
        key: bytes | str | None
        headers: list[tuple[str, str | bytes]] | None
        partition: NotRequired[int]
        timestamp: NotRequired[int]
        on_delivery: NotRequired[Callable[..., None]]


class AsyncConfluentProducer:
    """An asynchronous Python Kafka client using the "confluent-kafka" package."""

    def __init__(
        self,
        *,
        logger: "LoggerState",
        config: config_module.ConfluentFastConfig,
    ) -> None:
        self.logger_state = logger

        self.config = config.producer_config
        self.producer = Producer(
            self.config,
            logger=self.logger_state.logger.logger,
        )

        self.__running = True
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def _poll_loop(self) -> None:
        while self.__running:
            with suppress(Exception):
                await call_or_await(self.producer.poll, 0.1)

    async def stop(self) -> None:
        """Stop the Kafka producer and flush remaining messages."""
        if self.__running:
            self.__running = False
            if not self._poll_task.done():
                self._poll_task.cancel()
            await call_or_await(self.producer.flush)

    async def flush(self) -> None:
        await call_or_await(self.producer.flush)

    async def send(
        self,
        topic: str,
        value: bytes | str | None = None,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: list[tuple[str, str | bytes]] | None = None,
        no_confirm: bool = False,
    ) -> "asyncio.Future[Message | None] | Message | None":
        """Sends a single message to a Kafka topic."""
        kwargs: _SendKwargs = {
            "value": value,
            "key": key,
            "headers": headers,
        }

        if partition is not None:
            kwargs["partition"] = partition

        if timestamp_ms is not None:
            kwargs["timestamp"] = timestamp_ms

        loop = asyncio.get_running_loop()
        result_future: asyncio.Future[Message | None] = loop.create_future()

        def ack_callback(err: Any, msg: Message | None) -> None:
            if err or (msg is not None and (err := msg.error())):
                loop.call_soon_threadsafe(
                    result_future.set_exception,
                    KafkaException(err),
                )
            else:
                loop.call_soon_threadsafe(result_future.set_result, msg)

        kwargs["on_delivery"] = ack_callback

        # should be sync to prevent segfault
        self.producer.produce(topic, **kwargs)

        if no_confirm:
            return result_future
        return await result_future

    def create_batch(self) -> "BatchBuilder":
        """Creates a batch for sending multiple messages."""
        return BatchBuilder()

    async def send_batch(
        self,
        batch: "BatchBuilder",
        topic: str,
        *,
        partition: int | None,
        no_confirm: bool = False,
    ) -> None:
        """Sends a batch of messages to a Kafka topic."""
        async with anyio.create_task_group() as tg:
            for msg in batch._builder:
                tg.start_soon(
                    self.send,
                    topic,
                    msg["value"],
                    msg["key"],
                    partition,
                    msg["timestamp_ms"],
                    msg["headers"],
                    no_confirm,
                )

    async def ping(
        self,
        timeout: float | None = 5.0,
    ) -> bool:
        """Implement ping using `list_topics` information request."""
        if timeout is None:
            timeout = -1

        try:
            cluster_metadata = await call_or_await(
                self.producer.list_topics,
                timeout=timeout,
            )

            return bool(cluster_metadata)

        except Exception:
            return False


class AsyncConfluentConsumer:
    """An asynchronous Python Kafka client for consuming messages using the "confluent-kafka" package."""

    def __init__(
        self,
        *topics: str,
        config: config_module.ConfluentFastConfig,
        logger: "LoggerState",
        admin_service: "AdminService",
        # kwargs options
        partitions: Sequence["TopicPartition"],
        bootstrap_servers: str | list[str] = "localhost",
        # consumer options
        client_id: str | None = "confluent-kafka-consumer",
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 52428800,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        retry_backoff_ms: int = 100,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        check_crcs: bool = True,
        metadata_max_age_ms: int = 5 * 60 * 1000,
        partition_assignment_strategy: str | list[Any] = "roundrobin",
        max_poll_interval_ms: int = 300000,
        session_timeout_ms: int = 10000,
        heartbeat_interval_ms: int = 3000,
        security_protocol: str = "PLAINTEXT",
        connections_max_idle_ms: int = 540000,
        isolation_level: str = "read_uncommitted",
        allow_auto_create_topics: bool = True,
    ) -> None:
        self.admin_client = admin_service
        self.logger_state = logger

        self.topics = list(topics)
        self.partitions = partitions

        if not isinstance(partition_assignment_strategy, str):
            partition_assignment_strategy = ",".join(
                [
                    x if isinstance(x, str) else x().name
                    for x in partition_assignment_strategy
                ],
            )

        config_from_params = {
            "allow.auto.create.topics": allow_auto_create_topics,
            "topic.metadata.refresh.interval.ms": 1000,
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "group.id": group_id or "faststream-consumer-group",
            "group.instance.id": group_instance_id,
            "fetch.wait.max.ms": fetch_max_wait_ms,
            "fetch.max.bytes": fetch_max_bytes,
            "fetch.min.bytes": fetch_min_bytes,
            "max.partition.fetch.bytes": max_partition_fetch_bytes,
            "fetch.error.backoff.ms": retry_backoff_ms,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "auto.commit.interval.ms": auto_commit_interval_ms,
            "check.crcs": check_crcs,
            "metadata.max.age.ms": metadata_max_age_ms,
            "partition.assignment.strategy": partition_assignment_strategy,
            "max.poll.interval.ms": max_poll_interval_ms,
            "session.timeout.ms": session_timeout_ms,
            "heartbeat.interval.ms": heartbeat_interval_ms,
            "security.protocol": security_protocol.lower(),
            "connections.max.idle.ms": connections_max_idle_ms,
            "isolation.level": isolation_level,
        } | config.consumer_config

        self.config = config_from_params
        self.consumer = Consumer(self.config, logger=self.logger_state.logger.logger)

        # A pool with single thread is used in order to execute the commands of the consumer sequentially:
        # https://github.com/ag2ai/faststream/issues/1904#issuecomment-2506990895
        self._thread_pool = ThreadPoolExecutor(max_workers=1)

    @property
    def topics_to_create(self) -> list[str]:
        return list({*self.topics, *(p.topic for p in self.partitions)})

    async def start(self) -> None:
        """Starts the Kafka consumer and subscribes to the specified topics."""
        if self.config.get("allow.auto.create.topics", True):
            topics_creation_result = await run_in_executor(
                self._thread_pool,
                self.admin_client.create_topics,
                self.topics_to_create,
            )

            for create_result in topics_creation_result:
                if create_result.error:
                    self.logger_state.log(
                        log_level=logging.WARNING,
                        message=f"Failed to create topic {create_result.topic}: {create_result.error}",
                    )

        else:
            self.logger_state.log(
                log_level=logging.WARNING,
                message="Auto create topics is disabled. Make sure the topics exist.",
            )

        if self.topics:
            await run_in_executor(
                self._thread_pool,
                self.consumer.subscribe,
                topics=self.topics,
            )

        elif self.partitions:
            await run_in_executor(
                self._thread_pool,
                self.consumer.assign,
                [p.to_confluent() for p in self.partitions],
            )

        else:
            msg = "You must provide either `topics` or `partitions` option."
            raise SetupError(msg)

    async def commit(self, asynchronous: bool = True) -> None:
        """Commits the offsets of all messages returned by the last poll operation."""
        await run_in_executor(
            self._thread_pool,
            self.consumer.commit,
            asynchronous=asynchronous,
        )

    async def stop(self) -> None:
        """Stops the Kafka consumer and releases all resources."""
        # NOTE: If we don't explicitly call commit and then close the consumer, the confluent consumer gets stuck.
        # We are doing this to avoid the issue.
        enable_auto_commit = self.config.get("enable.auto.commit", True)

        try:
            if enable_auto_commit:
                await self.commit(asynchronous=False)

        except Exception as e:
            # No offset stored issue is not a problem - https://github.com/confluentinc/confluent-kafka-python/issues/295#issuecomment-355907183
            if "No offset stored" in str(e):
                pass
            else:
                self.logger_state.log(
                    log_level=logging.ERROR,
                    message="Consumer closing error occurred.",
                    exc_info=e,
                )

        # Wrap calls to async to make method cancelable by timeout
        # We shouldn't read messages and close consumer concurrently
        # https://github.com/ag2ai/faststream/issues/1904#issuecomment-2506990895
        # Now it works without lock due `ThreadPoolExecutor(max_workers=1)`
        # that makes all calls to consumer sequential
        await run_in_executor(self._thread_pool, self.consumer.close)

        self._thread_pool.shutdown(wait=False)

    async def getone(self, timeout: float = 0.1) -> Message | None:
        """Consumes a single message from Kafka."""
        msg = await run_in_executor(self._thread_pool, self.consumer.poll, timeout)
        return check_msg_error(msg)

    async def getmany(
        self,
        timeout: float = 0.1,
        max_records: int | None = 10,
    ) -> tuple[Message, ...]:
        """Consumes a batch of messages from Kafka and groups them by topic and partition."""
        raw_messages: list[Message | None] = await run_in_executor(
            self._thread_pool,
            self.consumer.consume,
            num_messages=max_records or 10,
            timeout=timeout,
        )
        return tuple(x for x in map(check_msg_error, raw_messages) if x is not None)

    async def seek(self, topic: str, partition: int, offset: int) -> None:
        """Seeks to the specified offset in the specified topic and partition."""
        topic_partition = TopicPartition(
            topic=topic,
            partition=partition,
            offset=offset,
        )
        await run_in_executor(
            self._thread_pool,
            self.consumer.seek,
            topic_partition.to_confluent(),
        )


def check_msg_error(msg: Message | None) -> Message | None:
    """Checks for errors in the consumed message."""
    if msg is None or msg.error():
        return None

    return msg


class BatchBuilder:
    """A helper class to build a batch of messages to send to Kafka."""

    def __init__(self) -> None:
        """Initializes a new BatchBuilder instance."""
        self._builder: list[dict[str, Any]] = []

    def append(
        self,
        *,
        value: bytes | str | None = None,
        key: bytes | str | None = None,
        timestamp: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        """Appends a message to the batch with optional timestamp, key, value, and headers."""
        if key is None and value is None:
            raise KafkaException(
                KafkaError(40, reason="Both key and value can't be None"),
            )

        self._builder.append(
            {
                "timestamp_ms": timestamp or round(time() * 1000),
                "key": key,
                "value": value,
                "headers": headers or [],
            },
        )
