import asyncio
import logging
from contextlib import suppress
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import anyio
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from faststream.confluent import config as config_module
from faststream.confluent.schemas import TopicPartition
from faststream.exceptions import SetupError
from faststream.log import logger as faststream_logger
from faststream.types import EMPTY
from faststream.utils.functions import call_or_await

if TYPE_CHECKING:
    from typing_extensions import NotRequired, TypedDict

    from faststream.confluent.schemas.params import SecurityOptions
    from faststream.types import AnyDict, LoggerProto

    class _SendKwargs(TypedDict):
        value: Optional[Union[str, bytes]]
        key: Optional[Union[str, bytes]]
        headers: Optional[List[Tuple[str, Union[str, bytes]]]]
        partition: NotRequired[int]
        timestamp: NotRequired[int]
        on_delivery: NotRequired[Callable[..., None]]


class AsyncConfluentProducer:
    """An asynchronous Python Kafka client using the "confluent-kafka" package."""

    def __init__(
        self,
        *,
        logger: Optional["LoggerProto"],
        config: config_module.ConfluentFastConfig,
        bootstrap_servers: Union[str, List[str]] = "localhost",
        client_id: Optional[str] = None,
        metadata_max_age_ms: int = 300000,
        request_timeout_ms: int = 40000,
        acks: Any = EMPTY,
        compression_type: Optional[str] = None,
        partitioner: str = "consistent_random",
        max_request_size: int = 1048576,
        linger_ms: int = 0,
        retry_backoff_ms: int = 100,
        security_protocol: str = "PLAINTEXT",
        connections_max_idle_ms: int = 540000,
        enable_idempotence: bool = False,
        transactional_id: Optional[Union[str, int]] = None,
        transaction_timeout_ms: int = 60000,
        allow_auto_create_topics: bool = True,
        security_config: Optional["SecurityOptions"] = None,
    ) -> None:
        self.logger = logger

        if isinstance(bootstrap_servers, Iterable) and not isinstance(
            bootstrap_servers, str
        ):
            bootstrap_servers = ",".join(bootstrap_servers)

        if compression_type is None:
            compression_type = "none"

        if acks is EMPTY or acks == "all":
            acks = -1

        config_from_params = {
            # "topic.metadata.refresh.interval.ms": 1000,
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "metadata.max.age.ms": metadata_max_age_ms,
            "request.timeout.ms": request_timeout_ms,
            "acks": acks,
            "compression.type": compression_type,
            "partitioner": partitioner,
            "message.max.bytes": max_request_size,
            "linger.ms": linger_ms,
            "enable.idempotence": enable_idempotence,
            "transactional.id": transactional_id,
            "transaction.timeout.ms": transaction_timeout_ms,
            "retry.backoff.ms": retry_backoff_ms,
            "security.protocol": security_protocol.lower(),
            "connections.max.idle.ms": connections_max_idle_ms,
            "allow.auto.create.topics": allow_auto_create_topics,
        }

        self.config = {
            **config_from_params,
            **dict(security_config or {}),
            **config.as_config_dict(),
        }

        self.producer = Producer(self.config, logger=self.logger)  # type: ignore[call-arg]

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
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[List[Tuple[str, Union[str, bytes]]]] = None,
        no_confirm: bool = False,
    ) -> None:
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

        if not no_confirm:
            result_future: asyncio.Future[Optional[Message]] = asyncio.Future()

            def ack_callback(err: Any, msg: Optional[Message]) -> None:
                if err or (msg is not None and (err := msg.error())):
                    result_future.set_exception(KafkaException(err))
                else:
                    result_future.set_result(msg)

            kwargs["on_delivery"] = ack_callback

        # should be sync to prevent segfault
        self.producer.produce(topic, **kwargs)

        if not no_confirm:
            await result_future

    def create_batch(self) -> "BatchBuilder":
        """Creates a batch for sending multiple messages."""
        return BatchBuilder()

    async def send_batch(
        self,
        batch: "BatchBuilder",
        topic: str,
        *,
        partition: Optional[int],
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
        timeout: Optional[float] = 5.0,
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
        partitions: Sequence["TopicPartition"],
        logger: Optional["LoggerProto"],
        config: config_module.ConfluentFastConfig,
        bootstrap_servers: Union[str, List[str]] = "localhost",
        client_id: Optional[str] = "confluent-kafka-consumer",
        group_id: Optional[str] = None,
        group_instance_id: Optional[str] = None,
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
        partition_assignment_strategy: Union[str, List[Any]] = "roundrobin",
        max_poll_interval_ms: int = 300000,
        session_timeout_ms: int = 10000,
        heartbeat_interval_ms: int = 3000,
        security_protocol: str = "PLAINTEXT",
        connections_max_idle_ms: int = 540000,
        isolation_level: str = "read_uncommitted",
        allow_auto_create_topics: bool = True,
        security_config: Optional["SecurityOptions"] = None,
    ) -> None:
        self.logger = logger

        if isinstance(bootstrap_servers, Iterable) and not isinstance(
            bootstrap_servers, str
        ):
            bootstrap_servers = ",".join(bootstrap_servers)

        self.topics = list(topics)
        self.partitions = partitions

        if not isinstance(partition_assignment_strategy, str):
            partition_assignment_strategy = ",".join(
                [
                    x if isinstance(x, str) else x().name
                    for x in partition_assignment_strategy
                ]
            )

        config_from_params = {
            "allow.auto.create.topics": allow_auto_create_topics,
            "topic.metadata.refresh.interval.ms": 1000,
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "group.id": config.config.get(
                "group.id", group_id or "faststream-consumer-group"
            ),
            "group.instance.id": config.config.get(
                "group.instance.id", group_instance_id
            ),
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
        }
        self.allow_auto_create_topics = allow_auto_create_topics

        self.config = {
            **config_from_params,
            **dict(security_config or {}),
            **config.as_config_dict(),
        }
        self.consumer = Consumer(self.config, logger=self.logger)  # type: ignore[call-arg]

        # We shouldn't read messages and close consumer concurrently
        # https://github.com/ag2ai/faststream/issues/1904#issuecomment-2506990895
        self._lock = anyio.Lock()

    @property
    def topics_to_create(self) -> List[str]:
        return list({*self.topics, *(p.topic for p in self.partitions)})

    async def start(self) -> None:
        """Starts the Kafka consumer and subscribes to the specified topics."""
        if self.allow_auto_create_topics:
            await call_or_await(
                create_topics, self.topics_to_create, self.config, self.logger
            )

        elif self.logger:
            self.logger.log(
                logging.WARNING,
                "Auto create topics is disabled. Make sure the topics exist.",
            )

        if self.topics:
            await call_or_await(self.consumer.subscribe, self.topics)

        elif self.partitions:
            await call_or_await(
                self.consumer.assign, [p.to_confluent() for p in self.partitions]
            )

        else:
            raise SetupError("You must provide either `topics` or `partitions` option.")

    async def commit(self, asynchronous: bool = True) -> None:
        """Commits the offsets of all messages returned by the last poll operation."""
        await call_or_await(self.consumer.commit, asynchronous=asynchronous)

    async def stop(self) -> None:
        """Stops the Kafka consumer and releases all resources."""
        # NOTE: If we don't explicitly call commit and then close the consumer, the confluent consumer gets stuck.
        # We are doing this to avoid the issue.
        enable_auto_commit = self.config["enable.auto.commit"]
        try:
            if enable_auto_commit:
                await self.commit(asynchronous=False)

        except Exception as e:
            # No offset stored issue is not a problem - https://github.com/confluentinc/confluent-kafka-python/issues/295#issuecomment-355907183
            if "No offset stored" in str(e):
                pass
            elif self.logger:
                self.logger.log(
                    logging.ERROR,
                    "Consumer closing error occurred.",
                    exc_info=e,
                )

        # Wrap calls to async to make method cancelable by timeout
        async with self._lock:
            await call_or_await(self.consumer.close)

    async def getone(self, timeout: float = 0.1) -> Optional[Message]:
        """Consumes a single message from Kafka."""
        async with self._lock:
            msg = await call_or_await(self.consumer.poll, timeout)
        return check_msg_error(msg)

    async def getmany(
        self,
        timeout: float = 0.1,
        max_records: Optional[int] = 10,
    ) -> Tuple[Message, ...]:
        """Consumes a batch of messages from Kafka and groups them by topic and partition."""
        async with self._lock:
            raw_messages: List[Optional[Message]] = await call_or_await(
                self.consumer.consume,  # type: ignore[arg-type]
                num_messages=max_records or 10,
                timeout=timeout,
            )

        return tuple(x for x in map(check_msg_error, raw_messages) if x is not None)

    async def seek(self, topic: str, partition: int, offset: int) -> None:
        """Seeks to the specified offset in the specified topic and partition."""
        topic_partition = TopicPartition(
            topic=topic, partition=partition, offset=offset
        )
        await call_or_await(self.consumer.seek, topic_partition.to_confluent())


def check_msg_error(msg: Optional[Message]) -> Optional[Message]:
    """Checks for errors in the consumed message."""
    if msg is None or msg.error():
        return None

    return msg


class BatchBuilder:
    """A helper class to build a batch of messages to send to Kafka."""

    def __init__(self) -> None:
        """Initializes a new BatchBuilder instance."""
        self._builder: List[AnyDict] = []

    def append(
        self,
        *,
        timestamp: Optional[int] = None,
        key: Optional[Union[str, bytes]] = None,
        value: Optional[Union[str, bytes]] = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
    ) -> None:
        """Appends a message to the batch with optional timestamp, key, value, and headers."""
        if key is None and value is None:
            raise KafkaException(
                KafkaError(40, reason="Both key and value can't be None")
            )

        self._builder.append(
            {
                "timestamp_ms": timestamp or round(time() * 1000),
                "key": key,
                "value": value,
                "headers": headers or [],
            }
        )


def create_topics(
    topics: List[str],
    config: Dict[str, Optional[Union[str, int, float, bool, Any]]],
    logger_: Optional["LoggerProto"] = None,
) -> None:
    logger_ = logger_ or faststream_logger

    """Creates Kafka topics using the provided configuration."""
    admin_client = AdminClient(
        {x: config[x] for x in ADMINCLIENT_CONFIG_PARAMS if x in config}
    )

    fs = admin_client.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
    )

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
        except Exception as e:  # noqa: PERF203
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                logger_.log(logging.WARN, f"Failed to create topic {topic}: {e}")
        else:
            logger_.log(logging.INFO, f"Topic `{topic}` created.")


ADMINCLIENT_CONFIG_PARAMS = (
    "allow.auto.create.topics",
    "bootstrap.servers",
    "client.id",
    "request.timeout.ms",
    "metadata.max.age.ms",
    "security.protocol",
    "connections.max.idle.ms",
    "sasl.mechanism",
    "sasl.username",
    "sasl.password",
)
