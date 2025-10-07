import asyncio
import logging
from collections.abc import Callable, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    TypeVar,
    Union,
    overload,
)

import anyio
from confluent_kafka import Message
from fast_depends import dependency_provider
from typing_extensions import deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.confluent.helpers import (
    AsyncConfluentConsumer,
    ConfluentFastConfig,
)
from faststream.confluent.publisher.producer import AsyncConfluentFastProducerImpl
from faststream.confluent.response import KafkaPublishCommand
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_kafka_logger_state
from .registrator import KafkaRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends import Provider
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import (
        LoggerProto,
        SendableMessage,
    )
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.confluent.helpers.config import ConfluentConfig
    from faststream.confluent.message import KafkaMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

Partition = TypeVar("Partition")


class KafkaBroker(
    KafkaRegistrator,
    BrokerUsecase[
        Message | tuple[Message, ...],
        Callable[..., AsyncConfluentConsumer],
    ],
):
    url: list[str]

    def __init__(
        self,
        bootstrap_servers: str | Iterable[str] = "localhost",
        *,
        # both
        request_timeout_ms: int = 40 * 1000,
        retry_backoff_ms: int = 100,
        metadata_max_age_ms: int = 5 * 60 * 1000,
        connections_max_idle_ms: int = 9 * 60 * 1000,
        client_id: str | None = SERVICE_NAME,
        allow_auto_create_topics: bool = True,
        config: Optional["ConfluentConfig"] = None,
        # publisher args
        acks: Literal[0, 1, -1, "all"] = EMPTY,
        compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None = None,
        partitioner: str
        | Callable[
            [bytes, list[Partition], list[Partition]],
            Partition,
        ] = "consistent_random",
        max_request_size: int = 1024 * 1024,
        linger_ms: int = 0,
        enable_idempotence: bool = False,
        transactional_id: str | None = None,
        transaction_timeout_ms: int = 60 * 1000,
        # broker base args
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[KafkaRegistrator] = (),
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | Iterable[str] | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "auto",
        description: str | None = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        provider: Optional["Provider"] = None,
        serializer: Optional["SerializerProto"] = EMPTY,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        """Initialize KafkaBroker.

        Args:
            bootstrap_servers: A `host[:port]` string (or list of `host[:port]` strings) that the consumer should contact to bootstrap
                initial cluster metadata.

                This does not have to be the full node list.
                It just needs to have at least one broker that will respond to a
                Metadata API Request. Default port is 9092.
            request_timeout_ms: Client request timeout in milliseconds.
            retry_backoff_ms: Milliseconds to backoff when retrying on errors.
            metadata_max_age_ms: The period of time in milliseconds after
                which we force a refresh of metadata even if we haven't seen any
                partition leadership changes to proactively discover any new
                brokers or partitions.
            connections_max_idle_ms: Close idle connections after the number
                of milliseconds specified by this config. Specifying `None` will
                disable idle checks.
            client_id: A name for this client. This string is passed in
                each request to servers and can be used to identify specific
                server-side log entries that correspond to this client. Also
                submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
                for logging with respect to consumer group administration.
            allow_auto_create_topics: Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics.
            config: Extra configuration for the confluent-kafka-python
                producer/consumer. See `confluent_kafka.Config <https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration>`_.
            acks: One of ``0``, ``1``, ``all``. The number of acknowledgments
                the producer requires the leader to have received before considering a
                request complete. This controls the durability of records that are
                sent. The following settings are common:

                * ``0``: Producer will not wait for any acknowledgment from the server
                  at all. The message will immediately be added to the socket
                  buffer and considered sent. No guarantee can be made that the
                  server has received the record in this case, and the retries
                  configuration will not take effect (as the client won't
                  generally know of any failures). The offset given back for each
                  record will always be set to -1.
                * ``1``: The broker leader will write the record to its local log but
                  will respond without awaiting full acknowledgement from all
                  followers. In this case should the leader fail immediately
                  after acknowledging the record but before the followers have
                  replicated it then the record will be lost.
                * ``all``: The broker leader will wait for the full set of in-sync
                  replicas to acknowledge the record. This guarantees that the
                  record will not be lost as long as at least one in-sync replica
                  remains alive. This is the strongest available guarantee.

                If unset, defaults to ``acks=1``. If `enable_idempotence` is
                :data:`True` defaults to ``acks=all``.
            compression_type: The compression type for all data generated bythe producer.
                Compression is of full batches of data, so the efficacy of batching
                will also impact the compression ratio (more batching means better
                compression).
            partitioner: Callable used to determine which partition
                each message is assigned to. Called (after key serialization):
                ``partitioner(key_bytes, all_partitions, available_partitions)``.
                The default partitioner implementation hashes each non-None key
                using the same murmur2 algorithm as the Java client so that
                messages with the same key are assigned to the same partition.
                When a key is :data:`None`, the message is delivered to a random partition
                (filtered to partitions with available leaders only, if possible).
            max_request_size: The maximum size of a request. This is also
                effectively a cap on the maximum record size. Note that the server
                has its own cap on record size which may be different from this.
                This setting will limit the number of record batches the producer
                will send in a single request to avoid sending huge requests.
            linger_ms: The producer groups together any records that arrive
                in between request transmissions into a single batched request.
                Normally this occurs only under load when records arrive faster
                than they can be sent out. However in some circumstances the client
                may want to reduce the number of requests even under moderate load.
                This setting accomplishes this by adding a small amount of
                artificial delay; that is, if first request is processed faster,
                than `linger_ms`, producer will wait ``linger_ms - process_time``.
            enable_idempotence: When set to `True`, the producer will
                ensure that exactly one copy of each message is written in the
                stream. If `False`, producer retries due to broker failures,
                etc., may write duplicates of the retried message in the stream.
                Note that enabling idempotence acks to set to ``all``. If it is not
                explicitly set by the user it will be chosen.
            transactional_id: Transactional ID for the producer.
            transaction_timeout_ms: Transaction timeout in milliseconds.
            graceful_timeout: Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.
            decoder: Custom decoder object.
            parser: Custom parser object.
            dependencies: Dependencies to apply to all broker subscribers.
            middlewares: Middlewares to apply to all broker publishers/subscribers.
            routers: Routers to apply to broker.
            security: Security options to connect broker and generate AsyncAPI server security information.
            specification_url: AsyncAPI hardcoded server addresses. Use `servers` if not specified.
            protocol: AsyncAPI server protocol.
            protocol_version: AsyncAPI server protocol version.
            description: AsyncAPI server description.
            tags: AsyncAPI server tags.
            logger: User specified logger to pass into Context and log service messages.
            log_level: Service messages log level.
            apply_types: Whether to use FastDepends or not.
            serializer: Serializer for FastDepends.
            provider: Provider for FastDepends.
            context: Context for FastDepends.
        """
        if protocol is None:
            if security is not None and security.use_ssl:
                protocol = "kafka-secure"
            else:
                protocol = "kafka"

        servers = (
            [bootstrap_servers]
            if isinstance(bootstrap_servers, str)
            else list(bootstrap_servers)
        )

        if specification_url is not None:
            if isinstance(specification_url, str):
                specification_url = [specification_url]
            else:
                specification_url = list(specification_url)
        else:
            specification_url = servers

        connection_config = ConfluentFastConfig(
            config=config,
            security=security,
            bootstrap_servers=servers,
            client_id=client_id,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            metadata_max_age_ms=metadata_max_age_ms,
            allow_auto_create_topics=allow_auto_create_topics,
            connections_max_idle_ms=connections_max_idle_ms,
            # publisher args
            acks=acks,
            compression_type=compression_type,
            partitioner=partitioner,
            max_request_size=max_request_size,
            linger_ms=linger_ms,
            enable_idempotence=enable_idempotence,
            transactional_id=transactional_id,
            transaction_timeout_ms=transaction_timeout_ms,
        )

        super().__init__(
            routers=routers,
            config=KafkaBrokerConfig(
                connection_config=connection_config,
                client_id=client_id,
                producer=AsyncConfluentFastProducerImpl(
                    parser=parser,
                    decoder=decoder,
                ),
                # both args,
                broker_decoder=decoder,
                broker_parser=parser,
                broker_middlewares=middlewares,
                logger=make_kafka_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                # subscriber args
                graceful_timeout=graceful_timeout,
                broker_dependencies=dependencies,
                extra_context={
                    "broker": self,
                },
            ),
            specification=BrokerSpec(
                description=description,
                url=specification_url,
                protocol=protocol,
                protocol_version=protocol_version,
                security=security,
                tags=tags,
            ),
        )

    @override
    async def _connect(self) -> Callable[..., AsyncConfluentConsumer]:
        await self.config.connect()
        return self.config.broker_config.builder

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    @deprecated(
        "Deprecated in **FastStream 0.5.44**. "
        "Please, use `stop` method instead. "
        "Method `close` will be removed in **FastStream 0.7.0**.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    async def close(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await self.stop(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        await self.connect()
        await super().start()

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[True] = ...,
    ) -> asyncio.Future[Message | None]: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[False] = False,
    ) -> Message | None: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> asyncio.Future[Message | None] | Message | None: ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> asyncio.Future[Message | None] | Message | None:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.

        Args:
            message: Message body to send.
            topic: Topic where the message will be published.
            key: Message key for partitioning.
            partition: Specific partition to publish to.
            timestamp_ms: Message timestamp in milliseconds.
            headers: Message headers to store metainformation.
            correlation_id: Manual message **correlation_id** setter. **correlation_id** is a useful option to trace messages.
            reply_to: Reply message topic name to send response.
            no_confirm: Do not wait for Kafka publish confirmation.

        Returns:
            asyncio.Future: Future object representing the publish operation.
        """
        cmd = KafkaPublishCommand(
            message,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            reply_to=reply_to,
            no_confirm=no_confirm,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )
        result: (
            asyncio.Future[Message | None] | Message | None
        ) = await super()._basic_publish(cmd, producer=self.config.producer)
        return result

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        timeout: float = 0.5,
    ) -> "KafkaMessage":
        cmd = KafkaPublishCommand(
            message,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            timeout=timeout,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.REQUEST,
        )

        msg: KafkaMessage = await super()._basic_request(
            cmd,
            producer=self.config.producer,
        )
        return msg

    @override
    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        topic: str,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: bool = False,
    ) -> None:
        cmd = KafkaPublishCommand(
            *messages,
            topic=topic,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            reply_to=reply_to,
            no_confirm=no_confirm,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )

        await self._basic_publish_batch(cmd, producer=self.config.producer)

    @override
    async def ping(self, timeout: float | None) -> bool:
        sleep_time = (timeout or 10) / 10

        producer = self.config.broker_config.producer

        with anyio.move_on_after(timeout) as cancel_scope:
            if not producer:
                return False

            while True:
                if cancel_scope.cancel_called:
                    return False

                if await producer.ping(timeout=timeout or 3.0):
                    return True

                await anyio.sleep(sleep_time)

        return False
