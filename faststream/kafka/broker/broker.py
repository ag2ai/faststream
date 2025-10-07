import asyncio
import logging
from collections.abc import Callable, Iterable, Sequence
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    TypeVar,
    Union,
    overload,
)

import aiokafka
import anyio
from aiokafka.partitioner import DefaultPartitioner
from aiokafka.producer.producer import _missing
from aiokafka.structs import RecordMetadata
from fast_depends import Provider, dependency_provider
from typing_extensions import deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream._internal.utils.data import filter_by_dict
from faststream.exceptions import IncorrectState
from faststream.kafka.configs import KafkaBrokerConfig
from faststream.kafka.publisher.producer import AioKafkaFastProducerImpl
from faststream.kafka.response import KafkaPublishCommand
from faststream.kafka.schemas.params import ConsumerConnectionParams
from faststream.kafka.security import parse_security
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_kafka_logger_state
from .registrator import KafkaRegistrator

Partition = TypeVar("Partition")

if TYPE_CHECKING:
    from types import TracebackType

    from aiokafka.abc import AbstractTokenProvider
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from typing_extensions import TypedDict

    from faststream._internal.basic_types import (
        LoggerProto,
        SendableMessage,
    )
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.kafka.message import KafkaMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

    class KafkaInitKwargs(TypedDict, total=False):
        """Kafka broker initialization keyword arguments.

        Attributes:
            request_timeout_ms: Client request timeout in milliseconds.
            retry_backoff_ms: Milliseconds to backoff when retrying on errors.
            metadata_max_age_ms: The period of time in milliseconds after
                which we force a refresh of metadata even if we haven't seen any
                partition leadership changes to proactively discover any new
                brokers or partitions.
            connections_max_idle_ms: Close idle connections after the number
                of milliseconds specified by this config. Specifying `None` will
                disable idle checks.
            sasl_kerberos_service_name: SASL Kerberos service name.
            sasl_kerberos_domain_name: SASL Kerberos domain name.
            sasl_oauth_token_provider: OAuthBearer token provider instance.
            loop: Event loop instance.
            client_id: A name for this client. This string is passed in
                each request to servers and can be used to identify specific
                server-side log entries that correspond to this client. Also
                submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
                for logging with respect to consumer group administration.
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
            key_serializer: Used to convert user-supplied keys to bytes.
            value_serializer: Used to convert user-supplied message values to bytes.
            compression_type: The compression type for all data generated by the producer.
                Compression is of full batches of data, so the efficacy of batching
                will also impact the compression ratio (more batching means better
                compression).
            max_batch_size: Maximum size of buffered data per partition.
                After this amount `send` coroutine will block until batch is drained.
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
        """

        request_timeout_ms: int
        retry_backoff_ms: int
        metadata_max_age_ms: int
        connections_max_idle_ms: int
        sasl_kerberos_service_name: str
        sasl_kerberos_domain_name: str | None
        sasl_oauth_token_provider: AbstractTokenProvider | None
        loop: asyncio.AbstractEventLoop | None
        client_id: str | None
        # publisher args
        acks: Literal[0, 1, -1, "all"] | object
        key_serializer: Callable[[Any], bytes] | None
        value_serializer: Callable[[Any], bytes] | None
        compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None
        max_batch_size: int
        partitioner: Callable[
            [bytes, list[Partition], list[Partition]],
            Partition,
        ]
        max_request_size: int
        linger_ms: int
        enable_idempotence: bool
        transactional_id: str | None
        transaction_timeout_ms: int


class KafkaBroker(
    KafkaRegistrator,
    BrokerUsecase[
        aiokafka.ConsumerRecord | tuple[aiokafka.ConsumerRecord, ...],
        Callable[..., aiokafka.AIOKafkaConsumer],
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
        sasl_kerberos_service_name: str = "kafka",
        sasl_kerberos_domain_name: str | None = None,
        sasl_oauth_token_provider: Optional["AbstractTokenProvider"] = None,
        loop: Optional["asyncio.AbstractEventLoop"] = None,
        client_id: str | None = SERVICE_NAME,
        # publisher args
        acks: Literal[0, 1, -1, "all"] | object = _missing,
        key_serializer: Callable[[Any], bytes] | None = None,
        value_serializer: Callable[[Any], bytes] | None = None,
        compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None = None,
        max_batch_size: int = 16 * 1024,
        partitioner: Callable[
            [bytes, list[Partition], list[Partition]],
            Partition,
        ] = DefaultPartitioner(),  # noqa: B008
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
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        """Kafka broker constructor.

        Args:
            bootstrap_servers (Union[str, Iterable[str]]):
                A `host[:port]` string (or list of `host[:port]` strings) that the consumer should contact to bootstrap
                initial cluster metadata. This does not have to be the full node list.
                It just needs to have at least one broker that will respond to a
                Metadata API Request. Default port is 9092.
            request_timeout_ms (int):
                Client request timeout in milliseconds.
            retry_backoff_ms (int):
                Milliseconds to backoff when retrying on errors.
            metadata_max_age_ms (int):
                The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any
                partition leadership changes to proactively discover any new brokers or partitions.
            connections_max_idle_ms (int):
                Close idle connections after the number of milliseconds specified by this config. Specifying `None` will
                disable idle checks.
            sasl_kerberos_service_name (str):
                Kerberos service name.
            sasl_kerberos_domain_name (Optional[str]):
                Kerberos domain name.
            sasl_oauth_token_provider (Optional[AbstractTokenProvider]):
                OAuthBearer token provider instance.
            loop (Optional[asyncio.AbstractEventLoop]):
                Event loop to use.
            client_id (Optional[str]):
                A name for this client. This string is passed in each request to servers and can be used to identify specific
                server-side log entries that correspond to this client. Also submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
                for logging with respect to consumer group administration.
            acks (Union[Literal[0, 1, -1, "all"], object]):
                One of ``0``, ``1``, ``all``. The number of acknowledgments the producer requires the leader to have received before considering a
                request complete. This controls the durability of records that are sent. The following settings are common:
                * ``0``: Producer will not wait for any acknowledgment from the server at all. The message will immediately be added to the socket
                  buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the retries
                  configuration will not take effect (as the client won't generally know of any failures). The offset given back for each
                  record will always be set to -1.
                * ``1``: The broker leader will write the record to its local log but will respond without awaiting full acknowledgement from all
                  followers. In this case should the leader fail immediately after acknowledging the record but before the followers have
                  replicated it then the record will be lost.
                * ``all``: The broker leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the
                  record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.
                If unset, defaults to ``acks=1``. If `enable_idempotence` is :data:`True` defaults to ``acks=all``.
            key_serializer (Optional[Callable[[Any], bytes]]):
                Used to convert user-supplied keys to bytes.
            value_serializer (Optional[Callable[[Any], bytes]]):
                Used to convert user-supplied message values to bytes.
            compression_type (Optional[Literal["gzip", "snappy", "lz4", "zstd"]]):
                The compression type for all data generated by the producer.
                Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).
            max_batch_size (int):
                Maximum size of buffered data per partition. After this amount `send` coroutine will block until batch is drained.
            partitioner (Callable):
                Callable used to determine which partition each message is assigned to. Called (after key serialization):
                ``partitioner(key_bytes, all_partitions, available_partitions)``.
                The default partitioner implementation hashes each non-None key using the same murmur2 algorithm as the Java client so that
                messages with the same key are assigned to the same partition. When a key is :data:`None`, the message is delivered to a random partition
                (filtered to partitions with available leaders only, if possible).
            max_request_size (int):
                The maximum size of a request. This is also effectively a cap on the maximum record size. Note that the server
                has its own cap on record size which may be different from this. This setting will limit the number of record batches the producer
                will send in a single request to avoid sending huge requests.
            linger_ms (int):
                The producer groups together any records that arrive in between request transmissions into a single batched request.
                Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client
                may want to reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount of
                artificial delay; that is, if first request is processed faster, than `linger_ms`, producer will wait ``linger_ms - process_time``.
            enable_idempotence (bool):
                When set to `True`, the producer will ensure that exactly one copy of each message is written in the stream.
                If `False`, producer retries due to broker failures, etc., may write duplicates of the retried message in the stream.
                Note that enabling idempotence acks to set to ``all``. If it is not explicitly set by the user it will be chosen.
            transactional_id (Optional[str]):
                Transactional id for the producer.
            transaction_timeout_ms (int):
                Transaction timeout in milliseconds.
            graceful_timeout (Optional[float]):
                Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.
            decoder (Optional[CustomCallable]):
                Custom decoder object.
            parser (Optional[CustomCallable]):
                Custom parser object.
            dependencies (Iterable[Dependant]):
                Dependencies to apply to all broker subscribers.
            middlewares (Sequence[BrokerMiddlewarep[Any, Any]]):
                Middlewares to apply to all broker publishers/subscribers.
            routers (Sequence[Registrator]):
                Routers to apply to broker.
            security (Optional[BaseSecurity]):
                Security options to connect broker and generate AsyncAPI server security information.
            specification_url (Union[str, Iterable[str], None]):
                AsyncAPI hardcoded server addresses. Use `servers` if not specified.
            protocol (Optional[str]):
                AsyncAPI server protocol.
            protocol_version (Optional[str]):
                AsyncAPI server protocol version.
            description (Optional[str]):
                AsyncAPI server description.
            tags (Iterable[Union[Tag, TagDict]]):
                AsyncAPI server tags.
            logger (Optional[LoggerProto]):
                User specified logger to pass into Context and log service messages.
            log_level (int):
                Service messages log level.
            apply_types (bool):
                Whether to use FastDepends or not.
            serializer (Optional[SerializerProto]):
                Serializer to use.
            provider (Optional[Provider]):
                Provider for FastDepends.
            context (Optional[ContextRepo]):
                Context for FastDepends.
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

        connection_params = dict(
            bootstrap_servers=servers,
            # both args
            client_id=client_id,
            api_version=protocol_version,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            metadata_max_age_ms=metadata_max_age_ms,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider,
            loop=loop,
            # publisher args
            acks=acks,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            compression_type=compression_type,
            max_batch_size=max_batch_size,
            partitioner=partitioner,
            max_request_size=max_request_size,
            linger_ms=linger_ms,
            enable_idempotence=enable_idempotence,
            transactional_id=transactional_id,
            transaction_timeout_ms=transaction_timeout_ms,
            **parse_security(security),
        )

        consumer_options, _ = filter_by_dict(
            ConsumerConnectionParams,
            connection_params,
        )
        builder = partial(aiokafka.AIOKafkaConsumer, **consumer_options)

        super().__init__(
            **connection_params,
            routers=routers,
            config=KafkaBrokerConfig(
                client_id=client_id,
                builder=builder,
                producer=AioKafkaFastProducerImpl(
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
    async def _connect(self) -> Callable[..., aiokafka.AIOKafkaConsumer]:
        await self.config.connect(**self._connection_kwargs)
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
        """Connect broker to Kafka and startup all subscribers."""
        await self.connect()
        await super().start()

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[False] = False,
    ) -> "RecordMetadata": ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: Literal[True] = ...,
    ) -> "asyncio.Future[RecordMetadata]": ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> asyncio.Future[RecordMetadata] | RecordMetadata: ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        topic: str = "",
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        no_confirm: bool = False,
    ) -> asyncio.Future[RecordMetadata] | RecordMetadata:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.

        Args:
            message:
                Message body to send.
            topic:
                Topic where the message will be published.
            key:
                A key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is `None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is `None`, partition is chosen randomly).
                Must be type `bytes`, or be serializable to bytes via configured
                `key_serializer`
            partition:
                Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`
            timestamp_ms:
                Epoch milliseconds (from Jan 1 1970 UTC) to use as
                the message timestamp. Defaults to current time.
            headers:
                Message headers to store metainformation.
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            reply_to:
                Reply message topic name to send response.
            no_confirm:
                Do not wait for Kafka publish confirmation.

        Returns:
            `asyncio.Future[RecordMetadata]` if no_confirm = True.
            `RecordMetadata` if no_confirm = False.
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
        return await super()._basic_publish(cmd, producer=self.config.producer)

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        topic: str,
        *,
        key: bytes | Any | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        timeout: float = 0.5,
    ) -> "KafkaMessage":
        """Send a request message and wait for a response.

        Args:
            message: Message body to send.
            topic: Topic where the message will be published.
            key: A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            partition: Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            timestamp_ms: Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            headers: Message headers to store metainformation.
            correlation_id: Manual message **correlation_id** setter.
            **correlation_id** is a useful option to trace messages.
            timeout: Timeout to send RPC request.

        Returns:
            KafkaMessage: The response message.
        """
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

    @overload  # type: ignore[override]
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: Literal[False] = False,
    ) -> "RecordMetadata": ...

    @overload
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: Literal[True] = ...,
    ) -> "asyncio.Future[RecordMetadata]": ...

    @overload
    async def publish_batch(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: bool = False,
    ) -> asyncio.Future[RecordMetadata] | RecordMetadata: ...

    async def publish_batch(
        self,
        *messages: "SendableMessage",
        topic: str = "",
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        no_confirm: bool = False,
    ) -> asyncio.Future[RecordMetadata] | RecordMetadata:
        """Publish a message batch as a single request to broker.

        Args:
            *messages:
                Messages bodies to send.
            topic:
                Topic where the message will be published.
            partition:
                Specify a partition. If not set, the partition will be
                selected using the configured `partitioner`
            timestamp_ms:
                Epoch milliseconds (from Jan 1 1970 UTC) to use as
                the message timestamp. Defaults to current time.
            headers:
                Message headers to store metainformation.
            reply_to:
                Reply message topic name to send response.
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            no_confirm:
                Do not wait for Kafka publish confirmation.

        Returns:
            `asyncio.Future[RecordMetadata]` if no_confirm = True.
            `RecordMetadata` if no_confirm = False.
        """
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

        return await self._basic_publish_batch(cmd, producer=self.config.producer)

    @override
    async def ping(self, timeout: float | None) -> bool:
        sleep_time = (timeout or 10) / 10

        with anyio.move_on_after(timeout) as cancel_scope:
            while True:
                if cancel_scope.cancel_called:
                    return False

                try:
                    await self.config.admin_client.describe_cluster()

                except IncorrectState:
                    return False

                except Exception:
                    await anyio.sleep(sleep_time)

                else:
                    return True

        return False
