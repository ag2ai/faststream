import logging
import warnings
from contextlib import suppress
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import aiokafka
import aiokafka.admin
import anyio
from aiokafka.partitioner import DefaultPartitioner
from aiokafka.producer.producer import _missing
from typing_extensions import Annotated, Doc, deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream.broker.message import gen_cor_id
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.kafka.broker.logging import KafkaLoggingBroker
from faststream.kafka.broker.registrator import KafkaRegistrator
from faststream.kafka.publisher.producer import AioKafkaFastProducer
from faststream.kafka.schemas.params import (
    AdminClientConnectionParams,
    ConsumerConnectionParams,
)
from faststream.kafka.security import parse_security
from faststream.types import EMPTY
from faststream.utils.data import filter_by_dict

Partition = TypeVar("Partition")

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from types import TracebackType

    from aiokafka import ConsumerRecord
    from aiokafka.abc import AbstractTokenProvider
    from aiokafka.admin.client import AIOKafkaAdminClient
    from fast_depends.dependencies import Depends
    from typing_extensions import TypedDict, Unpack

    from faststream.asyncapi import schema as asyncapi
    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.security import BaseSecurity
    from faststream.types import (
        AnyDict,
        AsyncFunc,
        Decorator,
        LoggerProto,
        SendableMessage,
    )

    class KafkaInitKwargs(TypedDict, total=False):
        request_timeout_ms: Annotated[
            int,
            Doc("Client request timeout in milliseconds."),
        ]
        retry_backoff_ms: Annotated[
            int,
            Doc("Milliseconds to backoff when retrying on errors."),
        ]
        metadata_max_age_ms: Annotated[
            int,
            Doc(
                """
            The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions.
            """
            ),
        ]
        connections_max_idle_ms: Annotated[
            int,
            Doc(
                """
             Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks.
            """
            ),
        ]
        sasl_kerberos_service_name: str
        sasl_kerberos_domain_name: Optional[str]
        sasl_oauth_token_provider: Annotated[
            Optional[AbstractTokenProvider],
            Doc("OAuthBearer token provider instance."),
        ]
        loop: Optional[AbstractEventLoop]
        client_id: Annotated[
            Optional[str],
            Doc(
                """
            A name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
            for logging with respect to consumer group administration.
            """
            ),
        ]
        # publisher args
        acks: Annotated[
            Union[Literal[0, 1, -1, "all"], object],
            Doc(
                """
            One of ``0``, ``1``, ``all``. The number of acknowledgments
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
            """
            ),
        ]
        key_serializer: Annotated[
            Optional[Callable[[Any], bytes]],
            Doc("Used to convert user-supplied keys to bytes."),
        ]
        value_serializer: Annotated[
            Optional[Callable[[Any], bytes]],
            Doc("used to convert user-supplied message values to bytes."),
        ]
        compression_type: Annotated[
            Optional[Literal["gzip", "snappy", "lz4", "zstd"]],
            Doc(
                """
            The compression type for all data generated bythe producer.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression).
            """
            ),
        ]
        max_batch_size: Annotated[
            int,
            Doc(
                """
            Maximum size of buffered data per partition.
            After this amount `send` coroutine will block until batch is drained.
            """
            ),
        ]
        partitioner: Annotated[
            Callable[
                [bytes, List[Partition], List[Partition]],
                Partition,
            ],
            Doc(
                """
            Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            ``partitioner(key_bytes, all_partitions, available_partitions)``.
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the Java client so that
            messages with the same key are assigned to the same partition.
            When a key is :data:`None`, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
            """
            ),
        ]
        max_request_size: Annotated[
            int,
            Doc(
                """
            The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            """
            ),
        ]
        linger_ms: Annotated[
            int,
            Doc(
                """
            The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, if first request is processed faster,
            than `linger_ms`, producer will wait ``linger_ms - process_time``.
            """
            ),
        ]
        enable_idempotence: Annotated[
            bool,
            Doc(
                """
            When set to `True`, the producer will
            ensure that exactly one copy of each message is written in the
            stream. If `False`, producer retries due to broker failures,
            etc., may write duplicates of the retried message in the stream.
            Note that enabling idempotence acks to set to ``all``. If it is not
            explicitly set by the user it will be chosen.
            """
            ),
        ]
        transactional_id: Optional[str]
        transaction_timeout_ms: int


class KafkaBroker(
    KafkaRegistrator,
    KafkaLoggingBroker,
):
    url: List[str]
    _producer: Optional["AioKafkaFastProducer"]
    _admin_client: Optional["AIOKafkaAdminClient"]

    def __init__(
        self,
        bootstrap_servers: Annotated[
            Union[str, Iterable[str]],
            Doc(
                """
            A `host[:port]` string (or list of `host[:port]` strings) that the consumer should contact to bootstrap
            initial cluster metadata.

            This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092.
            """
            ),
        ] = "localhost",
        *,
        # both
        request_timeout_ms: Annotated[
            int,
            Doc("Client request timeout in milliseconds."),
        ] = 40 * 1000,
        retry_backoff_ms: Annotated[
            int,
            Doc("Milliseconds to backoff when retrying on errors."),
        ] = 100,
        metadata_max_age_ms: Annotated[
            int,
            Doc(
                """
            The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions.
            """
            ),
        ] = 5 * 60 * 1000,
        connections_max_idle_ms: Annotated[
            int,
            Doc(
                """
             Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks.
            """
            ),
        ] = 9 * 60 * 1000,
        sasl_kerberos_service_name: str = "kafka",
        sasl_kerberos_domain_name: Optional[str] = None,
        sasl_oauth_token_provider: Annotated[
            Optional["AbstractTokenProvider"],
            Doc("OAuthBearer token provider instance."),
        ] = None,
        loop: Optional["AbstractEventLoop"] = None,
        client_id: Annotated[
            Optional[str],
            Doc(
                """
            A name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
            for logging with respect to consumer group administration.
            """
            ),
        ] = SERVICE_NAME,
        # publisher args
        acks: Annotated[
            Union[Literal[0, 1, -1, "all"], object],
            Doc(
                """
            One of ``0``, ``1``, ``all``. The number of acknowledgments
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
            """
            ),
        ] = _missing,
        key_serializer: Annotated[
            Optional[Callable[[Any], bytes]],
            Doc("Used to convert user-supplied keys to bytes."),
        ] = None,
        value_serializer: Annotated[
            Optional[Callable[[Any], bytes]],
            Doc("used to convert user-supplied message values to bytes."),
        ] = None,
        compression_type: Annotated[
            Optional[Literal["gzip", "snappy", "lz4", "zstd"]],
            Doc(
                """
            The compression type for all data generated bythe producer.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression).
            """
            ),
        ] = None,
        max_batch_size: Annotated[
            int,
            Doc(
                """
            Maximum size of buffered data per partition.
            After this amount `send` coroutine will block until batch is drained.
            """
            ),
        ] = 16 * 1024,
        partitioner: Annotated[
            Callable[
                [bytes, List[Partition], List[Partition]],
                Partition,
            ],
            Doc(
                """
            Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            ``partitioner(key_bytes, all_partitions, available_partitions)``.
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the Java client so that
            messages with the same key are assigned to the same partition.
            When a key is :data:`None`, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
            """
            ),
        ] = DefaultPartitioner(),
        max_request_size: Annotated[
            int,
            Doc(
                """
            The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            """
            ),
        ] = 1024 * 1024,
        linger_ms: Annotated[
            int,
            Doc(
                """
            The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, if first request is processed faster,
            than `linger_ms`, producer will wait ``linger_ms - process_time``.
            """
            ),
        ] = 0,
        enable_idempotence: Annotated[
            bool,
            Doc(
                """
            When set to `True`, the producer will
            ensure that exactly one copy of each message is written in the
            stream. If `False`, producer retries due to broker failures,
            etc., may write duplicates of the retried message in the stream.
            Note that enabling idempotence acks to set to ``all``. If it is not
            explicitly set by the user it will be chosen.
            """
            ),
        ] = False,
        transactional_id: Optional[str] = None,
        transaction_timeout_ms: int = 60 * 1000,
        # broker base args
        graceful_timeout: Annotated[
            Optional[float],
            Doc(
                "Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down."
            ),
        ] = 15.0,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Custom decoder object."),
        ] = None,
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Custom parser object."),
        ] = None,
        dependencies: Annotated[
            Iterable["Depends"],
            Doc("Dependencies to apply to all broker subscribers."),
        ] = (),
        middlewares: Annotated[
            Sequence[
                Union[
                    "BrokerMiddleware[ConsumerRecord]",
                    "BrokerMiddleware[Tuple[ConsumerRecord, ...]]",
                ]
            ],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ] = (),
        # AsyncAPI args
        security: Annotated[
            Optional["BaseSecurity"],
            Doc(
                "Security options to connect broker and generate AsyncAPI server security information."
            ),
        ] = None,
        asyncapi_url: Annotated[
            Union[str, Iterable[str], None],
            Doc("AsyncAPI hardcoded server addresses. Use `servers` if not specified."),
        ] = None,
        protocol: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol."),
        ] = None,
        protocol_version: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol version."),
        ] = "auto",
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI server description."),
        ] = None,
        tags: Annotated[
            Optional[Iterable[Union["asyncapi.Tag", "asyncapi.TagDict"]]],
            Doc("AsyncAPI server tags."),
        ] = None,
        # logging args
        logger: Annotated[
            Optional["LoggerProto"],
            Doc("User specified logger to pass into Context and log service messages."),
        ] = EMPTY,
        log_level: Annotated[
            int,
            Doc("Service messages log level."),
        ] = logging.INFO,
        log_fmt: Annotated[
            Optional[str],
            deprecated(
                "Argument `log_fmt` is deprecated since 0.5.42 and will be removed in 0.6.0. "
                "Pass a pre-configured `logger` instead."
            ),
            Doc("Default logger log format."),
        ] = EMPTY,
        # FastDepends args
        apply_types: Annotated[
            bool,
            Doc("Whether to use FastDepends or not."),
        ] = True,
        validate: Annotated[
            bool,
            Doc("Whether to cast types using Pydantic validation."),
        ] = True,
        _get_dependant: Annotated[
            Optional[Callable[..., Any]],
            Doc("Custom library dependant generator callback."),
        ] = None,
        _call_decorators: Annotated[
            Iterable["Decorator"],
            Doc("Any custom decorator to apply to wrapped functions."),
        ] = (),
    ) -> None:
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

        if asyncapi_url is not None:
            if isinstance(asyncapi_url, str):
                asyncapi_url = [asyncapi_url]
            else:
                asyncapi_url = list(asyncapi_url)
        else:
            asyncapi_url = servers

        super().__init__(
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
            # Basic args
            graceful_timeout=graceful_timeout,
            dependencies=dependencies,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            # AsyncAPI args
            description=description,
            asyncapi_url=asyncapi_url,
            protocol=protocol,
            protocol_version=protocol_version,
            security=security,
            tags=tags,
            # Logging args
            logger=logger,
            log_level=log_level,
            log_fmt=log_fmt,
            # FastDepends args
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
            apply_types=apply_types,
            validate=validate,
        )

        self.client_id = client_id
        self._producer = None
        self._admin_client = None

    async def _close(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        if self._producer is not None:  # pragma: no branch
            await self._producer.stop()
            self._producer = None
        if self._admin_client is not None:
            await self._admin_client.close()
            self._admin_client = None

        await super()._close(exc_type, exc_val, exc_tb)

    @override
    async def connect(  # type: ignore[override]
        self,
        bootstrap_servers: Annotated[
            Union[str, Iterable[str]],
            Doc("Kafka addresses to connect."),
        ] = EMPTY,
        **kwargs: "Unpack[KafkaInitKwargs]",
    ) -> Callable[..., aiokafka.AIOKafkaConsumer]:
        """Connect to Kafka servers manually.

        Consumes the same with `KafkaBroker.__init__` arguments and overrides them.
        To startup subscribers too you should use `broker.start()` after/instead this method.
        """
        if bootstrap_servers is not EMPTY or kwargs:
            warnings.warn(
                "`KafkaBroker().connect(...) options were "
                "deprecated in **FastStream 0.5.40**. "
                "Please, use `KafkaBroker(...)` instead. "
                "All these options will be removed in **FastStream 0.6.0**.",
                DeprecationWarning,
                stacklevel=2,
            )

        if bootstrap_servers is not EMPTY:
            connect_kwargs: AnyDict = {
                **kwargs,
                "bootstrap_servers": bootstrap_servers,
            }

        else:
            connect_kwargs = {**kwargs}

        return await super().connect(**connect_kwargs)

    @override
    async def _connect(  # type: ignore[override]
        self,
        *,
        client_id: str,
        **kwargs: Any,
    ) -> Callable[..., aiokafka.AIOKafkaConsumer]:
        security_params = parse_security(self.security)
        kwargs.update(security_params)

        self._admin_client = aiokafka.admin.client.AIOKafkaAdminClient(
            **filter_by_dict(AdminClientConnectionParams, kwargs),
        )
        producer = aiokafka.AIOKafkaProducer(
            **kwargs,
            client_id=client_id,
        )

        await self._admin_client.start()
        await producer.start()
        self._producer = AioKafkaFastProducer(
            producer=producer,
            parser=self._parser,
            decoder=self._decoder,
        )

        return partial(
            aiokafka.AIOKafkaConsumer,
            **filter_by_dict(ConsumerConnectionParams, kwargs),
        )

    async def start(self) -> None:
        """Connect broker to Kafka and startup all subscribers."""
        await super().start()

        for handler in self._subscribers.values():
            self._log(
                f"`{handler.call_name}` waiting for messages",
                extra=handler.get_log_context(None),
            )
            await handler.start()

    @property
    def _subscriber_setup_extra(self) -> "AnyDict":
        return {
            **super()._subscriber_setup_extra,
            "client_id": self.client_id,
            "builder": self._connection,
        }

    @override
    async def publish(  # type: ignore[override]
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message topic name to send response."),
        ] = "",
        no_confirm: Annotated[
            bool,
            Doc("Do not wait for Kafka publish confirmation."),
        ] = False,
        # extra options to be compatible with test client
        **kwargs: Any,
    ) -> Optional[Any]:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.
        """
        correlation_id = correlation_id or gen_cor_id()

        return await super().publish(
            message,
            producer=self._producer,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            no_confirm=no_confirm,
            **kwargs,
        )

    @override
    async def request(  # type: ignore[override]
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        *,
        key: Annotated[
            Union[bytes, Any, None],
            Doc(
                """
            A key to associate with the message. Can be used to
            determine which partition to send the message to. If partition
            is `None` (and producer's partitioner config is left as default),
            then messages with the same key will be delivered to the same
            partition (but if key is `None`, partition is chosen randomly).
            Must be type `bytes`, or be serializable to bytes via configured
            `key_serializer`.
            """
            ),
        ] = None,
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        timeout: Annotated[
            float,
            Doc("Timeout to send RPC request."),
        ] = 0.5,
    ) -> Optional[Any]:
        correlation_id = correlation_id or gen_cor_id()

        return await super().request(
            message,
            producer=self._producer,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id,
            timeout=timeout,
        )

    async def publish_batch(
        self,
        *msgs: Annotated[
            "SendableMessage",
            Doc("Messages bodies to send."),
        ],
        topic: Annotated[
            str,
            Doc("Topic where the message will be published."),
        ],
        partition: Annotated[
            Optional[int],
            Doc(
                """
            Specify a partition. If not set, the partition will be
            selected using the configured `partitioner`.
            """
            ),
        ] = None,
        timestamp_ms: Annotated[
            Optional[int],
            Doc(
                """
            Epoch milliseconds (from Jan 1 1970 UTC) to use as
            the message timestamp. Defaults to current time.
            """
            ),
        ] = None,
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc("Messages headers to store metainformation."),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message topic name to send response."),
        ] = "",
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        no_confirm: Annotated[
            bool,
            Doc("Do not wait for Kafka publish confirmation."),
        ] = False,
    ) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish_batch

        for m in self._middlewares[::-1]:
            call = partial(m(None).publish_scope, call)

        await call(
            *msgs,
            topic=topic,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            reply_to=reply_to,
            correlation_id=correlation_id,
            no_confirm=no_confirm,
        )

    @override
    async def ping(self, timeout: Optional[float]) -> bool:
        sleep_time = (timeout or 10) / 10

        if self._admin_client is None:
            return False

        with anyio.move_on_after(timeout) as cancel_scope:
            while True:
                if cancel_scope.cancel_called:
                    return False

                with suppress(Exception):
                    await self._admin_client.describe_cluster()
                    return True
                await anyio.sleep(sleep_time)

        return False
