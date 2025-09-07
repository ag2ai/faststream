import logging
from collections.abc import Callable, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from confluent_kafka import Message
from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute
from typing_extensions import deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.fastapi.router import StreamRouter
from faststream.confluent.broker import KafkaBroker as KB
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from enum import Enum

    from fastapi import params
    from fastapi.types import IncEx
    from starlette.types import ASGIApp, Lifespan

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.confluent.helpers.config import ConfluentConfig
    from faststream.confluent.message import KafkaMessage
    from faststream.confluent.publisher.usecase import (
        BatchPublisher,
        DefaultPublisher,
    )
    from faststream.confluent.schemas import TopicPartition
    from faststream.confluent.subscriber.usecase import (
        BatchSubscriber,
        ConcurrentDefaultSubscriber,
        DefaultSubscriber,
    )
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict


Partition = TypeVar("Partition")


class KafkaRouter(StreamRouter[Message | tuple[Message, ...]]):
    """A class to represent a Kafka router."""

    broker_class = KB
    broker: KB

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
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        # Specification args
        specification: Optional["SpecificationFactory"] = None,
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str = "auto",
        description: str | None = None,
        specification_tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # StreamRouter options
        setup_state: bool = True,
        context: ContextRepo | None = None,
        schema_url: str | None = "/asyncapi",
        # FastAPI args
        prefix: str = "",
        tags: list[Union[str, "Enum"]] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type["Response"] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[BaseRoute] | None = None,
        routes: list[BaseRoute] | None = None,
        redirect_slashes: bool = True,
        default: Optional["ASGIApp"] = None,
        dependency_overrides_provider: Any | None = None,
        route_class: type["APIRoute"] = APIRoute,
        on_startup: Sequence[Callable[[], Any]] | None = None,
        on_shutdown: Sequence[Callable[[], Any]] | None = None,
        lifespan: Optional["Lifespan[Any]"] = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[["APIRoute"], str] = Default(
            generate_unique_id,
        ),
    ) -> None:
        """Initialize the KafkaRouter.

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
            middlewares: Middlewares to apply to all broker publishers/subscribers.
            specification: Optional specification factory to generate the AsyncAPI schema for the broker.
                If provided, this will be used to customize or override the default schema generation.
            security: Security options to connect broker and generate Specification server security information.
            specification_url: Specification hardcoded server addresses. Use `servers` if not specified.
            protocol: Specification server protocol.
            protocol_version: Specification server protocol version.
            description: Specification server description.
            specification_tags: Specification server tags.
            context: faststream.ContextRepo object to store application injections.
            logger: User specified logger to pass into Context and log service messages.
            log_level: Service messages log level.
            setup_state: Whether to add broker to app scope in lifespan.
                You should disable this option at old ASGI servers.
            schema_url: The URL path where the AsyncAPI schema will be served (e.g. "/asyncapi").
                If set to None, the schema endpoint will not be added automatically.

            prefix: An optional path prefix for the router.
            tags: A list of tags to be applied to all the *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                FastAPI docs for Path Operation Configuration:
                https://fastapi.tiangolo.com/tutorial/path-operation-configuration/
            dependencies: A list of dependencies (using `Depends()`) to be applied to all the
                *path operations* in this router.

                Read more about it in the
                FastAPI docs for Bigger Applications - Multiple Files:
                https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies
            default_response_class: The default response class to be used.

                Read more in the
                FastAPI docs for Custom Response - HTML, Stream, File, others:
                https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class
            responses: Additional responses to be shown in OpenAPI.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                FastAPI docs for Additional Responses in OpenAPI:
                https://fastapi.tiangolo.com/advanced/additional-responses/

                And in the
                FastAPI docs for Bigger Applications:
                https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies
            callbacks: OpenAPI callbacks that should apply to all *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                FastAPI docs for OpenAPI Callbacks:
                https://fastapi.tiangolo.com/advanced/openapi-callbacks/
            routes: **Note**: you probably shouldn't use this parameter, it is inherited
                from Starlette and supported for compatibility.

                ---

                A list of routes to serve incoming HTTP and WebSocket requests.

                You normally wouldn't use this parameter with FastAPI, it is inherited
                from Starlette and supported for compatibility.

                In FastAPI, you normally would use the *path operation methods*,
                like `router.get()`, `router.post()`, etc.
            redirect_slashes: Whether to detect and redirect slashes in URLs when the client doesn't
                use the same format.
            default: Default function handler for this router. Used to handle
                404 Not Found errors.
            dependency_overrides_provider: Only used internally by FastAPI to handle dependency overrides.

                You shouldn't need to use it. It normally points to the `FastAPI` app
                object.
            route_class: Custom route (*path operation*) class to be used by this router.

                Read more about it in the
                FastAPI docs for Custom Request and APIRoute class:
                https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router
            on_startup: A list of startup event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the FastAPI docs for `lifespan`:
                https://fastapi.tiangolo.com/advanced/events/
            on_shutdown: A list of shutdown event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the
                FastAPI docs for `lifespan`:
                https://fastapi.tiangolo.com/advanced/events/
            lifespan: A `Lifespan` context manager handler. This replaces `startup` and
                `shutdown` functions with a single context manager.

                Read more in the
                FastAPI docs for `lifespan`:
                https://fastapi.tiangolo.com/advanced/events/
            deprecated: Mark all *path operations* in this router as deprecated.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                FastAPI docs for Path Operation Configuration:
                https://fastapi.tiangolo.com/tutorial/path-operation-configuration/
            include_in_schema: To include (or not) all the *path operations* in this router in the
                generated OpenAPI.

                This affects the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                FastAPI docs for Query Parameters and String Validations:
                https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-parameters-from-openapi
            generate_unique_id_function: Customize the function used to generate unique IDs for the *path
                operations* shown in the generated OpenAPI.

                This is particularly useful when automatically generating clients or
                SDKs for your API.

                Read more about it in the
                FastAPI docs about how to Generate Clients:
                https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function
        """
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            metadata_max_age_ms=metadata_max_age_ms,
            connections_max_idle_ms=connections_max_idle_ms,
            allow_auto_create_topics=allow_auto_create_topics,
            acks=acks,
            config=config,
            compression_type=compression_type,
            partitioner=partitioner,
            max_request_size=max_request_size,
            linger_ms=linger_ms,
            enable_idempotence=enable_idempotence,
            transactional_id=transactional_id,
            transaction_timeout_ms=transaction_timeout_ms,
            # broker args
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            schema_url=schema_url,
            setup_state=setup_state,
            context=context,
            # logger options
            logger=logger,
            log_level=log_level,
            # Specification options
            security=security,
            protocol=protocol,
            description=description,
            protocol_version=protocol_version,
            specification_tags=specification_tags,
            specification_url=specification_url,
            # FastAPI kwargs
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            routes=routes,
            redirect_slashes=redirect_slashes,
            default=default,
            dependency_overrides_provider=dependency_overrides_provider,
            route_class=route_class,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            lifespan=lifespan,
            generate_unique_id_function=generate_unique_id_function,
            specification=specification,
        )

    @overload  # type: ignore[override]
    def subscriber(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"] = (),
        polling_interval: float = 0.1,
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 50 * 1024 * 1024,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        auto_offset_reset: Literal["latest", "earliest", "none"] = "latest",
        auto_commit: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        auto_commit_interval_ms: int = 5 * 1000,
        check_crcs: bool = True,
        partition_assignment_strategy: Sequence[str] = ("roundrobin",),
        max_poll_interval_ms: int = 5 * 60 * 1000,
        session_timeout_ms: int = 10 * 1000,
        heartbeat_interval_ms: int = 3 * 1000,
        isolation_level: Literal[
            "read_uncommitted",
            "read_committed",
        ] = "read_uncommitted",
        batch: Literal[False] = False,
        max_records: int | None = None,
        # broker args
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # Specification args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "DefaultSubscriber": ...

    @overload
    def subscriber(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"] = (),
        polling_interval: float = 0.1,
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 50 * 1024 * 1024,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        auto_offset_reset: Literal["latest", "earliest", "none"] = "latest",
        auto_commit: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        auto_commit_interval_ms: int = 5 * 1000,
        check_crcs: bool = True,
        partition_assignment_strategy: Sequence[str] = ("roundrobin",),
        max_poll_interval_ms: int = 5 * 60 * 1000,
        session_timeout_ms: int = 10 * 1000,
        heartbeat_interval_ms: int = 3 * 1000,
        isolation_level: Literal[
            "read_uncommitted",
            "read_committed",
        ] = "read_uncommitted",
        batch: Literal[False] = False,
        max_records: int | None = None,
        # broker args
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        max_workers: int = ...,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # Specification args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "ConcurrentDefaultSubscriber": ...

    @overload
    def subscriber(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"] = (),
        polling_interval: float = 0.1,
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 50 * 1024 * 1024,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        auto_offset_reset: Literal["latest", "earliest", "none"] = "latest",
        auto_commit: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        auto_commit_interval_ms: int = 5 * 1000,
        check_crcs: bool = True,
        partition_assignment_strategy: Sequence[str] = ("roundrobin",),
        max_poll_interval_ms: int = 5 * 60 * 1000,
        session_timeout_ms: int = 10 * 1000,
        heartbeat_interval_ms: int = 3 * 1000,
        isolation_level: Literal[
            "read_uncommitted",
            "read_committed",
        ] = "read_uncommitted",
        batch: Literal[True] = ...,
        max_records: int | None = None,
        # broker args
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        max_workers: None = None,
        # Specification args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "BatchSubscriber": ...

    @overload
    def subscriber(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"] = (),
        polling_interval: float = 0.1,
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 50 * 1024 * 1024,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        auto_offset_reset: Literal["latest", "earliest", "none"] = "latest",
        auto_commit: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        auto_commit_interval_ms: int = 5 * 1000,
        check_crcs: bool = True,
        partition_assignment_strategy: Sequence[str] = ("roundrobin",),
        max_poll_interval_ms: int = 5 * 60 * 1000,
        session_timeout_ms: int = 10 * 1000,
        heartbeat_interval_ms: int = 3 * 1000,
        isolation_level: Literal[
            "read_uncommitted",
            "read_committed",
        ] = "read_uncommitted",
        batch: bool = False,
        max_records: int | None = None,
        # broker args
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        max_workers: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # Specification args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> Union[
        "BatchSubscriber",
        "DefaultSubscriber",
        "ConcurrentDefaultSubscriber",
    ]: ...

    @override
    def subscriber(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"] = (),
        polling_interval: float = 0.1,
        group_id: str | None = None,
        group_instance_id: str | None = None,
        fetch_max_wait_ms: int = 500,
        fetch_max_bytes: int = 50 * 1024 * 1024,
        fetch_min_bytes: int = 1,
        max_partition_fetch_bytes: int = 1 * 1024 * 1024,
        auto_offset_reset: Literal["latest", "earliest", "none"] = "latest",
        auto_commit: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        auto_commit_interval_ms: int = 5 * 1000,
        check_crcs: bool = True,
        partition_assignment_strategy: Sequence[str] = ("roundrobin",),
        max_poll_interval_ms: int = 5 * 60 * 1000,
        session_timeout_ms: int = 10 * 1000,
        heartbeat_interval_ms: int = 3 * 1000,
        isolation_level: Literal[
            "read_uncommitted",
            "read_committed",
        ] = "read_uncommitted",
        batch: bool = False,
        max_records: int | None = None,
        # broker args
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[KafkaMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        max_workers: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # Specification args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> Union["BatchSubscriber", "DefaultSubscriber", "ConcurrentDefaultSubscriber"]:
        """Create a subscriber for Kafka topics.

        Args:
            *topics: Kafka topics to consume messages from.
            partitions: Sequence of topic partitions.
            polling_interval: Polling interval in seconds.
            group_id: Name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. If `None`, auto-partition assignment (via
                group coordinator) and offset commits are disabled.
            group_instance_id: A unique string that identifies the consumer instance.
                If set, the consumer is treated as a static member of the group
                and does not participate in consumer group management (e.g.
                partition assignment, rebalances). This can be used to assign
                partitions to specific consumers, rather than letting the group
                assign partitions based on consumer metadata.
            fetch_max_wait_ms: The maximum amount of time in milliseconds
                the server will block before answering the fetch request if
                there isn't sufficient data to immediately satisfy the
                requirement given by `fetch_min_bytes`.
            fetch_max_bytes: The maximum amount of data the server should
                return for a fetch request. This is not an absolute maximum, if
                the first message in the first non-empty partition of the fetch
                is larger than this value, the message will still be returned
                to ensure that the consumer can make progress. NOTE: consumer
                performs fetches to multiple brokers in parallel so memory
                usage will depend on the number of brokers containing
                partitions for the topic.
            fetch_min_bytes: Minimum amount of data the server should
                return for a fetch request, otherwise wait up to
                `fetch_max_wait_ms` for more data to accumulate.
            max_partition_fetch_bytes: The maximum amount of data
                per-partition the server will return. The maximum total memory
                used for a request ``= #partitions * max_partition_fetch_bytes``.
                This size must be at least as large as the maximum message size
                the server allows or else it is possible for the producer to
                send messages larger than the consumer can fetch. If that
                happens, the consumer can get stuck trying to fetch a large
                message on a certain partition.
            auto_offset_reset: A policy for resetting offsets on `OffsetOutOfRangeError` errors:

                * `earliest` will move to the oldest available message
                * `latest` will move to the most recent
                * `none` will raise an exception so you can handle this case
            auto_commit: If `True` the consumer's offset will be
                periodically committed in the background.
            auto_commit_interval_ms: Milliseconds between automatic
                offset commits, if `auto_commit` is `True`.
            check_crcs: Automatically check the CRC32 of the records
                consumed. This ensures no on-the-wire or on-disk corruption to
                the messages occurred. This check adds some overhead, so it may
                be disabled in cases seeking extreme performance.
            partition_assignment_strategy: List of objects to use to
                distribute partition ownership amongst consumer instances when
                group management is used. This preference is implicit in the order
                of the strategies in the list. When assignment strategy changes:
                to support a change to the assignment strategy, new versions must
                enable support both for the old assignment strategy and the new
                one. The coordinator will choose the old assignment strategy until
                all members have been updated. Then it will choose the new
                strategy.
            max_poll_interval_ms: Maximum allowed time between calls to
                consume messages in batches. If this interval
                is exceeded the consumer is considered failed and the group will
                rebalance in order to reassign the partitions to another consumer
                group member. If API methods block waiting for messages, that time
                does not count against this timeout.
            session_timeout_ms: Client group session and failure detection
                timeout. The consumer sends periodic heartbeats
                (`heartbeat.interval.ms`) to indicate its liveness to the broker.
                If no hearts are received by the broker for a group member within
                the session timeout, the broker will remove the consumer from the
                group and trigger a rebalance. The allowed range is configured with
                the **broker** configuration properties
                `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            heartbeat_interval_ms: The expected time in milliseconds
                between heartbeats to the consumer coordinator when using
                Kafka's group management feature. Heartbeats are used to ensure
                that the consumer's session stays active and to facilitate
                rebalancing when new consumers join or leave the group. The
                value must be set lower than `session_timeout_ms`, but typically
                should be set no higher than 1/3 of that value. It can be
                adjusted even lower to control the expected time for normal
                rebalances.
            isolation_level: Controls how to read messages written
                transactionally.

                * `read_committed`, batch consumer will only return
                transactional messages which have been committed.

                * `read_uncommitted` (the default), batch consumer will
                return all messages, even transactional messages which have been
                aborted.

                Non-transactional messages will be returned unconditionally in
                either mode.

                Messages will always be returned in offset order. Hence, in
                `read_committed` mode, batch consumer will only return
                messages up to the last stable offset (ALSO), which is the one less
                than the offset of the first open transaction. In particular any
                messages appearing after messages belonging to ongoing transactions
                will be withheld until the relevant transaction has been completed.
                As a result, `read_committed` consumers will not be able to read up
                to the high watermark when there are in flight transactions.
                Further, when in `read_committed` the seek_to_end method will
                return the ALSO. See method docs below.
            batch: Whether to consume messages in batches or not.
            max_records: Number of messages to consume as one batch.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser: Parser to map original **Message** object to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            no_ack: Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy: Acknowledgement policy for the subscriber.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title: Specification subscriber object title.
            description: Specification subscriber object description.
                Uses decorated docstring as default.
            include_in_schema: Whether to include operation in Specification schema or not.
            max_workers: Number of workers to process messages concurrently.
            response_model: The type to use for the response.

                It could be any valid Pydantic *field* type. So, it doesn't have to
                be a Pydantic model, it could be other things, like a `list`, `dict`,
                etc.

                It will be used for:

                * Documentation: the generated OpenAPI (and the UI at `/docs`) will
                    show it as the response (JSON Schema).
                * Serialization: you could return an arbitrary object and the
                    `response_model` would be used to serialize that object into the
                    corresponding JSON.
                * Filtering: the JSON sent to the client will only contain the data
                    (fields) defined in the `response_model`. If you returned an object
                    that contains an attribute `password` but the `response_model` does
                    not include that field, the JSON sent to the client would not have
                    that `password`.
                * Validation: whatever you return will be serialized with the
                    `response_model`, converting any data as necessary to generate the
                    corresponding JSON. But if the data in the object returned is not
                    valid, that would mean a violation of the contract with the client,
                    so it's an error from the API developer. So, FastAPI will raise an
                    error and return a 500 error code (Internal Server Error).

                Read more about it in the
                [FastAPI docs for Response Model](https://fastapi.tiangolo.com/tutorial/response-model/).

            response_model_include: Configuration passed to Pydantic to include only certain fields in the
                response data.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_include-and-response_model_exclude).

            response_model_exclude: Configuration passed to Pydantic to exclude certain fields in the
                response data.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_include-and-response_model_exclude).

            response_model_by_alias: Configuration passed to Pydantic to define if the response model
                should be serialized by alias when an alias is used.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_include-and-response_model_exclude).

            response_model_exclude_unset: Configuration passed to Pydantic to define if the response data
                should have all the fields, including the ones that were not set and
                have their default values. This is different from
                `response_model_exclude_defaults` in that if the fields are set,
                they will be included in the response, even if the value is the same
                as the default.

                When `True`, default values are omitted from the response.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#use-the-response_model_exclude_unset-parameter).

            response_model_exclude_defaults: Configuration passed to Pydantic to define if the response data
                should have all the fields, including the ones that have the same value
                as the default. This is different from `response_model_exclude_unset`
                in that if the fields are set but contain the same default values,
                they will be excluded from the response.

                When `True`, default values are omitted from the response.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#use-the-response_model_exclude_unset-parameter).

            response_model_exclude_none: Configuration passed to Pydantic to define if the response data should
                exclude fields set to `None`.

                This is much simpler (less smart) than `response_model_exclude_unset`
                and `response_model_exclude_defaults`. You probably want to use one of
                those two instead of this one, as those allow returning `None` values
                when it makes sense.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_exclude_none).
        """
        subscriber = super().subscriber(
            *topics,
            polling_interval=polling_interval,
            max_workers=max_workers,
            partitions=partitions,
            group_id=group_id,
            group_instance_id=group_instance_id,
            fetch_max_wait_ms=fetch_max_wait_ms,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            max_partition_fetch_bytes=max_partition_fetch_bytes,
            auto_offset_reset=auto_offset_reset,
            auto_commit=auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            check_crcs=check_crcs,
            partition_assignment_strategy=partition_assignment_strategy,
            max_poll_interval_ms=max_poll_interval_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            isolation_level=isolation_level,
            batch=batch,
            max_records=max_records,
            # broker args
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            ack_policy=ack_policy,
            no_ack=no_ack,
            no_reply=no_reply,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            # FastAPI args
            response_model=response_model,
            response_model_include=response_model_include,
            response_model_exclude=response_model_exclude,
            response_model_by_alias=response_model_by_alias,
            response_model_exclude_unset=response_model_exclude_unset,
            response_model_exclude_defaults=response_model_exclude_defaults,
            response_model_exclude_none=response_model_exclude_none,
        )

        workers = max_workers or 1

        if batch:
            return cast("BatchSubscriber", subscriber)
        if workers > 1:
            return cast("ConcurrentDefaultSubscriber", subscriber)
        return cast("DefaultSubscriber", subscriber)

    @overload  # type: ignore[override]
    def publisher(
        self,
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        batch: Literal[False] = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # Specification args
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "DefaultPublisher": ...

    @overload
    def publisher(
        self,
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        batch: Literal[True] = ...,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # Specification args
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "BatchPublisher": ...

    @overload
    def publisher(
        self,
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        batch: bool = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # Specification args
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Union["BatchPublisher", "DefaultPublisher"]: ...

    @override
    def publisher(
        self,
        topic: str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        batch: bool = False,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # Specification args
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Union["BatchPublisher", "DefaultPublisher"]:
        """Publish messages to a Kafka topic.

        Args:
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
            headers: Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
                Can be overridden by `publish.headers` if specified.
            reply_to: Topic name to send response.
            batch: Whether to send messages in batches or not.
            middlewares: Publisher middlewares to wrap outgoing messages.
            title: Specification publisher object title.
            description: Specification publisher object description.
            schema: Specification publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema: Whetever to include operation in Specification schema or not.

        Returns:
            Union["BatchPublisher", "DefaultPublisher"]: The publisher instance.
        """
        return self.broker.publisher(
            topic=topic,
            key=key,
            partition=partition,
            headers=headers,
            batch=batch,
            reply_to=reply_to,
            # broker options
            middlewares=middlewares,
            # Specification options
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
