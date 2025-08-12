import logging
from collections.abc import Callable, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Optional,
    Union,
    cast,
)

from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from nats.aio.client import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_INBOX_PREFIX,
    DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
    DEFAULT_MAX_OUTSTANDING_PINGS,
    DEFAULT_MAX_RECONNECT_ATTEMPTS,
    DEFAULT_PENDING_SIZE,
    DEFAULT_PING_INTERVAL,
    DEFAULT_RECONNECT_TIME_WAIT,
)
from nats.js import api
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute
from typing_extensions import deprecated, overload, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.fastapi.router import StreamRouter
from faststream.middlewares import AckPolicy
from faststream.nats.broker import NatsBroker

if TYPE_CHECKING:
    from enum import Enum

    from fastapi import params
    from fastapi.types import IncEx
    from nats.aio.client import (
        Callback,
        Credentials,
        ErrorCallback,
        JWTCallback,
        SignatureCallback,
    )
    from nats.aio.msg import Msg
    from starlette.responses import Response
    from starlette.types import ASGIApp, Lifespan

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.nats.publisher.usecase import LogicPublisher
    from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub
    from faststream.nats.subscriber.usecases import (
        LogicSubscriber,
    )
    from faststream.nats.subscriber.usecases.core_subscriber import (
        ConcurrentCoreSubscriber,
        CoreSubscriber,
    )
    from faststream.nats.subscriber.usecases.key_value_subscriber import (
        KeyValueWatchSubscriber,
    )
    from faststream.nats.subscriber.usecases.object_storage_subscriber import (
        ObjStoreWatchSubscriber,
    )
    from faststream.nats.subscriber.usecases.stream_pull_subscriber import (
        BatchPullStreamSubscriber,
        ConcurrentPullStreamSubscriber,
        PullStreamSubscriber,
    )
    from faststream.nats.subscriber.usecases.stream_push_subscriber import (
        ConcurrentPushStreamSubscriber,
        PushStreamSubscriber,
    )
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict


class NatsRouter(StreamRouter["Msg"]):
    """A class to represent a NATS router."""

    broker_class = NatsBroker
    broker: NatsBroker

    def __init__(
        self,
        servers: str | Iterable[str] = ("nats://localhost:4222",),
        *,
        # connection args
        error_cb: Optional["ErrorCallback"] = None,
        disconnected_cb: Optional["Callback"] = None,
        closed_cb: Optional["Callback"] = None,
        discovered_server_cb: Optional["Callback"] = None,
        reconnected_cb: Optional["Callback"] = None,
        name: str | None = SERVICE_NAME,
        pedantic: bool = False,
        verbose: bool = False,
        allow_reconnect: bool = True,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: int = DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: bool = False,
        flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: bool = False,
        tls_hostname: str | None = None,
        token: str | None = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Optional["SignatureCallback"] = None,
        user_jwt_cb: Optional["JWTCallback"] = None,
        user_credentials: Optional["Credentials"] = None,
        nkeys_seed: str | None = None,
        nkeys_seed_str: str | None = None,
        inbox_prefix: str | bytes = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: float | None = None,
        # broker args
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        middlewares: Sequence["BrokerMiddleware[Msg, Any]"] = (),
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | Iterable[str] | None = None,
        protocol: str | None = "nats",
        protocol_version: str | None = "custom",
        description: str | None = None,
        specification_tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # StreamRouter options
        setup_state: bool = True,
        schema_url: str | None = "/asyncapi",
        context: ContextRepo | None = None,
        # FastAPI args
        prefix: str = "",
        tags: list[Union[str, "Enum"]] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type["Response"] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[BaseRoute] | None = None,
        routes: Annotated[
            list[BaseRoute] | None,
            deprecated(
                """
                You normally wouldn't use this parameter with FastAPI, it is inherited
                from Starlette and supported for compatibility.

                In FastAPI, you normally would use the *path operation methods*,
                like `router.get()`, `router.post()`, etc.
                """,
            ),
        ] = None,
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
            generate_unique_id
        ),
        specification: Optional["SpecificationFactory"] = None,
    ) -> None:
        """FastAPI router for NATS broker.

        Args:
            servers: NATS cluster addresses to connect.
            error_cb: Callback to report errors.
            disconnected_cb: Callback to report disconnection from NATS.
            closed_cb: Callback to report when client stops reconnection to NATS.
            discovered_server_cb: Callback to report when a new server joins the cluster.
            reconnected_cb: Callback to report success reconnection.
            name: Label the connection with name (shown in NATS monitoring).
            pedantic: Turn on NATS server pedantic mode that performs extra checks on the protocol.
                https://docs.nats.io/using-nats/developer/connecting/misc#turn-on-pedantic-mode
            verbose: Verbose mode produce more feedback about code execution.
            allow_reconnect: Whether recover connection automatically or not.
            connect_timeout: Timeout in seconds to establish connection with NATS server.
            reconnect_time_wait: Time in seconds to wait for reestablish connection to NATS server
            max_reconnect_attempts: Maximum attempts number to reconnect to NATS server.
            ping_interval: Interval in seconds to ping.
            max_outstanding_pings: Maximum number of failed pings
            dont_randomize: Boolean indicating should client randomly shuffle servers list for reconnection randomness.
            flusher_queue_size: Max count of commands awaiting to be flushed to the socket
            no_echo: Boolean indicating should commands be echoed.
            tls_hostname: Hostname for TLS.
            token: Auth token for NATS auth.
            drain_timeout: Timeout in seconds to drain subscriptions.
            signature_cb: A callback used to sign a nonce from the server while
                authenticating with nkeys. The user should sign the nonce and
                return the base64 encoded signature.
            user_jwt_cb: A callback used to fetch and return the account
                signed JWT for this user.
            user_credentials: A user credentials file or tuple of files.
            nkeys_seed: Nkeys seed to be used.
            nkeys_seed_str: Raw nkeys seed to be used.
            inbox_prefix: Prefix for generating unique inboxes, subjects with that prefix and NUID.ß
            pending_size: Max size of the pending buffer for publishing commands.
            flush_timeout: Max duration to wait for a forced flush to occur.
            graceful_timeout: Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.
            decoder: Custom decoder object.
            parser: Custom parser object.
            middlewares: Middlewares to apply to all broker publishers/subscribers.
            security: Security options to connect broker and generate AsyncAPI server security information.
            specification_url: AsyncAPI hardcoded server addresses. Use `servers` if not specified.
            protocol: AsyncAPI server protocol.
            protocol_version: AsyncAPI server protocol version.
            description: AsyncAPI server description.
            specification_tags: AsyncAPI server tags.
            logger: User specified logger to pass into Context and log service messages.
            log_level: Service messages log level.
            setup_state: Whether to add broker to app scope in lifespan.
                You should disable this option at old ASGI servers.
            schema_url: AsyncAPI schema url. You should set this option to `None` to disable AsyncAPI routes at all.
            prefix: An optional path prefix for the router.
            specification: Specification factory to use.
            context: Context repository to use.
            tags: A list of tags to be applied to all the *path operations* in this
                router.
                It will be added to the generated OpenAPI (e.g. visible at `/docs`).
                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
            dependencies: A list of dependencies (using `Depends()`) to be applied to all the
                *path and stream operations* in this router.
                Read more about it in the
                [FastAPI docs for Bigger Applications - Multiple Files](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
            default_response_class: The default response class to be used.
                Read more in the
                [FastAPI docs for Custom Response - HTML, Stream, File, others](https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class).
            responses: Additional responses to be shown in OpenAPI.
                It will be added to the generated OpenAPI (e.g. visible at `/docs`).
                Read more about it in the
                [FastAPI docs for Additional Responses in OpenAPI](https://fastapi.tiangolo.com/advanced/additional-responses/).
                And in the
                [FastAPI docs for Bigger Applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
            callbacks: OpenAPI callbacks that should apply to all *path operations* in this
                router.
                It will be added to the generated OpenAPI (e.g. visible at `/docs`).
                Read more about it in the
                [FastAPI docs for OpenAPI Callbacks](https://fastapi.tiangolo.com/advanced/openapi-callbacks/).
            routes: **Note**: you probably shouldn't use this parameter, it is inherited
                from Starlette and supported for compatibility.
                ---
                A list of routes to serve incoming HTTP and WebSocket requests.
            redirect_slashes: Whether to detect and redirect slashes in URLs when the client doesn't
                use the same format.
            default: Default function handler for this router. Used to handle
                404 Not Found errors.
            dependency_overrides_provider: Only used internally by FastAPI to handle dependency overrides.
                You shouldn't need to use it. It normally points to the `FastAPI` app
                object.
            route_class: Custom route (*path operation*) class to be used by this router.
                Read more about it in the
                [FastAPI docs for Custom Request and APIRoute class](https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router).
            on_startup: A list of startup event handler functions.
                You should instead use the `lifespan` handlers.
                Read more in the [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
            on_shutdown: A list of shutdown event handler functions.
                You should instead use the `lifespan` handlers.
                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
            lifespan: A `Lifespan` context manager handler. This replaces `startup` and
                `shutdown` functions with a single context manager.
                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
            deprecated: Mark all *path operations* in this router as deprecated.
                It will be added to the generated OpenAPI (e.g. visible at `/docs`).
                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
            include_in_schema: To include (or not) all the *path operations* in this router in the
                generated OpenAPI.
                This affects the generated OpenAPI (e.g. visible at `/docs`).
                Read more about it in the
                [FastAPI docs for Query Parameters and String Validations](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-from-openapi).
            generate_unique_id_function: Customize the function used to generate unique IDs for the *path
                operations* shown in the generated OpenAPI.
                This is particularly useful when automatically generating clients or
                SDKs for your API.
                Read more about it in the
                [FastAPI docs about how to Generate Clients](https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function).
        """
        super().__init__(
            servers,
            error_cb=error_cb,
            disconnected_cb=disconnected_cb,
            closed_cb=closed_cb,
            discovered_server_cb=discovered_server_cb,
            reconnected_cb=reconnected_cb,
            name=name,
            pedantic=pedantic,
            verbose=verbose,
            allow_reconnect=allow_reconnect,
            connect_timeout=connect_timeout,
            reconnect_time_wait=reconnect_time_wait,
            max_reconnect_attempts=max_reconnect_attempts,
            ping_interval=ping_interval,
            max_outstanding_pings=max_outstanding_pings,
            dont_randomize=dont_randomize,
            flusher_queue_size=flusher_queue_size,
            no_echo=no_echo,
            tls_hostname=tls_hostname,
            token=token,
            drain_timeout=drain_timeout,
            signature_cb=signature_cb,
            user_jwt_cb=user_jwt_cb,
            user_credentials=user_credentials,
            nkeys_seed=nkeys_seed,
            nkeys_seed_str=nkeys_seed_str,
            inbox_prefix=inbox_prefix,
            pending_size=pending_size,
            flush_timeout=flush_timeout,
            specification=specification,
            # broker options
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            security=security,
            specification_url=specification_url,
            protocol=protocol,
            protocol_version=protocol_version,
            description=description,
            logger=logger,
            log_level=log_level,
            specification_tags=specification_tags,
            schema_url=schema_url,
            setup_state=setup_state,
            context=context,
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
        )

    @overload  # type: ignore[override]
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "CoreSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "ConcurrentCoreSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "PushStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "ConcurrentPushStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[True] = ...,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "PullStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[True] = ...,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "ConcurrentPullStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: "PullSub" = ...,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> Union["PullStreamSubscriber", "BatchPullStreamSubscriber"]: ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: Union[str, "KvWatch"] = ...,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "KeyValueWatchSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Union[Literal[True], "ObjWatch"] = ...,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "ObjStoreWatchSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Union[bool, "PullSub"] = False,
        kv_watch: Union[str, "KvWatch", None] = None,
        obj_watch: Union[bool, "ObjWatch"] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream", None] = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "LogicSubscriber[Any]": ...

    @override
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Union[bool, "PullSub"] = False,
        kv_watch: Union[str, "KvWatch", None] = None,
        obj_watch: Union[bool, "ObjWatch"] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream", None] = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
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
        # AsyncAPI information
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
    ) -> "LogicSubscriber[Any]":
        """Creates NATS subscriber object.

        Args:
            subject: NATS subject to subscribe.
            queue: Subscribers' NATS queue name. Subscribers with same queue name will be load balanced by the NATS
                server.
            pending_msgs_limit: Limit of messages, considered by NATS server as possible to be delivered to the
                client without been answered. In case of NATS Core, if that limits exceeds, you will receive NATS
                'Slow Consumer' error. That's literally means that your worker can't handle the whole load. In case of
                NATS JetStream, you will no longer receive messages until some of delivered messages will be acked in
                any way.
            pending_bytes_limit: The number of bytes, considered by NATS server as possible to be delivered to the
                client without been answered. In case of NATS Core, if that limit exceeds, you will receive NATS 'Slow
                Consumer' error. That's literally means that your worker can't handle the whole load. In case of NATS
                JetStream, you will no longer receive messages until some of delivered messages will be acked in any
                way.
            max_msgs: Consuming messages limiter. Automatically disconnect if reached.
            durable: Name of the durable consumer to which the the subscription should be bound.
            config: Configuration of JetStream consumer to be subscribed with.
            ordered_consumer: Enable ordered consumer mode.
            idle_heartbeat: Enable Heartbeats for a consumer to detect failures.
            flow_control: Enable Flow Control for a consumer.
            deliver_policy: Deliver Policy to be used for subscription.
            headers_only: Should be message delivered without payload, only headers and metadata.
            pull_sub: NATS Pull consumer parameters container. Should be used with `stream` only.
            kv_watch: KeyValue watch parameters container.
            obj_watch: ObjectStore watch parameters container.
            inbox_prefix: Prefix for generating unique inboxes, subjects with that prefix and NUID.
            ack_first: Whether to `ack` message at start of consuming or not.
            stream: Subscribe to NATS Stream with `subject` filter.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser: Parser to map original **nats-py** Msg to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            max_workers: Number of workers to process messages concurrently.
            no_ack: Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy: Whether to `ack` message at start of consuming or not.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description. Uses decorated docstring as default.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
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
            response_model_exclude_none:  Configuration passed to Pydantic to define if the response data should
                exclude fields set to `None`.
                This is much simpler (less smart) than `response_model_exclude_unset`
                and `response_model_exclude_defaults`. You probably want to use one of
                those two instead of this one, as those allow returning `None` values
                when it makes sense.
                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_exclude_none).

        Returns:
            LogicSubscriber[Any]: The created subscriber object.
        """
        return cast(
            "LogicSubscriber[Any]",
            super().subscriber(
                subject=subject,
                queue=queue,
                pending_msgs_limit=pending_msgs_limit,
                pending_bytes_limit=pending_bytes_limit,
                max_msgs=max_msgs,
                durable=durable,
                config=config,
                ordered_consumer=ordered_consumer,
                idle_heartbeat=idle_heartbeat,
                flow_control=flow_control,
                deliver_policy=deliver_policy,
                headers_only=headers_only,
                pull_sub=pull_sub,
                kv_watch=kv_watch,
                obj_watch=obj_watch,
                inbox_prefix=inbox_prefix,
                ack_first=ack_first,
                stream=stream,
                parser=parser,
                decoder=decoder,
                middlewares=middlewares,
                max_workers=max_workers,
                ack_policy=ack_policy,
                no_ack=no_ack,
                no_reply=no_reply,
                title=title,
                description=description,
                include_in_schema=include_in_schema,
                dependencies=dependencies,
                # FastAPI args
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        subject: str,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        # JS
        stream: Union[str, "JStream", None] = None,
        timeout: float | None = None,
        # specific
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "LogicPublisher":
        """Creates long-living and AsyncAPI-documented publisher object.

        Args:
            subject: NATS subject to send message.
            headers: Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
                Can be overridden by `publish.headers` if specified.
            reply_to: NATS subject name to send response.
            stream: This option validates that the target `subject` is in presented stream.
                Can be omitted without any effect.
            timeout: Timeout to send message to NATS.
            middlewares: Publisher middlewares to wrap outgoing messages.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
        """
        return self.broker.publisher(
            subject,
            headers=headers,
            reply_to=reply_to,
            stream=stream,
            timeout=timeout,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
