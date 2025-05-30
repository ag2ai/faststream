import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Type,
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
from typing_extensions import Annotated, Doc, deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream.broker.fastapi.router import StreamRouter
from faststream.broker.utils import default_filter
from faststream.nats.broker import NatsBroker
from faststream.nats.publisher.asyncapi import AsyncAPIPublisher
from faststream.types import EMPTY

if TYPE_CHECKING:
    import ssl
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

    from faststream.asyncapi import schema as asyncapi
    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
        Filter,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.nats.message import NatsBatchMessage, NatsMessage
    from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub
    from faststream.nats.subscriber.asyncapi import AsyncAPISubscriber
    from faststream.security import BaseSecurity
    from faststream.types import AnyDict, LoggerProto


class NatsRouter(StreamRouter["Msg"]):
    """A class to represent a NATS router."""

    broker_class = NatsBroker
    broker: NatsBroker

    def __init__(
        self,
        servers: Annotated[
            Union[str, Iterable[str]],
            Doc("NATS cluster addresses to connect."),
        ] = ("nats://localhost:4222",),
        *,
        # connection args
        error_cb: Annotated[
            Optional["ErrorCallback"],
            Doc("Callback to report errors."),
        ] = None,
        disconnected_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report disconnection from NATS."),
        ] = None,
        closed_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report when client stops reconnection to NATS."),
        ] = None,
        discovered_server_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report when a new server joins the cluster."),
        ] = None,
        reconnected_cb: Annotated[
            Optional["Callback"], Doc("Callback to report success reconnection.")
        ] = None,
        name: Annotated[
            Optional[str],
            Doc("Label the connection with name (shown in NATS monitoring)."),
        ] = SERVICE_NAME,
        pedantic: Annotated[
            bool,
            Doc(
                "Turn on NATS server pedantic mode that performs extra checks on the protocol. "
                "https://docs.nats.io/using-nats/developer/connecting/misc#turn-on-pedantic-mode"
            ),
        ] = False,
        verbose: Annotated[
            bool,
            Doc("Verbose mode produce more feedback about code execution."),
        ] = False,
        allow_reconnect: Annotated[
            bool,
            Doc("Whether recover connection automatically or not."),
        ] = True,
        connect_timeout: Annotated[
            int,
            Doc("Timeout in seconds to establish connection with NATS server."),
        ] = DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: Annotated[
            int,
            Doc("Time in seconds to wait for reestablish connection to NATS server"),
        ] = DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: Annotated[
            int,
            Doc("Maximum attempts number to reconnect to NATS server."),
        ] = DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: Annotated[
            int,
            Doc("Interval in seconds to ping."),
        ] = DEFAULT_PING_INTERVAL,
        max_outstanding_pings: Annotated[
            int,
            Doc("Maximum number of failed pings"),
        ] = DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: Annotated[
            bool,
            Doc(
                "Boolean indicating should client randomly shuffle servers list for reconnection randomness."
            ),
        ] = False,
        flusher_queue_size: Annotated[
            int, Doc("Max count of commands awaiting to be flushed to the socket")
        ] = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: Annotated[
            bool,
            Doc("Boolean indicating should commands be echoed."),
        ] = False,
        tls: Annotated[
            Optional["ssl.SSLContext"],
            Doc("Some SSL context to make NATS connections secure."),
        ] = None,
        tls_hostname: Annotated[
            Optional[str],
            Doc("Hostname for TLS."),
        ] = None,
        user: Annotated[
            Optional[str],
            Doc("Username for NATS auth."),
        ] = None,
        password: Annotated[
            Optional[str],
            Doc("Username password for NATS auth."),
        ] = None,
        token: Annotated[
            Optional[str],
            Doc("Auth token for NATS auth."),
        ] = None,
        drain_timeout: Annotated[
            int,
            Doc("Timeout in seconds to drain subscriptions."),
        ] = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Annotated[
            Optional["SignatureCallback"],
            Doc(
                "A callback used to sign a nonce from the server while "
                "authenticating with nkeys. The user should sign the nonce and "
                "return the base64 encoded signature."
            ),
        ] = None,
        user_jwt_cb: Annotated[
            Optional["JWTCallback"],
            Doc(
                "A callback used to fetch and return the account "
                "signed JWT for this user."
            ),
        ] = None,
        user_credentials: Annotated[
            Optional["Credentials"],
            Doc("A user credentials file or tuple of files."),
        ] = None,
        nkeys_seed: Annotated[
            Optional[str],
            Doc("Nkeys seed to be used."),
        ] = None,
        nkeys_seed_str: Annotated[
            Optional[str],
            Doc("Raw nkeys seed to be used."),
        ] = None,
        inbox_prefix: Annotated[
            Union[str, bytes],
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID.ß"
            ),
        ] = DEFAULT_INBOX_PREFIX,
        pending_size: Annotated[
            int,
            Doc("Max size of the pending buffer for publishing commands."),
        ] = DEFAULT_PENDING_SIZE,
        flush_timeout: Annotated[
            Optional[float],
            Doc("Max duration to wait for a forced flush to occur."),
        ] = None,
        # broker args
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
        middlewares: Annotated[
            Sequence["BrokerMiddleware[Msg]"],
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
        ] = "nats",
        protocol_version: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol version."),
        ] = "custom",
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI server description."),
        ] = None,
        asyncapi_tags: Annotated[
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
        # StreamRouter options
        setup_state: Annotated[
            bool,
            Doc(
                "Whether to add broker to app scope in lifespan. "
                "You should disable this option at old ASGI servers."
            ),
        ] = True,
        schema_url: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI schema url. You should set this option to `None` to disable AsyncAPI routes at all."
            ),
        ] = "/asyncapi",
        # FastAPI args
        prefix: Annotated[
            str,
            Doc("An optional path prefix for the router."),
        ] = "",
        tags: Annotated[
            Optional[List[Union[str, "Enum"]]],
            Doc(
                """
                A list of tags to be applied to all the *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                """
            ),
        ] = None,
        dependencies: Annotated[
            Optional[Sequence["params.Depends"]],
            Doc(
                """
                A list of dependencies (using `Depends()`) to be applied to all the
                *path and stream operations* in this router.

                Read more about it in the
                [FastAPI docs for Bigger Applications - Multiple Files](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                """
            ),
        ] = None,
        default_response_class: Annotated[
            Type["Response"],
            Doc(
                """
                The default response class to be used.

                Read more in the
                [FastAPI docs for Custom Response - HTML, Stream, File, others](https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class).
                """
            ),
        ] = Default(JSONResponse),
        responses: Annotated[
            Optional[Dict[Union[int, str], "AnyDict"]],
            Doc(
                """
                Additional responses to be shown in OpenAPI.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Additional Responses in OpenAPI](https://fastapi.tiangolo.com/advanced/additional-responses/).

                And in the
                [FastAPI docs for Bigger Applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                """
            ),
        ] = None,
        callbacks: Annotated[
            Optional[List[BaseRoute]],
            Doc(
                """
                OpenAPI callbacks that should apply to all *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for OpenAPI Callbacks](https://fastapi.tiangolo.com/advanced/openapi-callbacks/).
                """
            ),
        ] = None,
        routes: Annotated[
            Optional[List[BaseRoute]],
            Doc(
                """
                **Note**: you probably shouldn't use this parameter, it is inherited
                from Starlette and supported for compatibility.

                ---

                A list of routes to serve incoming HTTP and WebSocket requests.
                """
            ),
            deprecated(
                """
                You normally wouldn't use this parameter with FastAPI, it is inherited
                from Starlette and supported for compatibility.

                In FastAPI, you normally would use the *path operation methods*,
                like `router.get()`, `router.post()`, etc.
                """
            ),
        ] = None,
        redirect_slashes: Annotated[
            bool,
            Doc(
                """
                Whether to detect and redirect slashes in URLs when the client doesn't
                use the same format.
                """
            ),
        ] = True,
        default: Annotated[
            Optional["ASGIApp"],
            Doc(
                """
                Default function handler for this router. Used to handle
                404 Not Found errors.
                """
            ),
        ] = None,
        dependency_overrides_provider: Annotated[
            Optional[Any],
            Doc(
                """
                Only used internally by FastAPI to handle dependency overrides.

                You shouldn't need to use it. It normally points to the `FastAPI` app
                object.
                """
            ),
        ] = None,
        route_class: Annotated[
            Type["APIRoute"],
            Doc(
                """
                Custom route (*path operation*) class to be used by this router.

                Read more about it in the
                [FastAPI docs for Custom Request and APIRoute class](https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router).
                """
            ),
        ] = APIRoute,
        on_startup: Annotated[
            Optional[Sequence[Callable[[], Any]]],
            Doc(
                """
                A list of startup event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """
            ),
        ] = None,
        on_shutdown: Annotated[
            Optional[Sequence[Callable[[], Any]]],
            Doc(
                """
                A list of shutdown event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """
            ),
        ] = None,
        lifespan: Annotated[
            Optional["Lifespan[Any]"],
            Doc(
                """
                A `Lifespan` context manager handler. This replaces `startup` and
                `shutdown` functions with a single context manager.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """
            ),
        ] = None,
        deprecated: Annotated[
            Optional[bool],
            Doc(
                """
                Mark all *path operations* in this router as deprecated.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                """
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc(
                """
                To include (or not) all the *path operations* in this router in the
                generated OpenAPI.

                This affects the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Query Parameters and String Validations](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-from-openapi).
                """
            ),
        ] = True,
        generate_unique_id_function: Annotated[
            Callable[["APIRoute"], str],
            Doc(
                """
                Customize the function used to generate unique IDs for the *path
                operations* shown in the generated OpenAPI.

                This is particularly useful when automatically generating clients or
                SDKs for your API.

                Read more about it in the
                [FastAPI docs about how to Generate Clients](https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function).
                """
            ),
        ] = Default(generate_unique_id),
    ) -> None:
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
            tls=tls,
            tls_hostname=tls_hostname,
            user=user,
            password=password,
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
            # broker options
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            security=security,
            asyncapi_url=asyncapi_url,
            protocol=protocol,
            protocol_version=protocol_version,
            description=description,
            logger=logger,
            log_level=log_level,
            log_fmt=log_fmt,
            asyncapi_tags=asyncapi_tags,
            schema_url=schema_url,
            setup_state=setup_state,
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

    def subscriber(  # type: ignore[override]
        self,
        subject: Annotated[
            str,
            Doc("NATS subject to subscribe."),
        ],
        queue: Annotated[
            str,
            Doc(
                "Subscribers' NATS queue name. Subscribers with same queue name will be load balanced by the NATS "
                "server."
            ),
        ] = "",
        pending_msgs_limit: Annotated[
            Optional[int],
            Doc(
                "Limit of messages, considered by NATS server as possible to be delivered to the client without "
                "been answered. In case of NATS Core, if that limits exceeds, you will receive NATS 'Slow Consumer' "
                "error. "
                "That's literally means that your worker can't handle the whole load. In case of NATS JetStream, "
                "you will no longer receive messages until some of delivered messages will be acked in any way."
            ),
        ] = None,
        pending_bytes_limit: Annotated[
            Optional[int],
            Doc(
                "The number of bytes, considered by NATS server as possible to be delivered to the client without "
                "been answered. In case of NATS Core, if that limit exceeds, you will receive NATS 'Slow Consumer' "
                "error."
                "That's literally means that your worker can't handle the whole load. In case of NATS JetStream, "
                "you will no longer receive messages until some of delivered messages will be acked in any way."
            ),
        ] = None,
        # Core arguments
        max_msgs: Annotated[
            int,
            Doc("Consuming messages limiter. Automatically disconnect if reached."),
        ] = 0,
        # JS arguments
        durable: Annotated[
            Optional[str],
            Doc(
                "Name of the durable consumer to which the the subscription should be bound."
            ),
        ] = None,
        config: Annotated[
            Optional["api.ConsumerConfig"],
            Doc("Configuration of JetStream consumer to be subscribed with."),
        ] = None,
        ordered_consumer: Annotated[
            bool,
            Doc("Enable ordered consumer mode."),
        ] = False,
        idle_heartbeat: Annotated[
            Optional[float],
            Doc("Enable Heartbeats for a consumer to detect failures."),
        ] = None,
        flow_control: Annotated[
            Optional[bool],
            Doc("Enable Flow Control for a consumer."),
        ] = None,
        deliver_policy: Annotated[
            Optional["api.DeliverPolicy"],
            Doc("Deliver Policy to be used for subscription."),
        ] = None,
        headers_only: Annotated[
            Optional[bool],
            Doc(
                "Should be message delivered without payload, only headers and metadata."
            ),
        ] = None,
        # pull arguments
        pull_sub: Annotated[
            Optional["PullSub"],
            Doc(
                "NATS Pull consumer parameters container. "
                "Should be used with `stream` only."
            ),
        ] = None,
        kv_watch: Annotated[
            Union[str, "KvWatch", None],
            Doc("KeyValue watch parameters container."),
        ] = None,
        obj_watch: Annotated[
            Union[bool, "ObjWatch"],
            Doc("ObjecStore watch parameters container."),
        ] = False,
        inbox_prefix: Annotated[
            bytes,
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID."
            ),
        ] = api.INBOX_PREFIX,
        # custom
        ack_first: Annotated[
            bool,
            Doc("Whether to `ack` message at start of consuming or not."),
        ] = False,
        stream: Annotated[
            Union[str, "JStream", None],
            Doc("Subscribe to NATS Stream with `subject` filter."),
        ] = None,
        # broker arguments
        dependencies: Annotated[
            Iterable["params.Depends"],
            Doc("Dependencies list (`[Depends(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Parser to map original **nats-py** Msg to FastStream one."),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[NatsMessage]"],
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        filter: Annotated[
            Union[
                "Filter[NatsMessage]",
                "Filter[NatsBatchMessage]",
            ],
            Doc(
                "Overload subscriber to consume various messages from the same source."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.0**. "
                "Please, create `subscriber` object and use it explicitly instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        max_workers: Annotated[
            int,
            Doc("Number of workers to process messages concurrently."),
        ] = 1,
        retry: Annotated[
            bool,
            Doc("Whether to `nack` message at processing exception."),
        ] = False,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** autoacknowledgement logic or not."),
        ] = False,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not."
            ),
        ] = False,
        # AsyncAPI information
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
        # FastAPI args
        response_model: Annotated[
            Any,
            Doc(
                """
                The type to use for the response.

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
                """
            ),
        ] = Default(None),
        response_model_include: Annotated[
            Optional["IncEx"],
            Doc(
                """
                Configuration passed to Pydantic to include only certain fields in the
                response data.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_include-and-response_model_exclude).
                """
            ),
        ] = None,
        response_model_exclude: Annotated[
            Optional["IncEx"],
            Doc(
                """
                Configuration passed to Pydantic to exclude certain fields in the
                response data.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_include-and-response_model_exclude).
                """
            ),
        ] = None,
        response_model_by_alias: Annotated[
            bool,
            Doc(
                """
                Configuration passed to Pydantic to define if the response model
                should be serialized by alias when an alias is used.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_include-and-response_model_exclude).
                """
            ),
        ] = True,
        response_model_exclude_unset: Annotated[
            bool,
            Doc(
                """
                Configuration passed to Pydantic to define if the response data
                should have all the fields, including the ones that were not set and
                have their default values. This is different from
                `response_model_exclude_defaults` in that if the fields are set,
                they will be included in the response, even if the value is the same
                as the default.

                When `True`, default values are omitted from the response.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#use-the-response_model_exclude_unset-parameter).
                """
            ),
        ] = False,
        response_model_exclude_defaults: Annotated[
            bool,
            Doc(
                """
                Configuration passed to Pydantic to define if the response data
                should have all the fields, including the ones that have the same value
                as the default. This is different from `response_model_exclude_unset`
                in that if the fields are set but contain the same default values,
                they will be excluded from the response.

                When `True`, default values are omitted from the response.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#use-the-response_model_exclude_unset-parameter).
                """
            ),
        ] = False,
        response_model_exclude_none: Annotated[
            bool,
            Doc(
                """
                Configuration passed to Pydantic to define if the response data should
                exclude fields set to `None`.

                This is much simpler (less smart) than `response_model_exclude_unset`
                and `response_model_exclude_defaults`. You probably want to use one of
                those two instead of this one, as those allow returning `None` values
                when it makes sense.

                Read more about it in the
                [FastAPI docs for Response Model - Return Type](https://fastapi.tiangolo.com/tutorial/response-model/#response_model_exclude_none).
                """
            ),
        ] = False,
    ) -> "AsyncAPISubscriber":
        return cast(
            "AsyncAPISubscriber",
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
                filter=filter,
                max_workers=max_workers,
                retry=retry,
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
        subject: Annotated[
            str,
            Doc("NATS subject to send message."),
        ],
        headers: Annotated[
            Optional[Dict[str, str]],
            Doc(
                "Message headers to store metainformation. "
                "**content-type** and **correlation_id** will be set automatically by framework anyway. "
                "Can be overridden by `publish.headers` if specified."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("NATS subject name to send response."),
        ] = "",
        # JS
        stream: Annotated[
            Union[str, "JStream", None],
            Doc(
                "This option validates that the target `subject` is in presented stream. "
                "Can be omitted without any effect."
            ),
        ] = None,
        timeout: Annotated[
            Optional[float],
            Doc("Timeout to send message to NATS."),
        ] = None,
        # specific
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI information
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Optional[Any],
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> AsyncAPIPublisher:
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
