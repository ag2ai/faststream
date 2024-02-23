import logging
import ssl
from types import TracebackType
from typing import (
    Any,
    Iterable,
    Sequence,
)

from fast_depends.dependencies import Depends
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
    Callback,
    Client,
    Credentials,
    ErrorCallback,
    JWTCallback,
    SignatureCallback,
)
from nats.aio.msg import Msg
from nats.js import api
from nats.js.client import JetStreamContext
from typing_extensions import override

from faststream.asyncapi import schema as asyncapi
from faststream.broker.core.broker import BrokerUsecase, default_filter
from faststream.broker.core.handler_wrapper_mixin import WrapperProtocol
from faststream.broker.types import (
    BrokerMiddleware,
    CustomDecoder,
    CustomParser,
    Filter,
    PublisherMiddleware,
    SubscriberMiddleware,
)
from faststream.nats.asyncapi import Handler, Publisher
from faststream.nats.broker.logging import NatsLoggingMixin
from faststream.nats.message import NatsMessage
from faststream.nats.producer import NatsFastProducer, NatsJSFastProducer
from faststream.nats.schemas import JStream, PullSub
from faststream.types import DecodedMessage, SendableMessage

Subject = str

class NatsBroker(
    NatsLoggingMixin,
    BrokerUsecase[Msg, Client],
):
    stream: JetStreamContext | None

    handlers: dict[Subject, Handler]
    _publishers: dict[Subject, Publisher]
    _producer: NatsFastProducer | None
    _js_producer: NatsJSFastProducer | None

    def __init__(
        self,
        servers: str | Sequence[str] = ("nats://localhost:4222",),
        *,
        error_cb: ErrorCallback | None = None,
        disconnected_cb: Callback | None = None,
        closed_cb: Callback | None = None,
        discovered_server_cb: Callback | None = None,
        reconnected_cb: Callback | None = None,
        name: str | None = None,
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
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        token: str | None = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: SignatureCallback | None = None,
        user_jwt_cb: JWTCallback | None = None,
        user_credentials: Credentials | None = None,
        nkeys_seed: str | None = None,
        inbox_prefix: str | bytes = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: float | None = None,
        # broker args
        graceful_timeout: float | None = None,
        apply_types: bool = True,
        validate: bool = True,
        dependencies: Sequence[Depends] = (),
        decoder: CustomDecoder[NatsMessage] | None = None,
        parser: CustomParser[Msg] | None = None,
        middlewares: Iterable[BrokerMiddleware[Msg]] = (),
        # AsyncAPI args
        asyncapi_url: str | list[str] | None = None,
        protocol: str = "nats",
        protocol_version: str | None = "custom",
        description: str | None = None,
        tags: Sequence[asyncapi.Tag] | None = None,
        # logging args
        logger: logging.Logger | None = None,
        log_level: int = logging.INFO,
        log_fmt: str | None = None,
    ) -> None: ...
    async def connect(
        self,
        servers: str | Sequence[str] = ("nats://localhost:4222",),
        *,
        error_cb: ErrorCallback | None = None,
        disconnected_cb: Callback | None = None,
        closed_cb: Callback | None = None,
        discovered_server_cb: Callback | None = None,
        reconnected_cb: Callback | None = None,
        name: str | None = None,
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
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        token: str | None = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: SignatureCallback | None = None,
        user_jwt_cb: JWTCallback | None = None,
        user_credentials: Credentials | None = None,
        nkeys_seed: str | None = None,
        inbox_prefix: str | bytes = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: float | None = None,
    ) -> Client: ...
    @override
    async def _connect(  # type: ignore[override]
        self,
        servers: str | Sequence[str] = ("nats://localhost:4222",),
        *,
        error_cb: ErrorCallback | None = None,
        disconnected_cb: Callback | None = None,
        closed_cb: Callback | None = None,
        discovered_server_cb: Callback | None = None,
        reconnected_cb: Callback | None = None,
        name: str | None = None,
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
        tls: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        token: str | None = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: SignatureCallback | None = None,
        user_jwt_cb: JWTCallback | None = None,
        user_credentials: Credentials | None = None,
        nkeys_seed: str | None = None,
        inbox_prefix: str | bytes = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: float | None = None,
    ) -> Client: ...
    async def _close(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None: ...
    async def start(self) -> None: ...
    def _log_connection_broken(
        self,
        error_cb: ErrorCallback | None = None,
    ) -> ErrorCallback: ...
    def _log_reconnected(
        self,
        cb: Callback | None = None,
    ) -> Callback: ...
    @override
    def subscriber(  # type: ignore[override]
        self,
        subject: str,
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        ack_first: bool = False,
        stream: str | JStream | None = None,
        durable: str | None = None,
        config: api.ConsumerConfig | None = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool = False,
        deliver_policy: api.DeliverPolicy | None = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: PullSub | None = None,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # broker arguments
        dependencies: Sequence[Depends] = (),
        parser: CustomParser[Msg] | None = None,
        decoder: CustomDecoder[NatsMessage] | None = None,
        middlewares: Iterable[SubscriberMiddleware] = (),
        filter: Filter[NatsMessage] = default_filter,
        retry: bool = False,
        no_ack: bool = False,
        max_workers: int = 1,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        **__service_kwargs: Any,
    ) -> WrapperProtocol[Msg]: ...
    @override
    def publisher(  # type: ignore[override]
        self,
        subject: str,
        headers: dict[str, str] | None = None,
        # Core
        reply_to: str = "",
        # JS
        stream: str | JStream | None = None,
        timeout: float | None = None,
        # specific
        middlewares: Iterable[PublisherMiddleware] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Publisher: ...
    @override
    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage,
        subject: str,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        # JS arguments
        stream: str | None = None,
        timeout: float | None = None,
        *,
        rpc: bool = False,
        rpc_timeout: float | None = 30.0,
        raise_timeout: bool = False,
    ) -> DecodedMessage | None: ...
