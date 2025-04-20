import logging
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Optional,
    Union,
)

import anyio
import nats
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
    Client,
)
from nats.aio.msg import Msg
from nats.errors import Error
from nats.js.errors import BadRequestError
from typing_extensions import Doc, deprecated, overload, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream.message import gen_cor_id
from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
from faststream.nats.publisher.producer import NatsFastProducer, NatsJSFastProducer
from faststream.nats.response import NatsPublishCommand
from faststream.nats.security import parse_security
from faststream.nats.subscriber.usecases.basic import LogicSubscriber
from faststream.response.publish_type import PublishType

from .logging import make_nats_logger_state
from .registrator import NatsRegistrator
from .state import BrokerState, ConnectedState, EmptyBrokerState

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from nats.aio.client import (
        Callback,
        Credentials,
        ErrorCallback,
        JWTCallback,
        SignatureCallback,
    )
    from nats.js.api import Placement, RePublish, StorageType
    from nats.js.kv import KeyValue
    from nats.js.object_store import ObjectStore
    from typing_extensions import TypedDict, Unpack

    from faststream._internal.basic_types import (
        AnyDict,
        Decorator,
        LoggerProto,
        SendableMessage,
    )
    from faststream._internal.broker.abc_broker import ABCBroker
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.nats.message import NatsMessage
    from faststream.nats.publisher.usecase import LogicPublisher
    from faststream.nats.schemas import PubAck
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

    class NatsInitKwargs(TypedDict, total=False):
        """NatsBroker.connect() method type hints."""

        error_cb: Annotated[
            Optional["ErrorCallback"],
            Doc("Callback to report errors."),
        ]
        disconnected_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report disconnection from NATS."),
        ]
        closed_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report when client stops reconnection to NATS."),
        ]
        discovered_server_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report when a new server joins the cluster."),
        ]
        reconnected_cb: Annotated[
            Optional["Callback"],
            Doc("Callback to report success reconnection."),
        ]
        name: Annotated[
            Optional[str],
            Doc("Label the connection with name (shown in NATS monitoring)."),
        ]
        pedantic: Annotated[
            bool,
            Doc(
                "Turn on NATS server pedantic mode that performs extra checks on the protocol. "
                "https://docs.nats.io/using-nats/developer/connecting/misc#turn-on-pedantic-mode",
            ),
        ]
        verbose: Annotated[
            bool,
            Doc("Verbose mode produce more feedback about code execution."),
        ]
        allow_reconnect: Annotated[
            bool,
            Doc("Whether recover connection automatically or not."),
        ]
        connect_timeout: Annotated[
            int,
            Doc("Timeout in seconds to establish connection with NATS server."),
        ]
        reconnect_time_wait: Annotated[
            int,
            Doc("Time in seconds to wait for reestablish connection to NATS server"),
        ]
        max_reconnect_attempts: Annotated[
            int,
            Doc("Maximum attempts number to reconnect to NATS server."),
        ]
        ping_interval: Annotated[
            int,
            Doc("Interval in seconds to ping."),
        ]
        max_outstanding_pings: Annotated[
            int,
            Doc("Maximum number of failed pings"),
        ]
        dont_randomize: Annotated[
            bool,
            Doc(
                "Boolean indicating should client randomly shuffle servers list for reconnection randomness.",
            ),
        ]
        flusher_queue_size: Annotated[
            int,
            Doc("Max count of commands awaiting to be flushed to the socket"),
        ]
        no_echo: Annotated[
            bool,
            Doc("Boolean indicating should commands be echoed."),
        ]
        tls_hostname: Annotated[
            Optional[str],
            Doc("Hostname for TLS."),
        ]
        token: Annotated[
            Optional[str],
            Doc("Auth token for NATS auth."),
        ]
        drain_timeout: Annotated[
            int,
            Doc("Timeout in seconds to drain subscriptions."),
        ]
        signature_cb: Annotated[
            Optional["SignatureCallback"],
            Doc(
                "A callback used to sign a nonce from the server while "
                "authenticating with nkeys. The user should sign the nonce and "
                "return the base64 encoded signature.",
            ),
        ]
        user_jwt_cb: Annotated[
            Optional["JWTCallback"],
            Doc(
                "A callback used to fetch and return the account "
                "signed JWT for this user.",
            ),
        ]
        user_credentials: Annotated[
            Optional["Credentials"],
            Doc("A user credentials file or tuple of files."),
        ]
        nkeys_seed: Annotated[
            Optional[str],
            Doc("Path-like object containing nkeys seed that will be used."),
        ]
        nkeys_seed_str: Annotated[
            Optional[str],
            Doc("Nkeys seed to be used."),
        ]
        inbox_prefix: Annotated[
            Union[str, bytes],
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID.ß",
            ),
        ]
        pending_size: Annotated[
            int,
            Doc("Max size of the pending buffer for publishing commands."),
        ]
        flush_timeout: Annotated[
            Optional[float],
            Doc("Max duration to wait for a forced flush to occur."),
        ]


class NatsBroker(
    NatsRegistrator,
    BrokerUsecase[Msg, Client],
):
    """A class to represent a NATS broker."""

    url: list[str]

    _producer: "NatsFastProducer"
    _js_producer: "NatsJSFastProducer"
    _kv_declarer: "KVBucketDeclarer"
    _os_declarer: "OSBucketDeclarer"

    def __init__(
        self,
        servers: Annotated[
            Union[str, Iterable[str]],
            Doc("NATS cluster addresses to connect."),
        ] = ("nats://localhost:4222",),
        *,
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
            Optional["Callback"],
            Doc("Callback to report success reconnection."),
        ] = None,
        name: Annotated[
            Optional[str],
            Doc("Label the connection with name (shown in NATS monitoring)."),
        ] = SERVICE_NAME,
        pedantic: Annotated[
            bool,
            Doc(
                "Turn on NATS server pedantic mode that performs extra checks on the protocol. "
                "https://docs.nats.io/using-nats/developer/connecting/misc#turn-on-pedantic-mode",
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
                "Boolean indicating should client randomly shuffle servers list for reconnection randomness.",
            ),
        ] = False,
        flusher_queue_size: Annotated[
            int,
            Doc("Max count of commands awaiting to be flushed to the socket"),
        ] = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: Annotated[
            bool,
            Doc("Boolean indicating should commands be echoed."),
        ] = False,
        tls_hostname: Annotated[
            Optional[str],
            Doc("Hostname for TLS."),
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
                "return the base64 encoded signature.",
            ),
        ] = None,
        user_jwt_cb: Annotated[
            Optional["JWTCallback"],
            Doc(
                "A callback used to fetch and return the account "
                "signed JWT for this user.",
            ),
        ] = None,
        user_credentials: Annotated[
            Optional["Credentials"],
            Doc("A user credentials file or tuple of files."),
        ] = None,
        nkeys_seed: Annotated[
            Optional[str],
            Doc("Path-like object containing nkeys seed that will be used."),
        ] = None,
        nkeys_seed_str: Annotated[
            Optional[str],
            Doc("Raw nkeys seed to be used."),
        ] = None,
        inbox_prefix: Annotated[
            Union[str, bytes],
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID.ß",
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
                "Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.",
            ),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Custom decoder object."),
        ] = None,
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Custom parser object."),
        ] = None,
        dependencies: Annotated[
            Iterable["Dependant"],
            Doc("Dependencies to apply to all broker subscribers."),
        ] = (),
        middlewares: Annotated[
            Sequence["BrokerMiddleware[Msg]"],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ] = (),
        routers: Annotated[
            Sequence["ABCBroker[Msg]"],
            Doc("Routers to apply to broker."),
        ] = (),
        # AsyncAPI args
        security: Annotated[
            Optional["BaseSecurity"],
            Doc(
                "Security options to connect broker and generate AsyncAPI server security information.",
            ),
        ] = None,
        specification_url: Annotated[
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
        tags: Annotated[
            Iterable[Union["Tag", "TagDict"]],
            Doc("AsyncAPI server tags."),
        ] = (),
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
            deprecated("Use `logger` instead. Will be removed in the 0.7.0 release."),
            Doc("Default logger log format."),
        ] = None,
        # FastDepends args
        apply_types: Annotated[
            bool,
            Doc("Whether to use FastDepends or not."),
        ] = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        _get_dependant: Annotated[
            Optional[Callable[..., Any]],
            Doc("Custom library dependant generator callback."),
        ] = None,
        _call_decorators: Annotated[
            Iterable["Decorator"],
            Doc("Any custom decorator to apply to wrapped functions."),
        ] = (),
    ) -> None:
        """Initialize the NatsBroker object."""
        secure_kwargs = parse_security(security)

        servers = [servers] if isinstance(servers, str) else list(servers)

        if specification_url is not None:
            if isinstance(specification_url, str):
                specification_url = [specification_url]
            else:
                specification_url = list(specification_url)
        else:
            specification_url = servers

        super().__init__(
            # NATS options
            servers=servers,
            name=name,
            verbose=verbose,
            allow_reconnect=allow_reconnect,
            reconnect_time_wait=reconnect_time_wait,
            max_reconnect_attempts=max_reconnect_attempts,
            no_echo=no_echo,
            pedantic=pedantic,
            inbox_prefix=inbox_prefix,
            pending_size=pending_size,
            connect_timeout=connect_timeout,
            drain_timeout=drain_timeout,
            flush_timeout=flush_timeout,
            ping_interval=ping_interval,
            max_outstanding_pings=max_outstanding_pings,
            dont_randomize=dont_randomize,
            flusher_queue_size=flusher_queue_size,
            # security
            tls_hostname=tls_hostname,
            token=token,
            user_credentials=user_credentials,
            nkeys_seed=nkeys_seed,
            nkeys_seed_str=nkeys_seed_str,
            **secure_kwargs,
            # callbacks
            error_cb=self._log_connection_broken(error_cb),
            reconnected_cb=self._log_reconnected(reconnected_cb),
            disconnected_cb=disconnected_cb,
            closed_cb=closed_cb,
            discovered_server_cb=discovered_server_cb,
            signature_cb=signature_cb,
            user_jwt_cb=user_jwt_cb,
            # Basic args
            # broker base
            graceful_timeout=graceful_timeout,
            dependencies=dependencies,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            routers=routers,
            # AsyncAPI
            description=description,
            specification_url=specification_url,
            protocol=protocol,
            protocol_version=protocol_version,
            security=security,
            tags=tags,
            # logging
            logger_state=make_nats_logger_state(
                logger=logger,
                log_level=log_level,
                log_fmt=log_fmt,
            ),
            # FastDepends args
            apply_types=apply_types,
            serializer=serializer,
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
        )

        self._state.patch_value(
            producer=NatsFastProducer(
                parser=self._parser,
                decoder=self._decoder,
            )
        )

        self._js_producer = NatsJSFastProducer(
            decoder=self._decoder,
            parser=self._parser,
        )

        self._kv_declarer = KVBucketDeclarer()
        self._os_declarer = OSBucketDeclarer()

        self._connection_state: BrokerState = EmptyBrokerState()

    @override
    async def connect(  # type: ignore[override]
        self,
        servers: Union[str, Iterable[str]] = EMPTY,
        **kwargs: "Unpack[NatsInitKwargs]",
    ) -> "Client":
        """Connect broker object to NATS cluster.

        To startup subscribers too you should use `broker.start()` after/instead this method.

        Args:
            servers: NATS cluster addresses to connect.
            **kwargs: all other options from connection signature.

        Returns:
            `nats.aio.Client` connected object.
        """
        if servers is not EMPTY:
            connect_kwargs: AnyDict = {
                **kwargs,
                "servers": servers,
            }
        else:
            connect_kwargs = {**kwargs}

        return await super().connect(**connect_kwargs)

    async def _connect(self, **kwargs: Any) -> "Client":
        connection = await nats.connect(**kwargs)

        stream = connection.jetstream()

        self._producer.connect(connection)
        self._js_producer.connect(stream)

        self._kv_declarer.connect(stream)
        self._os_declarer.connect(stream)

        self._connection_state = ConnectedState(connection, stream)
        return connection

    async def close(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().close(exc_type, exc_val, exc_tb)

        if self._connection is not None:
            await self._connection.drain()
            self._connection = None

        self._producer.disconnect()
        self._js_producer.disconnect()
        self._kv_declarer.disconnect()
        self._os_declarer.disconnect()

        self._connection_state = EmptyBrokerState()

    async def start(self) -> None:
        """Connect broker to NATS cluster and startup all subscribers."""
        await self.connect()
        self._setup()

        stream_context = self._connection_state.stream

        for stream in filter(
            lambda x: x.declare,
            self._stream_builder.objects.values(),
        ):
            try:
                await stream_context.add_stream(
                    config=stream.config,
                    subjects=stream.subjects,
                )

            except BadRequestError as e:  # noqa: PERF203
                log_context = LogicSubscriber.build_log_context(
                    message=None,
                    subject="",
                    queue="",
                    stream=stream.name,
                )

                logger_state = self._state.get().logger_state

                if (
                    e.description
                    == "stream name already in use with a different configuration"
                ):
                    old_config = (await stream_context.stream_info(stream.name)).config

                    logger_state.log(str(e), logging.WARNING, log_context)

                    for subject in old_config.subjects or ():
                        stream.add_subject(subject)

                    await stream_context.update_stream(config=stream.config)

                else:  # pragma: no cover
                    logger_state.log(
                        str(e),
                        logging.ERROR,
                        log_context,
                        exc_info=e,
                    )

            finally:
                # prevent from double declaration
                stream.declare = False

        await super().start()

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        subject: str,
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        stream: None = None,
        timeout: Optional[float] = None,
    ) -> None: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        subject: str,
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        stream: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> "PubAck": ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        subject: str,
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        stream: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional["PubAck"]:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.

        Args:
            message:
                Message body to send.
                Can be any encodable object (native python types or `pydantic.BaseModel`).
            subject:
                NATS subject to send message.
            headers:
                Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
            reply_to:
                NATS subject name to send response.
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            stream:
                This option validates that the target subject is in presented stream.
                Can be omitted without any effect if you doesn't want PubAck frame.
            timeout:
                Timeout to send message to NATS.

        Returns:
            `None` if you publishes a regular message.
            `faststream.nats.PubAck` if you publishes a message to stream.
        """
        cmd = NatsPublishCommand(
            message=message,
            correlation_id=correlation_id or gen_cor_id(),
            subject=subject,
            headers=headers,
            reply_to=reply_to,
            stream=stream,
            timeout=timeout,
            _publish_type=PublishType.PUBLISH,
        )

        producer = self._js_producer if stream is not None else self._producer

        return await super()._basic_publish(cmd, producer=producer)

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        subject: str,
        headers: Optional[dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        stream: Optional[str] = None,
        timeout: float = 0.5,
    ) -> "NatsMessage":
        """Make a synchronous request to outer subscriber.

        If out subscriber listens subject by stream, you should setup the same **stream** explicitly.
        Another way you will reseave confirmation frame as a response.

        Args:
            message:
                Message body to send.
                Can be any encodable object (native python types or `pydantic.BaseModel`).
            subject:
                NATS subject to send message.
            headers:
                Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway.
            reply_to:
                NATS subject name to send response.
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            stream:
                JetStream name. This option is required if your target subscriber listens for events using JetStream.
            timeout:
                Timeout to send message to NATS.

        Returns:
            `faststream.nats.message.NatsMessage` object as an outer subscriber response.
        """
        cmd = NatsPublishCommand(
            message=message,
            correlation_id=correlation_id or gen_cor_id(),
            subject=subject,
            headers=headers,
            timeout=timeout,
            stream=stream,
            _publish_type=PublishType.REQUEST,
        )

        producer = self._js_producer if stream is not None else self._producer

        msg: NatsMessage = await super()._basic_request(cmd, producer=producer)
        return msg

    @override
    def setup_subscriber(  # type: ignore[override]
        self,
        subscriber: "LogicSubscriber",
    ) -> None:
        return super().setup_subscriber(
            subscriber,
            connection_state=self._connection_state,
            kv_declarer=self._kv_declarer,
            os_declarer=self._os_declarer,
        )

    @override
    def setup_publisher(  # type: ignore[override]
        self,
        publisher: "LogicPublisher",
    ) -> None:
        producer = self._js_producer if publisher.stream is not None else self._producer

        super().setup_publisher(publisher, producer=producer)

    async def key_value(
        self,
        bucket: str,
        *,
        description: Optional[str] = None,
        max_value_size: Optional[int] = None,
        history: int = 1,
        ttl: Optional[float] = None,  # in seconds
        max_bytes: Optional[int] = None,
        storage: Optional["StorageType"] = None,
        replicas: int = 1,
        placement: Optional["Placement"] = None,
        republish: Optional["RePublish"] = None,
        direct: Optional[bool] = None,
        # custom
        declare: bool = True,
    ) -> "KeyValue":
        return await self._kv_declarer.create_key_value(
            bucket=bucket,
            description=description,
            max_value_size=max_value_size,
            history=history,
            ttl=ttl,
            max_bytes=max_bytes,
            storage=storage,
            replicas=replicas,
            placement=placement,
            republish=republish,
            direct=direct,
            declare=declare,
        )

    async def object_storage(
        self,
        bucket: str,
        *,
        description: Optional[str] = None,
        ttl: Optional[float] = None,
        max_bytes: Optional[int] = None,
        storage: Optional["StorageType"] = None,
        replicas: int = 1,
        placement: Optional["Placement"] = None,
        # custom
        declare: bool = True,
    ) -> "ObjectStore":
        return await self._os_declarer.create_object_store(
            bucket=bucket,
            description=description,
            ttl=ttl,
            max_bytes=max_bytes,
            storage=storage,
            replicas=replicas,
            placement=placement,
            declare=declare,
        )

    def _log_connection_broken(
        self,
        error_cb: Optional["ErrorCallback"] = None,
    ) -> "ErrorCallback":
        c = LogicSubscriber.build_log_context(None, "")

        async def wrapper(err: Exception) -> None:
            if error_cb is not None:
                await error_cb(err)

            if isinstance(err, Error) and self._connection_state:
                self._state.get().logger_state.log(
                    f"Connection broken with {err!r}",
                    logging.WARNING,
                    c,
                    exc_info=err,
                )
                self._connection_state = self._connection_state.brake()

        return wrapper

    def _log_reconnected(
        self,
        cb: Optional["Callback"] = None,
    ) -> "Callback":
        c = LogicSubscriber.build_log_context(None, "")

        async def wrapper() -> None:
            if cb is not None:
                await cb()

            if not self._connection_state:
                self._state.get().logger_state.log(
                    "Connection established", logging.INFO, c
                )
                self._connection_state = self._connection_state.reconnect()

        return wrapper

    async def new_inbox(self) -> str:
        """Return a unique inbox that can be used for NATS requests or subscriptions.

        The inbox prefix can be customised by passing `inbox_prefix` when creating your `NatsBroker`.

        This method calls `nats.aio.client.Client.new_inbox` [1] under the hood.

        [1] https://nats-io.github.io/nats.py/modules.html#nats.aio.client.Client.new_inbox
        """
        assert self._connection  # nosec B101

        return self._connection.new_inbox()

    @override
    async def ping(self, timeout: Optional[float]) -> bool:
        sleep_time = (timeout or 10) / 10

        with anyio.move_on_after(timeout) as cancel_scope:
            if self._connection is None:
                return False

            while True:
                if cancel_scope.cancel_called:
                    return False

                if self._connection.is_connected:
                    return True

                await anyio.sleep(sleep_time)

        return False
