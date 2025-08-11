import logging
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Optional,
    Union,
    cast,
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
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.nats.configs import NatsBrokerConfig
from faststream.nats.publisher.producer import (
    NatsFastProducerImpl,
    NatsJSFastProducer,
)
from faststream.nats.response import NatsPublishCommand
from faststream.nats.security import parse_security
from faststream.nats.subscriber.usecases.basic import LogicSubscriber
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_nats_logger_state
from .registrator import NatsRegistrator

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
    from typing_extensions import TypedDict

    from faststream._internal.basic_types import (
        LoggerProto,
        SendableMessage,
    )
    from faststream._internal.broker.registrator import Registrator
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
    from faststream.nats.message import NatsMessage
    from faststream.nats.schemas import PubAck
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

    class NatsInitKwargs(TypedDict, total=False):
        """NatsBroker.connect() method type hints."""

        error_cb: Annotated[
            "ErrorCallback" | None,
            Doc("Callback to report errors."),
        ]
        disconnected_cb: Annotated[
            "Callback" | None,
            Doc("Callback to report disconnection from NATS."),
        ]
        closed_cb: Annotated[
            "Callback" | None,
            Doc("Callback to report when client stops reconnection to NATS."),
        ]
        discovered_server_cb: Annotated[
            "Callback" | None,
            Doc("Callback to report when a new server joins the cluster."),
        ]
        reconnected_cb: Annotated[
            "Callback" | None,
            Doc("Callback to report success reconnection."),
        ]
        name: Annotated[
            str | None,
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
            str | None,
            Doc("Hostname for TLS."),
        ]
        token: Annotated[
            str | None,
            Doc("Auth token for NATS auth."),
        ]
        drain_timeout: Annotated[
            int,
            Doc("Timeout in seconds to drain subscriptions."),
        ]
        signature_cb: Annotated[
            "SignatureCallback" | None,
            Doc(
                "A callback used to sign a nonce from the server while "
                "authenticating with nkeys. The user should sign the nonce and "
                "return the base64 encoded signature.",
            ),
        ]
        user_jwt_cb: Annotated[
            "JWTCallback" | None,
            Doc(
                "A callback used to fetch and return the account "
                "signed JWT for this user.",
            ),
        ]
        user_credentials: Annotated[
            "Credentials" | None,
            Doc("A user credentials file or tuple of files."),
        ]
        nkeys_seed: Annotated[
            str | None,
            Doc("Path-like object containing nkeys seed that will be used."),
        ]
        nkeys_seed_str: Annotated[
            str | None,
            Doc("Nkeys seed to be used."),
        ]
        inbox_prefix: Annotated[
            str | bytes,
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID.ß",
            ),
        ]
        pending_size: Annotated[
            int,
            Doc("Max size of the pending buffer for publishing commands."),
        ]
        flush_timeout: Annotated[
            float | None,
            Doc("Max duration to wait for a forced flush to occur."),
        ]


class NatsBroker(
    NatsRegistrator,
    BrokerUsecase[Msg, Client],
):
    """A class to represent a NATS broker."""

    url: list[str]

    def __init__(
        self,
        servers: Annotated[
            str | Iterable[str],
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
            str | None,
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
            str | None,
            Doc("Hostname for TLS."),
        ] = None,
        token: Annotated[
            str | None,
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
            str | None,
            Doc("Path-like object containing nkeys seed that will be used."),
        ] = None,
        nkeys_seed_str: Annotated[
            str | None,
            Doc("Raw nkeys seed to be used."),
        ] = None,
        inbox_prefix: Annotated[
            str | bytes,
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID.ß",
            ),
        ] = DEFAULT_INBOX_PREFIX,
        pending_size: Annotated[
            int,
            Doc("Max size of the pending buffer for publishing commands."),
        ] = DEFAULT_PENDING_SIZE,
        flush_timeout: Annotated[
            float | None,
            Doc("Max duration to wait for a forced flush to occur."),
        ] = None,
        # broker args
        graceful_timeout: Annotated[
            float | None,
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
            Sequence["BrokerMiddleware[Any, Any]"],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ] = (),
        routers: Annotated[
            Sequence["Registrator[Msg]"],
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
            str | Iterable[str] | None,
            Doc("AsyncAPI hardcoded server addresses. Use `servers` if not specified."),
        ] = None,
        protocol: Annotated[
            str | None,
            Doc("AsyncAPI server protocol."),
        ] = "nats",
        protocol_version: Annotated[
            str | None,
            Doc("AsyncAPI server protocol version."),
        ] = "custom",
        description: Annotated[
            str | None,
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
        # FastDepends args
        apply_types: Annotated[
            bool,
            Doc("Whether to use FastDepends or not."),
        ] = True,
        serializer: Optional["SerializerProto"] = EMPTY,
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

        js_producer = NatsJSFastProducer(
            parser=parser,
            decoder=decoder,
        )

        producer = NatsFastProducerImpl(
            parser=parser,
            decoder=decoder,
        )

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
            routers=routers,
            config=NatsBrokerConfig(
                producer=producer,
                js_producer=js_producer,
                # both args
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                logger=make_nats_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                ),
                # subscriber args
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
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

    async def _connect(self) -> "Client":
        connection = await nats.connect(**self._connection_kwargs)
        self.config.connect(connection)
        return connection

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)

        if self._connection is not None:
            await self._connection.drain()
            self._connection = None

        self.config.disconnect()

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
        """Connect broker to NATS cluster and startup all subscribers."""
        await self.connect()

        stream_context = self.config.connection_state.stream

        for stream, subjects in filter(
            lambda x: x[0].declare,
            self._stream_builder.objects.values(),
        ):
            try:
                await stream_context.add_stream(
                    config=stream.config,
                    subjects=list(subjects),
                )

            except BadRequestError as e:  # noqa: PERF203
                self._setup_logger()

                log_context = LogicSubscriber.build_log_context(
                    message=None,
                    subject="",
                    queue="",
                    stream=stream.name,
                )

                logger_state = self.config.logger

                if (
                    e.description
                    == "stream name already in use with a different configuration"
                ):
                    old_config = (await stream_context.stream_info(stream.name)).config

                    logger_state.log(str(e), logging.WARNING, log_context)

                    for subject in old_config.subjects or ():
                        subjects.append(subject)

                    stream.config.subjects = list(subjects)
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
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        stream: None = None,
        timeout: float | None = None,
    ) -> None: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        subject: str,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        stream: str | None = None,
        timeout: float | None = None,
    ) -> "PubAck": ...

    @override
    async def publish(
        self,
        message: "SendableMessage",
        subject: str,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        stream: str | None = None,
        timeout: float | None = None,
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
            timeout=timeout or 0.5,
            _publish_type=PublishType.PUBLISH,
        )

        result: PubAck | None
        if stream:
            result = await super()._basic_publish(cmd, producer=self.config.js_producer)
        else:
            result = await super()._basic_publish(cmd, producer=self.config.producer)
        return result

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        subject: str,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        stream: str | None = None,
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

        producer = self.config.js_producer if stream is not None else self.config.producer

        msg: NatsMessage = await super()._basic_request(cmd, producer=producer)
        return msg

    async def key_value(
        self,
        bucket: str,
        *,
        description: str | None = None,
        max_value_size: int | None = None,
        history: int = 1,
        ttl: float | None = None,  # in seconds
        max_bytes: int | None = None,
        storage: Optional["StorageType"] = None,
        replicas: int = 1,
        placement: Optional["Placement"] = None,
        republish: Optional["RePublish"] = None,
        direct: bool | None = None,
        # custom
        declare: bool = True,
    ) -> "KeyValue":
        kv_declarer = cast("KVBucketDeclarer", self.config.kv_declarer)
        return await kv_declarer.create_key_value(
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
        description: str | None = None,
        ttl: float | None = None,
        max_bytes: int | None = None,
        storage: Optional["StorageType"] = None,
        replicas: int = 1,
        placement: Optional["Placement"] = None,
        declare: bool = True,
    ) -> "ObjectStore":
        os_declarer = cast("OSBucketDeclarer", self.config.os_declarer)
        return await os_declarer.create_object_store(
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

            if isinstance(err, Error) and self.config.connection_state:
                self.config.logger.log(
                    f"Connection broken with {err!r}",
                    logging.WARNING,
                    c,
                    exc_info=err,
                )

        return wrapper

    def _log_reconnected(
        self,
        cb: Optional["Callback"] = None,
    ) -> "Callback":
        c = LogicSubscriber.build_log_context(None, "")

        async def wrapper() -> None:
            if cb is not None:
                await cb()

            if not self.config.connection_state:
                self.config.logger.log("Connection established", logging.INFO, c)

        return wrapper

    async def new_inbox(self) -> str:
        """Return a unique inbox that can be used for NATS requests or subscriptions.

        The inbox prefix can be customised by passing `inbox_prefix` when creating your `NatsBroker`.

        This method calls `nats.aio.client.Client.new_inbox` [1] under the hood.

        [1] https://nats-io.github.io/nats.py/modules.html#nats.aio.client.Client.new_inbox
        """
        return self.connection.new_inbox()

    @override
    async def ping(self, timeout: float | None) -> bool:
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

    @property
    def connection(self) -> "Client":
        return self.config.broker_config.connection_state.connection
