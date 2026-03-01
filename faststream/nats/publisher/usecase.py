from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from typing_extensions import overload, override

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.message import gen_cor_id
from faststream.nats.response import NatsPublishCommand
from faststream.nats.schemas.js_stream import compile_nats_wildcard
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.producer import ProducerProto
    from faststream._internal.types import PublisherMiddleware
    from faststream.nats.configs import NatsBrokerConfig
    from faststream.nats.message import NatsMessage
    from faststream.nats.schemas import PubAck
    from faststream.response.response import PublishCommand

    from .config import NatsPublisherConfig


class LogicPublisher(PublisherUsecase):
    """A class to represent a NATS publisher."""

    _outer_config: "NatsBrokerConfig"

    def __init__(
        self,
        config: "NatsPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        """Initialize NATS publisher object."""
        super().__init__(config, specification)

        self._subject = config.subject
        self.stream = config.stream
        self.timeout = config.timeout or 0.5
        self.headers = config.headers or {}
        self.reply_to = config.reply_to

    @property
    def clear_subject(self) -> str:
        """Compile `test.{name}` to `test.*` subject."""
        _, path = compile_nats_wildcard(self.subject)
        return path

    @property
    def subject(self) -> str:
        return f"{self._outer_config.prefix}{self._subject}"

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        subject: str = "",
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
        subject: str = "",
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
        subject: str = "",
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        stream: str | None = None,
        timeout: float | None = None,
    ) -> Optional["PubAck"]:
        """Publish message directly.

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
            message,
            subject=subject or self.subject,
            headers=self.headers | (headers or {}),
            reply_to=reply_to or self.reply_to,
            correlation_id=correlation_id or gen_cor_id(),
            stream=stream or getattr(self.stream, "name", None),
            timeout=timeout or self.timeout,
            _publish_type=PublishType.PUBLISH,
        )

        response: PubAck | None
        if cmd.stream:
            response = cast(
                "PubAck",
                await self._basic_publish(
                    cmd,
                    producer=self._outer_config.js_producer,
                    _extra_middlewares=(),
                ),
            )
        else:
            response = await self._basic_publish(
                cmd,
                producer=self._outer_config.producer,
                _extra_middlewares=(),
            )

        return response

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "NatsPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """This method should be called in subscriber flow only."""
        cmd = NatsPublishCommand.from_cmd(cmd)

        cmd.destination = self.subject
        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to

        if self.stream:
            cmd.stream = self.stream.name
            cmd.timeout = self.timeout

        if cmd.stream:
            producer: ProducerProto[Any] = self._outer_config.js_producer
        else:
            producer = self._outer_config.producer

        await self._basic_publish(
            cmd,
            producer=producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(
        self,
        message: "SendableMessage",
        subject: str = "",
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
            correlation_id:
                Manual message **correlation_id** setter.
                **correlation_id** is a useful option to trace messages.
            stream:
                This allows to make RPC calls over JetStream subjects.
            timeout:
                Timeout to send message to NATS.

        Returns:
            `faststream.nats.message.NatsMessage` object as an outer subscriber response.
        """
        cmd = NatsPublishCommand(
            message=message,
            subject=subject or self.subject,
            headers=self.headers | (headers or {}),
            timeout=timeout or self.timeout,
            correlation_id=correlation_id or gen_cor_id(),
            stream=stream or getattr(self.stream, "name", None),
            _publish_type=PublishType.REQUEST,
        )

        if cmd.stream:
            producer: ProducerProto[Any] = self._outer_config.js_producer
        else:
            producer = self._outer_config.producer

        msg: NatsMessage = await self._basic_request(cmd, producer=producer)
        return msg
