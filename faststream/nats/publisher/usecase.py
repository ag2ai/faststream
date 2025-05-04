from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Optional, Union

from nats.aio.msg import Msg
from typing_extensions import overload, override

from faststream._internal.publisher.usecase import PublisherUsecase
from faststream.message import gen_cor_id
from faststream.nats.response import NatsPublishCommand
from faststream.response.publish_type import PublishType

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware
    from faststream.nats.message import NatsMessage
    from faststream.nats.publisher.producer import NatsFastProducer, NatsJSFastProducer
    from faststream.nats.schemas import JStream, PubAck
    from faststream.response.response import PublishCommand


class LogicPublisher(PublisherUsecase[Msg]):
    """A class to represent a NATS publisher."""

    _producer: Union["NatsFastProducer", "NatsJSFastProducer"]

    def __init__(
        self,
        *,
        subject: str,
        reply_to: str,
        headers: Optional[dict[str, str]],
        stream: Optional["JStream"],
        timeout: Optional[float],
        # Publisher args
        broker_middlewares: Iterable["BrokerMiddleware[Msg]"],
        middlewares: Sequence["PublisherMiddleware"],
    ) -> None:
        """Initialize NATS publisher object."""
        super().__init__(
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
        )

        self.subject = subject
        self.stream = stream
        self.timeout = timeout
        self.headers = headers or {}
        self.reply_to = reply_to

    @overload
    async def publish(
        self,
        message: "SendableMessage",
        subject: str = "",
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
        subject: str = "",
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
        subject: str = "",
        headers: Optional[dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        stream: Optional[str] = None,
        timeout: Optional[float] = None,
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
        return await self._basic_publish(cmd, _extra_middlewares=())

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

        return await self._basic_publish(cmd, _extra_middlewares=_extra_middlewares)

    @override
    async def request(
        self,
        message: "SendableMessage",
        subject: str = "",
        headers: Optional[dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        timeout: float = 0.5,
    ) -> "NatsMessage":
        """Make a synchronous request to outer subscriber.

        If out subscriber listens subject by stream, you should setup the same **stream** explicitly.
        Another way you will reseave confirmation frame as a response.

        Note:
            To setup **stream** option, please use `__init__` method.

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
            stream=getattr(self.stream, "name", None),
            _publish_type=PublishType.REQUEST,
        )

        msg: NatsMessage = await self._basic_request(cmd)
        return msg

    def add_prefix(self, prefix: str) -> None:
        self.subject = prefix + self.subject
