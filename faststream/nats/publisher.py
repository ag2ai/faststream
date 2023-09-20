from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from nats.aio.msg import Msg

from faststream._compat import override
from faststream.broker.publisher import BasePublisher
from faststream.nats.js_stream import JsStream
from faststream.nats.producer import NatsFastProducer, NatsJSFastProducer
from faststream.types import DecodedMessage, SendableMessage


@dataclass
class LogicPublisher(BasePublisher[Msg]):
    subject: str = field(default="")
    reply_to: str = field(default="")
    headers: Optional[Dict[str, str]] = field(default=None)
    stream: Optional[JsStream] = field(default=None)
    timeout: Optional[float] = field(default=None)

    _producer: Union[NatsFastProducer, NatsJSFastProducer, None] = field(
        default=None, init=False
    )

    @override
    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage = "",
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        **producer_kwargs: Any,
    ) -> Optional[DecodedMessage]:
        assert self._producer, "Please, setup `_producer` first"  # nosec B101
        assert self.subject, "You have to specify outcome subject"

        if self.stream is not None:
            extra = {
                "stream": self.stream.name,
                "timeout": self.timeout,
            }
        else:
            extra = {
                "reply_to": reply_to or self.reply_to,
            }

        return await self._producer.publish(
            message=message,
            subject=self.subject,
            headers=headers or self.headers,
            correlation_id=correlation_id,
            **extra,
            **producer_kwargs,
        )
