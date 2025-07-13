from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

from typing_extensions import override

from faststream.asyncapi.schema import (
    Channel,
    ChannelBinding,
    CorrelationId,
    Message,
    Operation,
)
from faststream.asyncapi.schema.bindings import nats
from faststream.asyncapi.utils import resolve_payloads
from faststream.nats.publisher.usecase import LogicPublisher

if TYPE_CHECKING:
    from nats.aio.msg import Msg

    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware
    from faststream.nats.schemas.js_stream import JStream


class AsyncAPIPublisher(LogicPublisher):
    """A class to represent a NATS publisher."""

    def get_name(self) -> str:
        return f"{self.clear_subject}:Publisher"

    def get_schema(self) -> Dict[str, Channel]:
        payloads = self.get_payloads()

        return {
            self.name: Channel(
                description=self.description,
                publish=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    nats=nats.ChannelBinding(
                        subject=self.subject,
                    )
                ),
            )
        }

    @override
    @classmethod
    def create(  # type: ignore[override]
        cls,
        *,
        subject: str,
        reply_to: str,
        headers: Optional[Dict[str, str]],
        stream: Optional["JStream"],
        timeout: Optional[float],
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[Msg]"],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> "AsyncAPIPublisher":
        return cls(
            subject=subject,
            reply_to=reply_to,
            headers=headers,
            stream=stream,
            timeout=timeout,
            # Publisher args
            broker_middlewares=broker_middlewares,
            middlewares=middlewares,
            # AsyncAPI args
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
