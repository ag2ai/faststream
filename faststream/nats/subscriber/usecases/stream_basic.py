from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Annotated, Any, Optional

from nats.errors import ConnectionClosedError, TimeoutError
from typing_extensions import Doc, override

from faststream._internal.endpoint.utils import process_msg
from faststream.nats.parser import JsParser

from .basic import DefaultSubscriber

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.nats.message import NatsMessage
    from faststream.nats.schemas import JStream
    from faststream.nats.subscriber.config import NatsSubscriberConfig


class StreamSubscriber(DefaultSubscriber["Msg"]):
    _fetch_sub: Optional["JetStreamContext.PullSubscription"]

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Msg]",
        *,
        stream: "JStream",
        queue: str,
    ) -> None:
        parser = JsParser(pattern=config.subject)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)

        self.queue = queue
        self.stream = stream

    def get_log_context(
        self,
        message: Annotated[
            Optional["StreamMessage[Msg]"],
            Doc("Message which we are building context for"),
        ],
    ) -> dict[str, str]:
        """Log context factory using in `self.consume` scope."""
        return self.build_log_context(
            message=message,
            subject=self._resolved_subject_string,
            queue=self.queue,
            stream=self.stream.name,
        )

    @override
    async def get_one(self, *, timeout: float = 5) -> Optional["NatsMessage"]:
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if not self._fetch_sub:
            extra_options = {
                "pending_bytes_limit": self.extra_options["pending_bytes_limit"],
                "pending_msgs_limit": self.extra_options["pending_msgs_limit"],
                "durable": self.extra_options["durable"],
                "stream": self.extra_options["stream"],
            }
            if inbox_prefix := self.extra_options.get("inbox_prefix"):
                extra_options["inbox_prefix"] = inbox_prefix

            self._fetch_sub = await self.jetstream.pull_subscribe(
                subject=self.clear_subject,
                config=self.config,
                **extra_options,
            )

        try:
            raw_message = (
                await self._fetch_sub.fetch(
                    batch=1,
                    timeout=timeout,
                )
            )[0]
        except (TimeoutError, ConnectionClosedError):
            return None

        context = self._outer_config.fd_config.context

        msg: NatsMessage = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["NatsMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        if not self._fetch_sub:
            extra_options = {
                "pending_bytes_limit": self.extra_options["pending_bytes_limit"],
                "pending_msgs_limit": self.extra_options["pending_msgs_limit"],
                "durable": self.extra_options["durable"],
                "stream": self.extra_options["stream"],
            }
            if inbox_prefix := self.extra_options.get("inbox_prefix"):
                extra_options["inbox_prefix"] = inbox_prefix

            self._fetch_sub = await self.jetstream.pull_subscribe(
                subject=self.clear_subject,
                config=self.config,
                **extra_options,
            )

        while True:
            raw_message = (
                await self._fetch_sub.fetch(
                    batch=1,
                    timeout=None,
                )
            )[0]

            context = self._outer_config.fd_config.context

            msg: NatsMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
            )
            yield msg
