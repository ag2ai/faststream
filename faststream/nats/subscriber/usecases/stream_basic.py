from typing import (
    TYPE_CHECKING,
    Annotated,
    Optional,
)

from nats.errors import ConnectionClosedError, TimeoutError
from typing_extensions import Doc, override

from faststream._internal.subscriber.utils import process_msg
from faststream.nats.parser import (
    JsParser,
)
from faststream.nats.schemas.subscribers import NatsLogicSubscriberOptions

from .basic import DefaultSubscriber

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

    from faststream.message import StreamMessage
    from faststream.nats.message import NatsMessage
    from faststream.nats.schemas import JStream


class StreamSubscriber(DefaultSubscriber["Msg"]):
    _fetch_sub: Optional["JetStreamContext.PullSubscription"]

    def __init__(
        self,
        *,
        stream: "JStream",
        queue: str,
        stream_options: NatsLogicSubscriberOptions,
    ) -> None:
        parser_ = JsParser(pattern=stream_options.subject)

        self.queue = queue
        self.stream = stream
        stream_options.internal_options.default_decoder = parser_.decode_message
        stream_options.internal_options.default_parser = parser_.parse_message
        super().__init__(logic_options=stream_options.internal_options)

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
    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["NatsMessage"]:
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

        if not self._fetch_sub:
            extra_options = {
                "pending_bytes_limit": self.extra_options["pending_bytes_limit"],
                "pending_msgs_limit": self.extra_options["pending_msgs_limit"],
                "durable": self.extra_options["durable"],
                "stream": self.extra_options["stream"],
            }
            if inbox_prefix := self.extra_options.get("inbox_prefix"):
                extra_options["inbox_prefix"] = inbox_prefix

            self._fetch_sub = await self._connection_state.js.pull_subscribe(
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

        context = self._state.get().di_state.context

        msg: NatsMessage = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )
        return msg
