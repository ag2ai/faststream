from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Optional

from nats.errors import TimeoutError
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.nats.parser import NatsParser

from .basic import DefaultSubscriber

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription

    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.nats.message import NatsMessage
    from faststream.nats.subscriber.config import NatsSubscriberConfig


class CoreSubscriber(DefaultSubscriber["Msg"]):
    subscription: Optional["Subscription"]
    _fetch_sub: Optional["Subscription"]

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Msg]",
        *,
        queue: str,
    ) -> None:
        parser = NatsParser(
            pattern=config.subject,
            is_ack_disabled=True,  # core subscriber has no ack policy
        )
        config.parser = parser.parse_message
        config.decoder = parser.decode_message
        super().__init__(config, specification, calls)

        self.queue = queue

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "NatsMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if self._fetch_sub is None:
            fetch_sub = self._fetch_sub = await self.connection.subscribe(
                subject=self.clear_subject,
                queue=self.queue,
                **self.extra_options,
            )
        else:
            fetch_sub = self._fetch_sub

        try:
            raw_message = await fetch_sub.next_msg(timeout=timeout)
        except TimeoutError:
            return None

        context = self._outer_config.fd_config.context

        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: NatsMessage = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["NatsMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        if self._fetch_sub is None:
            fetch_sub = self._fetch_sub = await self.connection.subscribe(
                subject=self.clear_subject,
                queue=self.queue,
                **self.extra_options,
            )
        else:
            fetch_sub = self._fetch_sub

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        async for raw_message in fetch_sub.messages:
            msg: NatsMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg

    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self.connection.subscribe(
            subject=self.clear_subject,
            queue=self.queue,
            cb=self.consume,
            **self.extra_options,
        )

    def get_log_context(
        self,
        message: Optional["StreamMessage[Msg]"],
    ) -> dict[str, str]:
        """Log context factory using in `self.consume` scope.

        Args:
            message: Message which we are building context for
        """
        return self.build_log_context(
            message=message,
            subject=self.subject,
            queue=self.queue,
        )


class ConcurrentCoreSubscriber(ConcurrentMixin["Msg"], CoreSubscriber):
    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.start_consume_task()

        self.subscription = await self.connection.subscribe(
            subject=self.clear_subject,
            queue=self.queue,
            cb=self._put_msg,
            **self.extra_options,
        )
