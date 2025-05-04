from collections.abc import AsyncIterator, Iterable
from typing import (
    TYPE_CHECKING,
    Annotated,
    Optional,
)

from nats.errors import TimeoutError
from typing_extensions import Doc, override

from faststream._internal.subscriber.mixins import ConcurrentMixin
from faststream._internal.subscriber.utils import process_msg
from faststream.middlewares import AckPolicy
from faststream.nats.parser import NatsParser

from .basic import DefaultSubscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription
    from nats.js.api import ConsumerConfig

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
    from faststream.message import StreamMessage
    from faststream.nats.message import NatsMessage


class CoreSubscriber(DefaultSubscriber["Msg"]):
    subscription: Optional["Subscription"]
    _fetch_sub: Optional["Subscription"]

    def __init__(
        self,
        *,
        # default args
        subject: str,
        config: "ConsumerConfig",
        queue: str,
        extra_options: Optional["AnyDict"],
        ack_policy: AckPolicy,
        # Subscriber args
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[Msg]"],
    ) -> None:
        parser_ = NatsParser(
            pattern=subject,
            is_ack_disabled=ack_policy is not AckPolicy.DO_NOTHING,
        )

        self.queue = queue

        super().__init__(
            subject=subject,
            config=config,
            extra_options=extra_options,
            # subscriber args
            default_parser=parser_.parse_message,
            default_decoder=parser_.decode_message,
            # Propagated args
            ack_policy=AckPolicy.DO_NOTHING,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "Optional[NatsMessage]":
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

        if self._fetch_sub is None:
            fetch_sub = self._fetch_sub = await self._connection_state.client.subscribe(
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

    @override
    async def __aiter__(self) -> AsyncIterator["NatsMessage"]:  # type: ignore[override]
        assert (  # nosec B101
            not self.calls
        ), "You can't use iterator if subscriber has registered handlers."

        if self._fetch_sub is None:
            fetch_sub = self._fetch_sub = await self._connection_state.client.subscribe(
                subject=self.clear_subject,
                queue=self.queue,
                **self.extra_options,
            )
        else:
            fetch_sub = self._fetch_sub

        async for raw_message in fetch_sub.messages:
            context = self._state.get().di_state.context

            msg: NatsMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
            )
            yield msg

    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self._connection_state.client.subscribe(
            subject=self.clear_subject,
            queue=self.queue,
            cb=self.consume,
            **self.extra_options,
        )

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

        self.subscription = await self._connection_state.client.subscribe(
            subject=self.clear_subject,
            queue=self.queue,
            cb=self._put_msg,
            **self.extra_options,
        )
