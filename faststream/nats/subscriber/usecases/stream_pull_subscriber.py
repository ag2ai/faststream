from collections.abc import AsyncIterator, Awaitable, Iterable
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
    cast,
)

import anyio
from nats.errors import ConnectionClosedError, TimeoutError
from typing_extensions import override

from faststream._internal.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.subscriber.utils import process_msg
from faststream.nats.parser import (
    BatchParser,
)

from .basic import DefaultSubscriber
from .stream_basic import StreamSubscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext
    from nats.js.api import ConsumerConfig

    from faststream._internal.basic_types import (
        AnyDict,
        SendableMessage,
    )
    from faststream._internal.types import (
        BrokerMiddleware,
    )
    from faststream.middlewares import AckPolicy
    from faststream.nats.message import NatsMessage
    from faststream.nats.schemas import JStream, PullSub


class PullStreamSubscriber(
    TasksMixin,
    StreamSubscriber,
):
    subscription: Optional["JetStreamContext.PullSubscription"]

    def __init__(
        self,
        *,
        pull_sub: "PullSub",
        stream: "JStream",
        # default args
        subject: str,
        config: "ConsumerConfig",
        extra_options: Optional["AnyDict"],
        # Subscriber args
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[Msg]"],
    ) -> None:
        self.pull_sub = pull_sub

        super().__init__(
            # basic args
            stream=stream,
            subject=subject,
            config=config,
            extra_options=extra_options,
            queue="",
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self._connection_state.js.pull_subscribe(
            subject=self.clear_subject,
            config=self.config,
            **self.extra_options,
        )
        self.add_task(self._consume_pull(cb=self.consume))

    async def _consume_pull(
        self,
        cb: Callable[["Msg"], Awaitable["SendableMessage"]],
    ) -> None:
        """Endless task consuming messages using NATS Pull subscriber."""
        assert self.subscription  # nosec B101

        while self.running:  # pragma: no branch
            messages = []
            with suppress(TimeoutError, ConnectionClosedError):
                messages = await self.subscription.fetch(
                    batch=self.pull_sub.batch_size,
                    timeout=self.pull_sub.timeout,
                )

            if messages:
                async with anyio.create_task_group() as tg:
                    for msg in messages:
                        tg.start_soon(cb, msg)


class ConcurrentPullStreamSubscriber(ConcurrentMixin["Msg"], PullStreamSubscriber):
    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.start_consume_task()

        self.subscription = await self._connection_state.js.pull_subscribe(
            subject=self.clear_subject,
            config=self.config,
            **self.extra_options,
        )
        self.add_task(self._consume_pull(cb=self._put_msg))


class BatchPullStreamSubscriber(
    TasksMixin,
    DefaultSubscriber[list["Msg"]],
):
    """Batch-message consumer class."""

    subscription: Optional["JetStreamContext.PullSubscription"]
    _fetch_sub: Optional["JetStreamContext.PullSubscription"]

    def __init__(
        self,
        *,
        # default args
        subject: str,
        config: "ConsumerConfig",
        stream: "JStream",
        pull_sub: "PullSub",
        extra_options: Optional["AnyDict"],
        # Subscriber args
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[list[Msg]]"],
    ) -> None:
        parser = BatchParser(pattern=subject)

        self.stream = stream
        self.pull_sub = pull_sub

        super().__init__(
            subject=subject,
            config=config,
            extra_options=extra_options,
            # subscriber args
            default_parser=parser.parse_batch,
            default_decoder=parser.decode_batch,
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
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
            fetch_sub = (
                self._fetch_sub
            ) = await self._connection_state.js.pull_subscribe(
                subject=self.clear_subject,
                config=self.config,
                **self.extra_options,
            )
        else:
            fetch_sub = self._fetch_sub

        try:
            raw_message = await fetch_sub.fetch(
                batch=1,
                timeout=timeout,
            )
        except TimeoutError:
            return None

        context = self._state.get().di_state.context

        return cast(
            "NatsMessage",
            await process_msg(
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
            ),
        )

    @override
    async def __aiter__(self) -> AsyncIterator["NatsMessage"]:  # type: ignore[override]
        assert (  # nosec B101
            not self.calls
        ), "You can't use iterator if subscriber has registered handlers."

        if not self._fetch_sub:
            fetch_sub = (
                self._fetch_sub
            ) = await self._connection_state.js.pull_subscribe(
                subject=self.clear_subject,
                config=self.config,
                **self.extra_options,
            )
        else:
            fetch_sub = self._fetch_sub

        while True:
            raw_message = await fetch_sub.fetch(batch=1)

            context = self._state.get().di_state.context

            yield cast(
                "NatsMessage",
                await process_msg(
                    msg=raw_message,
                    middlewares=(
                        m(raw_message, context=context) for m in self._broker_middlewares
                    ),
                    parser=self._parser,
                    decoder=self._decoder,
                ),
            )

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self._connection_state.js.pull_subscribe(
            subject=self.clear_subject,
            config=self.config,
            **self.extra_options,
        )
        self.add_task(self._consume_pull())

    async def _consume_pull(self) -> None:
        """Endless task consuming messages using NATS Pull subscriber."""
        assert self.subscription, "You should call `create_subscription` at first."  # nosec B101

        while self.running:  # pragma: no branch
            with suppress(TimeoutError, ConnectionClosedError):
                messages = await self.subscription.fetch(
                    batch=self.pull_sub.batch_size,
                    timeout=self.pull_sub.timeout,
                )

                if messages:
                    await self.consume(messages)
