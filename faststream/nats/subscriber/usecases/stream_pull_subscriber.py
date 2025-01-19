from collections.abc import Awaitable
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
from faststream.nats.schemas.subscribers import NatsLogicSubscriberOptions

from .basic import DefaultSubscriber
from .stream_basic import StreamSubscriber

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

    from faststream._internal.basic_types import (
        SendableMessage,
    )
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
        pull_stream_options: NatsLogicSubscriberOptions,
    ) -> None:
        self.pull_sub = pull_sub

        super().__init__(
            # basic args
            stream=stream,
            stream_options=pull_stream_options,
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
        stream: "JStream",
        pull_sub: "PullSub",
        pull_stream_options: NatsLogicSubscriberOptions,
    ) -> None:
        parser = BatchParser(pattern=pull_stream_options.subject)

        self.stream = stream
        self.pull_sub = pull_sub
        pull_stream_options.internal_options.default_decoder = parser.decode_batch
        pull_stream_options.internal_options.default_parser = parser.parse_batch
        super().__init__(options=pull_stream_options.internal_options)

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
