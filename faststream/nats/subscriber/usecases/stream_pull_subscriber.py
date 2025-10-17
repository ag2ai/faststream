from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
from nats.errors import ConnectionClosedError, TimeoutError
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.nats.parser import (
    BatchParser,
)

from .basic import DefaultSubscriber
from .stream_basic import StreamSubscriber

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.nats.message import NatsMessage
    from faststream.nats.schemas import JStream, PullSub
    from faststream.nats.subscriber.config import NatsSubscriberConfig


class PullStreamSubscriber(
    TasksMixin,
    StreamSubscriber,
):
    subscription: Optional["JetStreamContext.PullSubscription"]

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Msg]",
        *,
        queue: str,
        pull_sub: "PullSub",
        stream: "JStream",
    ) -> None:
        super().__init__(
            config,
            specification,
            calls,
            # basic args
            queue=queue,
            stream=stream,
        )

        self.pull_sub = pull_sub

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self.jetstream.pull_subscribe(
            subject=self.clear_subject,
            config=self.config,
            **self.extra_options,
        )
        self.add_task(self._consume_pull, func_kwargs={"cb": self.consume})

    async def _consume_pull(
        self,
        cb: Callable[["Msg"], Awaitable["SendableMessage"]],
    ) -> None:
        """Endless task consuming messages using NATS Pull subscriber."""
        assert self.subscription

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

        self.subscription = await self.jetstream.pull_subscribe(
            subject=self.clear_subject,
            config=self.config,
            **self.extra_options,
        )
        self.add_task(self._consume_pull, func_kwargs={"cb": self._put_msg})


class BatchPullStreamSubscriber(
    TasksMixin,
    DefaultSubscriber[list["Msg"]],
):
    """Batch-message consumer class."""

    subscription: Optional["JetStreamContext.PullSubscription"]
    _fetch_sub: Optional["JetStreamContext.PullSubscription"]

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[list[Msg]]",
        *,
        stream: "JStream",
        pull_sub: "PullSub",
    ) -> None:
        parser = BatchParser(pattern=config.subject)
        config.decoder = parser.decode_batch
        config.parser = parser.parse_batch
        super().__init__(config, specification, calls)

        self.stream = stream
        self.pull_sub = pull_sub

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["NatsMessage"]:
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if not self._fetch_sub:
            fetch_sub = self._fetch_sub = await self.jetstream.pull_subscribe(
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

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        return cast(
            "NatsMessage",
            await process_msg(
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            ),
        )

    @override
    async def __aiter__(self) -> AsyncIterator["NatsMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        if not self._fetch_sub:
            fetch_sub = self._fetch_sub = await self.jetstream.pull_subscribe(
                subject=self.clear_subject,
                config=self.config,
                **self.extra_options,
            )
        else:
            fetch_sub = self._fetch_sub

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            raw_message = await fetch_sub.fetch(batch=1)

            yield cast(
                "NatsMessage",
                await process_msg(
                    msg=raw_message,
                    middlewares=(
                        m(raw_message, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                ),
            )

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self.jetstream.pull_subscribe(
            subject=self.clear_subject,
            config=self.config,
            **self.extra_options,
        )
        self.add_task(self._consume_pull)

    async def _consume_pull(self) -> None:
        """Endless task consuming messages using NATS Pull subscriber."""
        assert self.subscription, "You should call `create_subscription` at first."

        while self.running:  # pragma: no branch
            with suppress(TimeoutError, ConnectionClosedError):
                messages = await self.subscription.fetch(
                    batch=self.pull_sub.batch_size,
                    timeout=self.pull_sub.timeout,
                )

                if messages:
                    await self.consume(messages)
