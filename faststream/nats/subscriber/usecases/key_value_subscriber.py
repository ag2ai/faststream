from collections.abc import AsyncIterator, Iterable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional, cast

import anyio
from nats.errors import ConnectionClosedError, TimeoutError
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.nats.parser import KvParser
from faststream.nats.subscriber.adapters import UnsubscribeAdapter

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from nats.js.kv import KeyValue

    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.nats.message import NatsKvMessage
    from faststream.nats.schemas import KvWatch
    from faststream.nats.subscriber.config import NatsSubscriberConfig


class KeyValueWatchSubscriber(
    TasksMixin,
    LogicSubscriber["KeyValue.Entry"],
):
    subscription: Optional["UnsubscribeAdapter[KeyValue.KeyWatcher]"]
    _fetch_sub: UnsubscribeAdapter["KeyValue.KeyWatcher"] | None

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[KeyValue.Entry]",
        *,
        kv_watch: "KvWatch",
    ) -> None:
        parser = KvParser(pattern=config.subject)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config, specification, calls)

        self.kv_watch = kv_watch

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["NatsKvMessage"]:
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if not self._fetch_sub:
            bucket = await self._outer_config.kv_declarer.create_key_value(
                bucket=self.kv_watch.name,
                declare=self.kv_watch.declare,
            )

            fetch_sub = self._fetch_sub = UnsubscribeAdapter["KeyValue.KeyWatcher"](
                await bucket.watch(
                    keys=self.clear_subject,
                    headers_only=self.kv_watch.headers_only,
                    include_history=self.kv_watch.include_history,
                    ignore_deletes=self.kv_watch.ignore_deletes,
                    meta_only=self.kv_watch.meta_only,
                ),
            )
        else:
            fetch_sub = self._fetch_sub

        msg = None
        sleep_interval = timeout / 10
        with anyio.move_on_after(timeout):
            while (  # noqa: ASYNC110
                msg := await fetch_sub.obj.updates(timeout)  # type: ignore[no-untyped-call]
            ) is None:
                await anyio.sleep(sleep_interval)

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        return cast(
            "NatsKvMessage",
            await process_msg(
                msg=msg,
                middlewares=(m(msg, context=context) for m in self._broker_middlewares),
                parser=async_parser,
                decoder=async_decoder,
            ),
        )

    @override
    async def __aiter__(self) -> AsyncIterator["NatsKvMessage"]:  # type: ignore[override]
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        if not self._fetch_sub:
            bucket = await self._outer_config.kv_declarer.create_key_value(
                bucket=self.kv_watch.name,
                declare=self.kv_watch.declare,
            )

            fetch_sub = self._fetch_sub = UnsubscribeAdapter["KeyValue.KeyWatcher"](
                await bucket.watch(
                    keys=self.clear_subject,
                    headers_only=self.kv_watch.headers_only,
                    include_history=self.kv_watch.include_history,
                    ignore_deletes=self.kv_watch.ignore_deletes,
                    meta_only=self.kv_watch.meta_only,
                ),
            )
        else:
            fetch_sub = self._fetch_sub

        timeout = 5
        sleep_interval = timeout / 10

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        while True:
            msg = None
            with anyio.move_on_after(timeout):
                while (  # noqa: ASYNC110
                    msg := await fetch_sub.obj.updates(timeout)  # type: ignore[no-untyped-call]
                ) is None:
                    await anyio.sleep(sleep_interval)

            if msg is None:
                continue

            yield cast(
                "NatsKvMessage",
                await process_msg(
                    msg=msg,
                    middlewares=(
                        m(msg, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                ),
            )

    @override
    async def _create_subscription(self) -> None:
        if self.subscription:
            return

        bucket = await self._outer_config.kv_declarer.create_key_value(
            bucket=self.kv_watch.name,
            declare=self.kv_watch.declare,
        )

        self.subscription = UnsubscribeAdapter["KeyValue.KeyWatcher"](
            await bucket.watch(
                keys=self.clear_subject,
                headers_only=self.kv_watch.headers_only,
                include_history=self.kv_watch.include_history,
                ignore_deletes=self.kv_watch.ignore_deletes,
                meta_only=self.kv_watch.meta_only,
            ),
        )

        self.add_task(self.__consume_watch)

    async def __consume_watch(self) -> None:
        assert self.subscription, "You should call `create_subscription` at first."

        key_watcher = self.subscription.obj

        while self.running:
            with suppress(ConnectionClosedError, TimeoutError):
                message = cast(
                    "KeyValue.Entry | None",
                    await key_watcher.updates(self.kv_watch.timeout),  # type: ignore[no-untyped-call]
                )

                if message:
                    await self.consume(message)

    def _make_response_publisher(
        self,
        message: "StreamMessage[KeyValue.Entry]",
    ) -> Iterable["PublisherProto"]:
        """Create Publisher objects to use it as one of `publishers` in `self.consume` scope.

        Args:
            message: Message requiring reply
        """
        return ()

    def get_log_context(
        self,
        message: Optional["StreamMessage[KeyValue.Entry]"],
    ) -> dict[str, str]:
        """Log context factory using in `self.consume` scope.

        Args:
            message: Message which we are building context for
        """
        return self.build_log_context(
            message=message,
            subject=self.subject,
            stream=self.kv_watch.name,
        )
