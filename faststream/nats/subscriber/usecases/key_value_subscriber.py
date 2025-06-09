from collections.abc import AsyncIterator, Iterable
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Annotated,
    Optional,
    cast,
)

import anyio
from nats.errors import ConnectionClosedError, TimeoutError
from typing_extensions import Doc, override

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream.nats.parser import KvParser
from faststream.nats.subscriber.adapters import UnsubscribeAdapter

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from nats.js.kv import KeyValue

    from faststream._internal.endpoint.publisher import BasePublisherProto
    from faststream.message import StreamMessage
    from faststream.nats.configs import NatsSubscriberConfig
    from faststream.nats.message import NatsKvMessage
    from faststream.nats.schemas import KvWatch


class KeyValueWatchSubscriber(
    TasksMixin,
    LogicSubscriber["KeyValue.Entry"],
):
    subscription: Optional["UnsubscribeAdapter[KeyValue.KeyWatcher]"]
    _fetch_sub: UnsubscribeAdapter["KeyValue.KeyWatcher"] | None

    def __init__(
        self,
        config: "NatsSubscriberConfig",
        /,
        *,
        kv_watch: "KvWatch",
    ) -> None:
        parser = KvParser(pattern=config.subject)
        config.decoder = parser.decode_message
        config.parser = parser.parse_message
        super().__init__(config)

        self.kv_watch = kv_watch

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["NatsKvMessage"]:
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

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

        raw_message = None
        sleep_interval = timeout / 10
        with anyio.move_on_after(timeout):
            while (  # noqa: ASYNC110
                # type: ignore[no-untyped-call]
                raw_message := await fetch_sub.obj.updates(timeout)
            ) is None:
                await anyio.sleep(sleep_interval)

        context = self._outer_config.fd_config.context

        msg: NatsKvMessage = await process_msg(
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["NatsKvMessage"]:  # type: ignore[override]
        assert (  # nosec B101
            not self.calls
        ), "You can't use iterator if subscriber has registered handlers."

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

        while True:
            raw_message = None
            with anyio.move_on_after(timeout):
                while (  # noqa: ASYNC110
                    # type: ignore[no-untyped-call]
                    raw_message := await fetch_sub.obj.updates(timeout)
                ) is None:
                    await anyio.sleep(sleep_interval)

            if raw_message is None:
                continue

            context = self._outer_config.fd_config.context

            msg: NatsKvMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
            )
            yield msg

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

        self.add_task(self.__consume_watch())

    async def __consume_watch(self) -> None:
        assert self.subscription, "You should call `create_subscription` at first."  # nosec B101

        key_watcher = self.subscription.obj

        while self.running:
            with suppress(ConnectionClosedError, TimeoutError):
                message = cast(
                    "KeyValue.Entry | None",
                    # type: ignore[no-untyped-call]
                    await key_watcher.updates(self.kv_watch.timeout),
                )

                if message:
                    await self.consume(message)

    def _make_response_publisher(
        self,
        message: Annotated[
            "StreamMessage[KeyValue.Entry]",
            Doc("Message requiring reply"),
        ],
    ) -> Iterable["BasePublisherProto"]:
        """Create Publisher objects to use it as one of `publishers` in `self.consume` scope."""
        return ()

    def get_log_context(
        self,
        message: Annotated[
            Optional["StreamMessage[KeyValue.Entry]"],
            Doc("Message which we are building context for"),
        ],
    ) -> dict[str, str]:
        """Log context factory using in `self.consume` scope."""
        return self.build_log_context(
            message=message,
            subject=self.subject,
            stream=self.kv_watch.name,
        )
