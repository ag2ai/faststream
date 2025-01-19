from collections.abc import Iterable
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

from faststream._internal.subscriber.mixins import TasksMixin
from faststream._internal.subscriber.utils import process_msg
from faststream.nats.parser import (
    KvParser,
)
from faststream.nats.schemas.subscribers import NatsLogicSubscriberOptions
from faststream.nats.subscriber.adapters import (
    UnsubscribeAdapter,
)

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from nats.js.kv import KeyValue

    from faststream._internal.publisher.proto import BasePublisherProto
    from faststream.message import StreamMessage
    from faststream.nats.message import NatsKvMessage
    from faststream.nats.schemas import KvWatch


class KeyValueWatchSubscriber(
    TasksMixin,
    LogicSubscriber["KeyValue.Entry"],
):
    subscription: Optional["UnsubscribeAdapter[KeyValue.KeyWatcher]"]
    _fetch_sub: Optional[UnsubscribeAdapter["KeyValue.KeyWatcher"]]

    def __init__(
        self, *, kv_watch: "KvWatch", key_value_options: NatsLogicSubscriberOptions
    ) -> None:
        parser = KvParser(pattern=key_value_options.subject)
        self.kv_watch = kv_watch
        key_value_options.internal_options.default_decoder = parser.decode_message
        key_value_options.internal_options.default_parser = parser.parse_message
        super().__init__(logic_options=key_value_options.internal_options)

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
            bucket = await self._connection_state.kv_declarer.create_key_value(
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

        context = self._state.get().di_state.context

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
    async def _create_subscription(self) -> None:
        if self.subscription:
            return

        bucket = await self._connection_state.kv_declarer.create_key_value(
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
                    "Optional[KeyValue.Entry]",
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
