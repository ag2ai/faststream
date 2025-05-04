from collections.abc import AsyncIterator, Iterable
from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Annotated,
    Optional,
    cast,
)

import anyio
from nats.errors import TimeoutError
from nats.js.api import ConsumerConfig, ObjectInfo
from typing_extensions import Doc, override

from faststream._internal.subscriber.mixins import TasksMixin
from faststream._internal.subscriber.utils import process_msg
from faststream.middlewares import AckPolicy
from faststream.nats.parser import (
    ObjParser,
)
from faststream.nats.subscriber.adapters import (
    UnsubscribeAdapter,
)

from .basic import LogicSubscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from nats.aio.msg import Msg
    from nats.js.object_store import ObjectStore

    from faststream._internal.publisher.proto import BasePublisherProto
    from faststream._internal.types import (
        BrokerMiddleware,
    )
    from faststream.message import StreamMessage
    from faststream.nats.message import NatsObjMessage
    from faststream.nats.schemas import ObjWatch


OBJECT_STORAGE_CONTEXT_KEY = "__object_storage"


class ObjStoreWatchSubscriber(
    TasksMixin,
    LogicSubscriber[ObjectInfo],
):
    subscription: Optional["UnsubscribeAdapter[ObjectStore.ObjectWatcher]"]
    _fetch_sub: Optional[UnsubscribeAdapter["ObjectStore.ObjectWatcher"]]

    def __init__(
        self,
        *,
        subject: str,
        config: "ConsumerConfig",
        obj_watch: "ObjWatch",
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Iterable["BrokerMiddleware[list[Msg]]"],
    ) -> None:
        parser = ObjParser(pattern="")

        self.obj_watch = obj_watch
        self.obj_watch_conn = None

        super().__init__(
            subject=subject,
            config=config,
            extra_options=None,
            ack_policy=AckPolicy.DO_NOTHING,
            no_reply=True,
            default_parser=parser.parse_message,
            default_decoder=parser.decode_message,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5,
    ) -> Optional["NatsObjMessage"]:
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

        if not self._fetch_sub:
            self.bucket = await self._connection_state.os_declarer.create_object_store(
                bucket=self.subject,
                declare=self.obj_watch.declare,
            )

            obj_watch = await self.bucket.watch(
                ignore_deletes=self.obj_watch.ignore_deletes,
                include_history=self.obj_watch.include_history,
                meta_only=self.obj_watch.meta_only,
            )
            fetch_sub = self._fetch_sub = UnsubscribeAdapter[
                "ObjectStore.ObjectWatcher"
            ](obj_watch)
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

        msg: NatsObjMessage = await process_msg(
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["NatsObjMessage"]:  # type: ignore[override]
        assert (  # nosec B101
            not self.calls
        ), "You can't use iterator if subscriber has registered handlers."

        if not self._fetch_sub:
            self.bucket = await self._connection_state.os_declarer.create_object_store(
                bucket=self.subject,
                declare=self.obj_watch.declare,
            )

            obj_watch = await self.bucket.watch(
                ignore_deletes=self.obj_watch.ignore_deletes,
                include_history=self.obj_watch.include_history,
                meta_only=self.obj_watch.meta_only,
            )
            fetch_sub = self._fetch_sub = UnsubscribeAdapter[
                "ObjectStore.ObjectWatcher"
            ](obj_watch)
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

            context = self._state.get().di_state.context

            msg: NatsObjMessage = await process_msg(  # type: ignore[assignment]
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

        self.bucket = await self._connection_state.os_declarer.create_object_store(
            bucket=self.subject,
            declare=self.obj_watch.declare,
        )

        self.add_task(self.__consume_watch())

    async def __consume_watch(self) -> None:
        assert self.bucket, "You should call `create_subscription` at first."  # nosec B101

        # Should be created inside task to avoid nats-py lock
        obj_watch = await self.bucket.watch(
            ignore_deletes=self.obj_watch.ignore_deletes,
            include_history=self.obj_watch.include_history,
            meta_only=self.obj_watch.meta_only,
        )

        self.subscription = UnsubscribeAdapter["ObjectStore.ObjectWatcher"](obj_watch)

        context = self._state.get().di_state.context

        while self.running:
            with suppress(TimeoutError):
                message = cast(
                    "Optional[ObjectInfo]",
                    await obj_watch.updates(self.obj_watch.timeout),  # type: ignore[no-untyped-call]
                )

                if message:
                    with context.scope(OBJECT_STORAGE_CONTEXT_KEY, self.bucket):
                        await self.consume(message)

    def _make_response_publisher(
        self,
        message: Annotated[
            "StreamMessage[ObjectInfo]",
            Doc("Message requiring reply"),
        ],
    ) -> Iterable["BasePublisherProto"]:
        """Create Publisher objects to use it as one of `publishers` in `self.consume` scope."""
        return ()

    def get_log_context(
        self,
        message: Annotated[
            Optional["StreamMessage[ObjectInfo]"],
            Doc("Message which we are building context for"),
        ],
    ) -> dict[str, str]:
        """Log context factory using in `self.consume` scope."""
        return self.build_log_context(
            message=message,
            subject=self.subject,
        )
