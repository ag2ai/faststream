from typing import TYPE_CHECKING, Optional

from nats.aio.msg import Msg
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin

from .stream_basic import StreamSubscriber

if TYPE_CHECKING:
    from nats.js import JetStreamContext


class PushStreamSubscriber(StreamSubscriber):
    subscription: Optional["JetStreamContext.PushSubscription"]

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self.jetstream.subscribe(
            subject=self.clear_subject,
            queue=self.queue,
            cb=self.consume,
            config=self.config,
            **self.extra_options,
        )


class ConcurrentPushStreamSubscriber(ConcurrentMixin[Msg], StreamSubscriber):
    subscription: Optional["JetStreamContext.PushSubscription"]

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.start_consume_task()

        self.subscription = await self.jetstream.subscribe(
            subject=self.clear_subject,
            queue=self.queue,
            cb=self._put_msg,
            config=self.config,
            **self.extra_options,
        )
