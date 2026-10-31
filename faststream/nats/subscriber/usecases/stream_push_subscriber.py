from typing import TYPE_CHECKING, Optional

from typing_extensions import override

from .stream_basic import StreamSubscriber

if TYPE_CHECKING:
    from nats.js import JetStreamContext


class PushStreamSubscriber(StreamSubscriber):
    __slots__ = ()

    subscription: Optional["JetStreamContext.PushSubscription"]

    @override
    async def _create_subscription(self) -> None:
        """Create NATS subscription and start consume task."""
        if self.subscription:
            return

        self.subscription = await self.jetstream.subscribe(
            subject=self.subject.broker_address,
            queue=self.queue,
            cb=self.consume_dispatcher.dispatch,
            config=self.config,
            **self.extra_options,
        )
