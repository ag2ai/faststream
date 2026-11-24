from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.nats import JStream, NatsBroker, NatsRouter, TestNatsBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class NatsTestcaseConfig(BaseTestcaseConfig[NatsBroker]):
    supports_cancel_ack_skip: bool = False
    # Ack params use an ephemeral JetStream consumer, so a restart replays acked messages.
    redelivers_to_new_consumer: bool = False

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> NatsBroker:
        return NatsBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> NatsRouter:
        return NatsRouter(**kwargs)

    @override
    def get_ack_subscriber_params(
        self,
        queue: str,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        # Core subjects force MANUAL ack; JetStream carries the acknowledgement.
        return (queue,), {"stream": JStream(queue)}

    @override
    async def publish_to_ack_subscriber(
        self,
        broker: Any,
        queue: str,
        message: str,
    ) -> None:
        await broker.publish(message, queue, stream=queue)


class NatsMemoryTestcaseConfig(NatsTestcaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: NatsBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[NatsBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: NatsBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[NatsBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: NatsBroker,
        **kwargs: Any,
    ) -> Any:
        return TestNatsBroker(*brokers, **kwargs)
