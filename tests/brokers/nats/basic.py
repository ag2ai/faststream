from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.nats import NatsBroker, NatsRouter, TestNatsBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class NatsTestcaseConfig(BaseTestcaseConfig[NatsBroker]):
    supports_cancel_ack_skip: bool = False

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> NatsBroker:
        return NatsBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> NatsRouter:
        return NatsRouter(**kwargs)


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
