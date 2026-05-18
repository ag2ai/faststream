from typing import Any

from faststream.nats import NatsBroker, NatsRouter, TestNatsBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class NatsTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> NatsBroker:
        return NatsBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> NatsRouter:
        return NatsRouter(**kwargs)


class NatsMemoryTestcaseConfig(NatsTestcaseConfig):
    def patch_broker(self, *brokers: NatsBroker, **kwargs: Any) -> TestNatsBroker:
        return TestNatsBroker(*brokers, **kwargs)
