from typing import Any

from faststream.rabbit import RabbitBroker, RabbitRouter, TestRabbitBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class RabbitTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RabbitBroker:
        return RabbitBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> RabbitRouter:
        return RabbitRouter(**kwargs)


class RabbitMemoryTestcaseConfig(RabbitTestcaseConfig):
    def patch_broker(self, *brokers: RabbitBroker, **kwargs: Any) -> TestRabbitBroker:
        return TestRabbitBroker(*brokers, **kwargs)
