from typing import Any

from faststream.kafka import KafkaBroker, KafkaRouter, TestKafkaBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class KafkaTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> KafkaBroker:
        return KafkaBroker(apply_types=apply_types, **kwargs)

    def patch_broker(self, broker: KafkaBroker, **kwargs: Any) -> KafkaBroker:
        return broker

    def get_router(self, **kwargs: Any) -> KafkaRouter:
        return KafkaRouter(**kwargs)


class KafkaMemoryTestcaseConfig(KafkaTestcaseConfig):
    def patch_broker(self, broker: KafkaBroker, **kwargs: Any) -> KafkaBroker:
        return TestKafkaBroker(broker, **kwargs)
