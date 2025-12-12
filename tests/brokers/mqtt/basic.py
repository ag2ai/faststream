from typing import Any

from faststream.mqtt import MQTTBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class MQTTTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> MQTTBroker:
        return MQTTBroker(apply_types=apply_types, **kwargs)

    def patch_broker(self, broker: MQTTBroker, **kwargs: Any) -> MQTTBroker:
        return broker

    # TODO
    # def get_router(self, **kwargs: Any) -> KafkaRouter:
    #     return MQTTRouter(**kwargs)


# class KafkaMemoryTestcaseConfig(KafkaTestcaseConfig):
#     def patch_broker(self, broker: KafkaBroker, **kwargs: Any) -> KafkaBroker:
#         return TestKafkaBroker(broker, **kwargs)
