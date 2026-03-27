from typing import Any

from faststream.mq import MQBroker, MQRouter, TestMQBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class MQTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> MQBroker:
        return MQBroker(
            queue_manager="QM1",
            channel="DEV.APP.SVRCONN",
            conn_name="127.0.0.1(1414)",
            apply_types=apply_types,
            **kwargs,
        )

    def patch_broker(self, broker: MQBroker, **kwargs: Any) -> MQBroker:
        return broker

    def get_router(self, **kwargs: Any) -> MQRouter:
        return MQRouter(**kwargs)


class MQMemoryTestcaseConfig(MQTestcaseConfig):
    def patch_broker(self, broker: MQBroker, **kwargs: Any) -> MQBroker:
        return TestMQBroker(broker, **kwargs)
