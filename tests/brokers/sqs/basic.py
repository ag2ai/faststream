from typing import Any

from faststream.sqs.broker.broker import SQSBroker
from faststream.sqs.broker.router import SQSRouter
from faststream.sqs.testing import TestSQSBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class SQSTestcaseConfig(BaseTestcaseConfig):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> SQSBroker:
        return SQSBroker(region_name="us-east-1", apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> SQSRouter:
        return SQSRouter(**kwargs)


class SQSMemoryTestcaseConfig(SQSTestcaseConfig):
    def patch_broker(self, *brokers: SQSBroker, **kwargs: Any) -> TestSQSBroker:
        return TestSQSBroker(*brokers, **kwargs)
