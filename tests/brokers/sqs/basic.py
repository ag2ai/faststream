from typing import Any

from faststream.sqs.broker.broker import SQSBroker
from faststream.sqs.broker.router import SQSRouter
from faststream.sqs.testing import TestSQSBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class SQSTestcaseConfig(BaseTestcaseConfig):
    # SQS is poll-based (long-polling), so round-trips are slower than the
    # push-based brokers the default 3s timeout was tuned for.
    timeout: float = 15.0

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> SQSBroker:
        return SQSBroker(
            endpoint_url="http://localhost:9324",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            apply_types=apply_types,
            **kwargs,
        )

    def get_router(self, **kwargs: Any) -> SQSRouter:
        return SQSRouter(**kwargs)


class SQSMemoryTestcaseConfig(SQSTestcaseConfig):
    def patch_broker(self, *brokers: SQSBroker, **kwargs: Any) -> TestSQSBroker:
        return TestSQSBroker(*brokers, **kwargs)
