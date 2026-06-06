import os
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
        # LocalStack defaults; override via env for real AWS / other emulators.
        return SQSBroker(
            endpoint_url=os.environ.get("SQS_ENDPOINT_URL", "http://localhost:4566"),
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            apply_types=apply_types,
            **kwargs,
        )

    def get_router(self, **kwargs: Any) -> SQSRouter:
        return SQSRouter(**kwargs)


class SQSMemoryTestcaseConfig(SQSTestcaseConfig):
    def patch_broker(self, *brokers: SQSBroker, **kwargs: Any) -> TestSQSBroker:
        return TestSQSBroker(*brokers, **kwargs)
