import os
from dataclasses import dataclass

import pytest

from faststream.sqs.broker.router import SQSRouter


@dataclass
class Settings:
    # LocalStack defaults; override via env for real AWS / other emulators.
    endpoint_url: str = os.environ.get("SQS_ENDPOINT_URL", "http://localhost:4566")
    region_name: str = os.environ.get("AWS_REGION", "us-east-1")
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def router() -> SQSRouter:
    return SQSRouter()
