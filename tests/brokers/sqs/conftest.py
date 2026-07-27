from dataclasses import dataclass

import pytest

from faststream.sqs.broker.router import SQSRouter

from .basic import ELASTICMQ_CONNECTION


@dataclass
class Settings:
    endpoint_url: str = ELASTICMQ_CONNECTION["endpoint_url"]
    region_name: str = ELASTICMQ_CONNECTION["region_name"]
    aws_access_key_id: str = ELASTICMQ_CONNECTION["aws_access_key_id"]
    aws_secret_access_key: str = ELASTICMQ_CONNECTION["aws_secret_access_key"]


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def router() -> SQSRouter:
    return SQSRouter()
