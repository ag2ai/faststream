import pytest

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.config import ConfigTestcase


@pytest.mark.kafka()
class TestConfigValues(ConfigTestcase):
    broker_class = KafkaBroker
