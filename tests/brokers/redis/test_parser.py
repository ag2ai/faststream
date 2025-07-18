import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
class TestCustomParser(RedisTestcaseConfig, CustomParserTestcase):
    pass
