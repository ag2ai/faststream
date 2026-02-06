import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestCustomParser(SqlaTestcaseConfig, CustomParserTestcase):
    async def test_iterator_respect_decoder(self): ...

    async def test_get_one_respect_decoder(self): ...
