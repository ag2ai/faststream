import pytest

from faststream.sqla.broker.router import (
    SqlaPublisher,
    SqlaRoute,
)
from tests.brokers.base.router import RouterTestcase

from .basic import SqlaTestcaseConfig


@pytest.mark.sqla()
@pytest.mark.connected()
@pytest.mark.slow()
class TestRouter(SqlaTestcaseConfig, RouterTestcase):
    route_class = SqlaRoute
    publisher_class = SqlaPublisher

    async def test_get_one_respect_decoder(self) -> None:
        ...

    async def test_iterator_respect_decoder(self) -> None:
        ...
