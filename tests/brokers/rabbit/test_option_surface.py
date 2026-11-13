import pytest

from faststream.rabbit import RabbitBroker, RabbitPublisher, RabbitRoute, RabbitRouter
from tests.brokers.base.option_surface import (
    assert_every_level_takes_the_options_the_broker_takes,
)


@pytest.mark.rabbit()
def test_every_level_takes_the_options_the_broker_takes() -> None:
    assert_every_level_takes_the_options_the_broker_takes(
        RabbitBroker,
        RabbitRouter,
        RabbitRoute,
        RabbitPublisher,
    )
