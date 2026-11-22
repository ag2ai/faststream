import pytest

from faststream.nats import NatsBroker, NatsPublisher, NatsRoute, NatsRouter
from tests.brokers.base.option_surface import (
    assert_every_level_takes_the_options_the_broker_takes,
)


@pytest.mark.nats()
def test_every_level_takes_the_options_the_broker_takes() -> None:
    assert_every_level_takes_the_options_the_broker_takes(
        NatsBroker,
        NatsRouter,
        NatsRoute,
        NatsPublisher,
    )
