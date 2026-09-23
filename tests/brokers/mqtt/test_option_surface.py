import pytest

from faststream.mqtt import MQTTBroker, MQTTPublisher, MQTTRoute, MQTTRouter
from tests.brokers.base.option_surface import (
    assert_every_level_takes_the_options_the_broker_takes,
)


@pytest.mark.mqtt()
def test_every_level_takes_the_options_the_broker_takes() -> None:
    assert_every_level_takes_the_options_the_broker_takes(
        MQTTBroker,
        MQTTRouter,
        MQTTRoute,
        MQTTPublisher,
    )
