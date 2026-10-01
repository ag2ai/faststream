import pytest

from faststream.confluent import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
from tests.brokers.base.option_surface import (
    assert_every_level_takes_the_options_the_broker_takes,
)


@pytest.mark.confluent()
def test_every_level_takes_the_options_the_broker_takes() -> None:
    assert_every_level_takes_the_options_the_broker_takes(
        KafkaBroker,
        KafkaRouter,
        KafkaRoute,
        KafkaPublisher,
    )
