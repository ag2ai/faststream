import pytest

from tests.brokers.base.address import AddressPublisherDeliveryTestcase

from .basic import MQTTMemoryTestcaseConfig, MQTTTestcaseConfig


@pytest.mark.mqtt()
class TestAddressDelivery(MQTTMemoryTestcaseConfig, AddressPublisherDeliveryTestcase):
    """MQTT separates topic levels with `/` and captures one level with `+`."""

    separator = "/"


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestAddressDeliveryReal(MQTTTestcaseConfig, AddressPublisherDeliveryTestcase):
    separator = "/"
