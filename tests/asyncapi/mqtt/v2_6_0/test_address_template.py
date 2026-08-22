import pytest

from faststream.mqtt import MQTTBroker
from tests.asyncapi.base.v2_6_0.address_template import AddressTemplateTestcase


@pytest.mark.mqtt()
class TestAddressTemplate(AddressTemplateTestcase):
    """MQTT never compiles its topic, so the template is what it already renders.

    Correct, but never decided — this pins it. MQTT has no Address syntax of its
    own, so `logs/+` is what it *would* compile to and nothing can emit it today;
    the leak assertion holds the line if MQTT ever gains one.
    """

    broker_class = MQTTBroker

    address_template = "logs/{level}"
    broker_address = "logs/+"
