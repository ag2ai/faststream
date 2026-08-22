import pytest

from faststream.rabbit import RabbitBroker
from tests.asyncapi.base.v2_6_0.address_field import AddressFieldTestcase


@pytest.mark.rabbit()
class TestAddressField(AddressFieldTestcase):
    broker_class = RabbitBroker
