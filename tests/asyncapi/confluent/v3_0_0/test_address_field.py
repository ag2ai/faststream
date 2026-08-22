import pytest

from faststream.confluent import KafkaBroker
from tests.asyncapi.base.v3_0_0.address_field import AddressFieldTestcase


@pytest.mark.confluent()
class TestAddressField(AddressFieldTestcase):
    broker_class = KafkaBroker
