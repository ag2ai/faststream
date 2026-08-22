import pytest

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v2_6_0.address_field import AddressFieldTestcase


@pytest.mark.kafka()
class TestAddressField(AddressFieldTestcase):
    broker_class = KafkaBroker
