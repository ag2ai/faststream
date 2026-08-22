import pytest

from faststream.nats import NatsBroker
from tests.asyncapi.base.v3_0_0.address_field import AddressFieldTestcase


@pytest.mark.nats()
class TestAddressField(AddressFieldTestcase):
    broker_class = NatsBroker
