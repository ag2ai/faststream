import pytest

from faststream.redis import RedisBroker
from tests.asyncapi.base.v3_0_0.address_field import AddressFieldTestcase


@pytest.mark.redis()
class TestAddressField(AddressFieldTestcase):
    broker_class = RedisBroker
