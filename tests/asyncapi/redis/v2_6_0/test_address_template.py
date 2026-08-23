import pytest

from faststream.redis import RedisBroker
from tests.asyncapi.base.v2_6_0.address_template import AddressTemplateTestcase


@pytest.mark.redis()
class TestAddressTemplate(AddressTemplateTestcase):
    broker_class = RedisBroker
