import pytest

from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0.address_template import AddressTemplateTestcase


@pytest.mark.nats()
class TestAddressTemplate(AddressTemplateTestcase):
    broker_class = NatsBroker
