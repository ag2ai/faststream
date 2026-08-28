import pytest

from tests.brokers.base.address import AddressPublisherDeliveryTestcase

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


@pytest.mark.nats()
class TestAddressDelivery(NatsMemoryTestcaseConfig, AddressPublisherDeliveryTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.nats()
class TestAddressDeliveryReal(NatsTestcaseConfig, AddressPublisherDeliveryTestcase):
    pass
