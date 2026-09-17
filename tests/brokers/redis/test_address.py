import pytest

from tests.brokers.base.address import AddressPublisherDeliveryTestcase

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


@pytest.mark.redis()
class TestAddressDelivery(RedisMemoryTestcaseConfig, AddressPublisherDeliveryTestcase):
    pass


@pytest.mark.connected()
@pytest.mark.redis()
class TestAddressDeliveryReal(RedisTestcaseConfig, AddressPublisherDeliveryTestcase):
    pass
