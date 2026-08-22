from tests.asyncapi.base.address_field import (
    AddressFieldTestcase as BaseAddressFieldTestcase,
)

from .basic import AsyncAPI300Factory


class AddressFieldTestcase(BaseAddressFieldTestcase, AsyncAPI300Factory):
    def test_subscriber_address_is_the_address(self) -> None:
        key, channel = self.subscriber_channel()

        assert channel["address"] == self.rendered_address
        # and the key goes on naming the channel, handler name and all
        assert key != channel["address"]

    def test_publisher_address_is_the_address(self) -> None:
        key, channel = self.publisher_channel()

        assert channel["address"] == self.rendered_address
        assert key != channel["address"]
