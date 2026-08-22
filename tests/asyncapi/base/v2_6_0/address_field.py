from tests.asyncapi.base.address_field import (
    AddressFieldTestcase as BaseAddressFieldTestcase,
)

from .basic import AsyncAPI260Factory


class AddressFieldTestcase(BaseAddressFieldTestcase, AsyncAPI260Factory):
    """2.6 gains no address, and this is what says so.

    There is nowhere to put one here: the channel key *is* the name in 2.6, and
    handing it the address instead would collide two subscribers listening on one
    address. So the field stays absent — and because the channel model allows
    extras, an `address` leaking in would be accepted rather than rejected, which
    is the whole reason to assert on it.
    """

    def test_subscriber_has_no_address_field(self) -> None:
        _, channel = self.subscriber_channel()

        assert "address" not in channel, channel

    def test_publisher_has_no_address_field(self) -> None:
        _, channel = self.publisher_channel()

        assert "address" not in channel, channel

    def test_two_subscribers_on_one_address_keep_two_channels(self) -> None:
        """The collision the missing field exists to avoid.

        Two handlers listening on one address are two channels here, told apart by
        the handler name the key carries. Hand the key the address instead and they
        become one, which is why 2.6 keeps naming channels the way it does.

        Declared plainly rather than through `declare_subscriber`: the question is
        what the keys do, not how any one broker spells an address.
        """
        broker = self.broker_class()

        @broker.subscriber(self.address)
        async def first(msg) -> None: ...

        @broker.subscriber(self.address)
        async def second(msg) -> None: ...

        channels = self.get_spec(broker).to_jsonable()["channels"]

        assert len(channels) == 2, channels
