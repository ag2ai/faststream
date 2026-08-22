from abc import abstractmethod
from typing import Any

from faststream._internal.utils.path import Address

from .basic import BaseTestcaseConfig


class AddressTemplateTestcase(BaseTestcaseConfig):
    """An Address template and a Broker address are two reads, never one field.

    Every broker that supports Address templates answers both under the same
    names, and a Router prefix reaches each of them.
    """

    template = "logs.{level}"
    broker_address: str

    @abstractmethod
    def get_subscriber_address(self, subscriber: Any) -> Address:
        """Return the Address the Subscriber reads through."""
        raise NotImplementedError

    def declare_subscriber(self, obj: Any, template: str) -> Any:
        args, kwargs = self.get_subscriber_params(template)
        return obj.subscriber(*args, **kwargs)

    def test_template_and_broker_address_are_separate_reads(self) -> None:
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, self.template)

        address = self.get_subscriber_address(subscriber)
        assert address.template == self.template
        assert address.broker_address == self.broker_address

    def test_router_prefix_reaches_both_reads(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        self.declare_subscriber(router, self.template)
        broker.include_router(router)

        address = self.get_subscriber_address(broker.subscribers[0])
        assert address.template == f"prefix_{self.template}"
        assert address.broker_address == f"prefix_{self.broker_address}"

    def test_a_literal_address_is_its_own_broker_address(self) -> None:
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, "logs.info")

        address = self.get_subscriber_address(subscriber)
        assert address.template == "logs.info"
        assert address.broker_address == "logs.info"
        assert address.regex is None
