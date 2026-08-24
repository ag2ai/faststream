from abc import abstractmethod
from typing import Any

import pytest

from faststream._internal.utils.path import Address
from faststream.exceptions import IncorrectState, SetupError
from faststream.params import Path

from .basic import BaseTestcaseConfig


class AddressTemplateTestcase(BaseTestcaseConfig):
    """An Address template and a Broker address are two reads, never one field.

    Every broker that supports Address templates answers both under the same
    names, and a Router prefix reaches each of them.
    """

    template = "logs.{level}"
    broker_address: str

    #: A plain address with no Path parameter in it.
    literal = "logs.info"

    #: An address the broker accepts that is not a valid Address template.
    broken_template = "logs.${ENV"

    @abstractmethod
    def get_subscriber_address(self, subscriber: Any) -> Address:
        """Return the Address the Subscriber reads through."""
        raise NotImplementedError

    def read_address(self, subscriber: Any) -> Address:
        """Prepare the Subscriber, then read — the order in which a read answers.

        Preparation is the moment an address is settled, so every assertion here
        takes it first. Skipping it would be reading from a composition the
        Subscriber has not been told is final.
        """
        subscriber.prepare()
        return self.get_subscriber_address(subscriber)

    def declare_subscriber(self, obj: Any, template: str) -> Any:
        args, kwargs = self.get_subscriber_params(template)
        return obj.subscriber(*args, **kwargs)

    def test_template_and_broker_address_are_separate_reads(self) -> None:
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, self.template)

        address = self.read_address(subscriber)
        assert address.template == self.template
        assert address.broker_address == self.broker_address

    def test_router_prefix_reaches_both_reads(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        self.declare_subscriber(router, self.template)
        broker.include_router(router)

        address = self.read_address(broker.subscribers[0])
        assert address.template == f"prefix_{self.template}"
        assert address.broker_address == f"prefix_{self.broker_address}"

    def test_an_early_read_does_not_pin_an_outer_router_prefix(self) -> None:
        """The read a Broker refuses, in place of the one it used to defend against.

        A Router included into another Router composes a longer prefix than its
        endpoints saw when they were declared. Asked in between, a Subscriber
        says the read came too early instead of answering with an address
        derived from a composition that is not final.
        """
        broker = self.get_broker()
        outer = self.get_router(prefix="outer_")
        inner = self.get_router(prefix="inner_")

        subscriber = self.declare_subscriber(inner, self.template)

        # The read under test: taken while only the inner prefix is in scope.
        with pytest.raises(IncorrectState, match="too early"):
            self.get_subscriber_address(subscriber)

        outer.include_router(inner)
        broker.include_router(outer)

        address = self.read_address(broker.subscribers[0])
        assert address.template == f"outer_inner_{self.template}"
        assert address.broker_address == f"outer_inner_{self.broker_address}"

    def test_a_literal_address_is_its_own_broker_address(self) -> None:
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, self.literal)

        address = self.read_address(subscriber)
        assert address.template == self.literal
        assert address.broker_address == self.literal
        assert address.regex is None

    def test_the_compiled_address_is_kept_rather_than_re_derived(self) -> None:
        """A Config value is fixed at Preparation, so one read settles it (ADR-0004)."""
        broker = self.get_broker()

        subscriber = self.declare_subscriber(broker, self.template)

        first = self.read_address(subscriber)
        assert self.read_address(subscriber) is first


class AddressCheckTestcase(AddressTemplateTestcase):
    """What Preparation refuses: an address that cannot deliver what was promised."""

    @pytest.mark.asyncio()
    async def test_a_path_parameter_with_a_capture_group_is_accepted(self) -> None:
        broker = self.get_broker(apply_types=True)

        @self.declare_subscriber(broker, self.template)
        async def handler(msg: Any, level: str = Path()) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()

    @pytest.mark.asyncio()
    async def test_a_path_parameter_without_a_capture_group_fails_at_connect(
        self,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        @self.declare_subscriber(broker, self.literal)
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="level"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_path_parameter_with_a_default_needs_no_capture_group(self) -> None:
        broker = self.get_broker(apply_types=True)

        @self.declare_subscriber(broker, self.literal)
        async def handler(msg: Any, level: str = Path(default="unknown")) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()

    @pytest.mark.asyncio()
    async def test_an_address_that_is_not_a_template_fails_at_connect(self) -> None:
        broker = self.get_broker()

        @self.declare_subscriber(broker, self.broken_template)
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match=r"\$\{ENV"):
            async with self.patch_broker(broker):
                pass
