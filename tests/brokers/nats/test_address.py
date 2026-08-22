from typing import Any

import pytest
from nats.js.api import ConsumerConfig
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.exceptions import SetupError
from faststream.nats import JStream
from faststream.params import Path
from tests.brokers.base.address import AddressCheckTestcase

from .basic import NatsMemoryTestcaseConfig


@pytest.mark.nats()
class TestNatsAddressTemplate(NatsMemoryTestcaseConfig, AddressCheckTestcase):
    broker_address = "logs.*"

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.subject

    def test_publisher_reads_through_the_same_address(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        publisher = router.publisher("out.{id}")
        broker.include_router(router)

        assert publisher.subject.template == "prefix_out.{id}"
        assert publisher.subject.broker_address == "prefix_out.*"

    @pytest.mark.asyncio()
    async def test_a_path_parameter_must_be_captured_by_every_filter_subject(
        self,
        queue: str,
    ) -> None:
        """One address short of the promise is a runtime failure on part of the traffic."""
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(
            config=ConsumerConfig(
                filter_subjects=[f"{queue}.{{level}}.a", f"{queue}.b"],
            ),
            stream=JStream(queue, subjects=[f"{queue}.>"]),
        )
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match=f"{queue}.b"):
            async with self.patch_broker(broker):
                pass
