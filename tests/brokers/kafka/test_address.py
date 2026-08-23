from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, TestKafkaBroker
from faststream.params import Path
from tests.brokers.base.address import AddressCheckTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
class TestKafkaAddressTemplate(KafkaMemoryTestcaseConfig, AddressCheckTestcase):
    broker_address = "logs..*"

    @override
    def declare_subscriber(self, obj: Any, template: str) -> Any:
        return obj.subscriber(pattern=template)

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.pattern

    def test_a_topic_subscriber_has_no_pattern(self) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber("topic")
        subscriber.prepare()

        assert subscriber.pattern is None

    @pytest.mark.asyncio()
    async def test_a_path_parameter_must_be_captured_by_every_topic(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(f"{queue}-a", f"{queue}-b")
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match=f"{queue}-a"):
            async with self.patch_broker(broker):
                pass


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_a_topic_is_never_read_as_a_template() -> None:
    """A brace in a topic is a literal, so it cannot fill a `Path()`.

    Only `pattern=` compiles; `topics` hands the string to Kafka verbatim. Reading
    a topic as a template would accept this subscriber at Preparation and leave
    `level` unfillable for every message it received.
    """
    broker = KafkaBroker(apply_types=True)

    @broker.subscriber("logs.{level}")
    async def handler(msg: Any, level: str = Path()) -> None: ...

    with pytest.raises(SetupError, match="level"):
        async with TestKafkaBroker(broker):
            pass
