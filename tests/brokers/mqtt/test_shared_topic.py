from typing import Any

import pytest

from faststream.exceptions import IncorrectState
from faststream.mqtt import MQTTRouter
from tests.brokers.mqtt.basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
class TestSharedTopicOrder(MQTTMemoryTestcaseConfig):
    def test_shared_topic(self) -> None:
        broker = self.get_broker()

        sub = broker.subscriber("sub", shared="shared")
        broker.prepare()

        assert sub.topic == "$share/shared/sub"

    def test_router_supports_shared_topic(self) -> None:
        broker = self.get_broker()
        router = MQTTRouter(prefix="router")

        sub = router.subscriber("/sub", shared="shared")
        broker.include_router(router)
        broker.prepare()

        assert sub.topic == "$share/shared/router/sub"

    def test_no_shared_router_topic_name(self) -> None:
        broker = self.get_broker()
        router = MQTTRouter(prefix="router")

        sub = router.subscriber("/sub")
        broker.include_router(router)
        broker.prepare()

        assert sub.topic == "router/sub"

    def test_reading_a_shared_group_before_preparation_refuses(self) -> None:
        """The group is a resolved read, so it waits like the topic does.

        Here rather than beside the shared rule in `tests/lifecycle`: that rule
        is identical for all six Brokers and asserted once on Kafka, while a
        shared-subscription group is MQTT's alone. It can arrive from a Config
        value, which is what puts it on the same terms as an address — and the
        topic that carries one cannot answer before it does.
        """
        broker = self.get_broker()

        sub = broker.subscriber("sub", shared="shared")

        with pytest.raises(IncorrectState, match="too early"):
            _ = sub.shared

        with pytest.raises(IncorrectState, match="too early"):
            _ = sub.topic

    @pytest.mark.asyncio()
    async def test_after_connect_both_reads_answer(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, shared="group")
        async def handler(msg: Any) -> None: ...

        (sub,) = broker.subscribers

        async with self.patch_broker(broker):
            assert sub.shared == "group"
            assert sub.topic == f"$share/group/{queue}"
