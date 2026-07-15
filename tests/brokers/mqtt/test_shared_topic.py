import pytest

from faststream.mqtt import MQTTRouter
from tests.brokers.mqtt.basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
class TestSharedTopicOrder(MQTTMemoryTestcaseConfig):
    def test_shared_topic(self) -> None:
        broker = self.get_broker()

        sub = broker.subscriber("sub", shared="shared")

        assert sub.topic == "$share/shared/sub"

    def test_router_supports_shared_topic(self) -> None:
        broker = self.get_broker()
        router = MQTTRouter(prefix="router")

        sub = router.subscriber("/sub", shared="shared")
        broker.include_router(router)

        assert sub.topic == "$share/shared/router/sub"

    def test_no_shared_router_topic_name(self) -> None:
        broker = self.get_broker()
        router = MQTTRouter(prefix="router")

        sub = router.subscriber("/sub")
        broker.include_router(router)

        assert sub.topic == "router/sub"
