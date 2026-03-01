import pytest

from faststream.kafka import KafkaBroker, KafkaRoute, KafkaRouter


@pytest.mark.kafka()
class TestGroupInstanceId:
    def test_subscriber_passes_group_instance_id(self) -> None:
        broker = KafkaBroker()

        sub = broker.subscriber(
            "test-topic",
            group_id="test-group",
            group_instance_id="instance-1",
        )

        assert sub._connection_args["group_instance_id"] == "instance-1"

    def test_subscriber_group_instance_id_default_none(self) -> None:
        broker = KafkaBroker()

        sub = broker.subscriber(
            "test-topic",
            group_id="test-group",
        )

        assert sub._connection_args["group_instance_id"] is None

    def test_router_passes_group_instance_id(self) -> None:
        router = KafkaRouter()

        sub = router.subscriber(
            "test-topic",
            group_id="test-group",
            group_instance_id="instance-2",
        )

        assert sub._connection_args["group_instance_id"] == "instance-2"

    def test_route_accepts_group_instance_id(self) -> None:
        async def handler(msg: str) -> None: ...

        route = KafkaRoute(
            handler,
            "test-topic",
            group_id="test-group",
            group_instance_id="instance-3",
        )

        assert route.kwargs["group_instance_id"] == "instance-3"
