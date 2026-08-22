import pytest

from faststream.kafka import KafkaBroker, KafkaRouter


@pytest.mark.kafka()
def test_pattern_keeps_both_the_template_and_the_broker_address() -> None:
    broker = KafkaBroker()

    @broker.subscriber(pattern="logs.{level}")
    async def handler(msg: str) -> None: ...

    subscriber = broker.subscribers[0]
    assert subscriber.pattern_template == "logs.{level}"
    assert subscriber.broker_pattern == "logs..*"


@pytest.mark.kafka()
def test_router_prefix_reaches_both_reads() -> None:
    broker = KafkaBroker()
    router = KafkaRouter(prefix="prefix_")

    @router.subscriber(pattern="logs.{level}")
    async def handler(msg: str) -> None: ...

    broker.include_router(router)

    subscriber = broker.subscribers[0]
    assert subscriber.pattern_template == "prefix_logs.{level}"
    assert subscriber.broker_pattern == "prefix_logs..*"


@pytest.mark.kafka()
def test_topic_subscriber_has_no_pattern() -> None:
    broker = KafkaBroker()

    @broker.subscriber("topic")
    async def handler(msg: str) -> None: ...

    subscriber = broker.subscribers[0]
    assert subscriber.pattern_template is None
    assert subscriber.broker_pattern is None
