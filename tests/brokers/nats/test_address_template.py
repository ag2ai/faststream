import pytest

from faststream.nats import NatsBroker, NatsRouter


@pytest.mark.nats()
def test_subject_keeps_both_the_template_and_the_broker_address() -> None:
    broker = NatsBroker()

    @broker.subscriber("logs.{level}")
    async def handler(msg: str) -> None: ...

    publisher = broker.publisher("out.{id}")

    subscriber = broker.subscribers[0]
    assert subscriber.subject_template == "logs.{level}"
    assert subscriber.broker_subject == "logs.*"

    assert publisher.subject_template == "out.{id}"
    assert publisher.broker_subject == "out.*"


@pytest.mark.nats()
def test_router_prefix_reaches_both_reads() -> None:
    broker = NatsBroker()
    router = NatsRouter(prefix="prefix_")

    @router.subscriber("logs.{level}")
    async def handler(msg: str) -> None: ...

    publisher = router.publisher("out.{id}")

    broker.include_router(router)

    subscriber = broker.subscribers[0]
    assert subscriber.subject_template == "prefix_logs.{level}"
    assert subscriber.broker_subject == "prefix_logs.*"

    assert publisher.subject_template == "prefix_out.{id}"
    assert publisher.broker_subject == "prefix_out.*"
