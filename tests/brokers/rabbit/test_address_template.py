import pytest

from faststream.rabbit import RabbitQueue


@pytest.mark.rabbit()
def test_routing_key_keeps_both_the_template_and_the_broker_address() -> None:
    queue = RabbitQueue("test", routing_key="logs.{level}")

    assert queue.routing_template() == "logs.{level}"
    assert queue.routing() == "logs.*"


@pytest.mark.rabbit()
def test_both_reads_fall_back_to_the_queue_name() -> None:
    queue = RabbitQueue("test")

    assert queue.routing_template() == "test"
    assert queue.routing() == "test"


@pytest.mark.rabbit()
def test_prefix_reaches_both_reads() -> None:
    queue = RabbitQueue("test", routing_key="logs.{level}").add_prefix("prefix_")

    assert queue.routing_template() == "prefix_logs.{level}"
    assert queue.routing() == "prefix_logs.*"
