import pytest

from faststream.rabbit import RabbitQueue


@pytest.mark.rabbit()
def test_same_queue() -> None:
    assert (
        len({
            RabbitQueue("test"): 0,
            RabbitQueue("test"): 1,
        })
        == 1
    )


@pytest.mark.rabbit()
def test_different_queue_routing_key() -> None:
    assert (
        len({
            RabbitQueue("test", routing_key="binding-1"): 0,
            RabbitQueue("test", routing_key="binding-2"): 1,
        })
        == 1
    )
