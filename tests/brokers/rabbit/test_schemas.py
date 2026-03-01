import pytest

from faststream.rabbit import Channel, RabbitExchange, RabbitQueue


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


@pytest.mark.rabbit()
def test_different_queue_params() -> None:
    assert (
        len({
            RabbitQueue("test", durable=True): 0,
            RabbitQueue("test", durable=False): 1,
        })
        == 2
    )


@pytest.mark.rabbit()
def test_exchange_equality() -> None:
    assert (
        len({
            RabbitExchange("test", durable=True): 0,
            RabbitExchange("test", durable=True): 1,
        })
        == 1
    )
    assert (
        len({
            RabbitExchange("test", durable=True): 0,
            RabbitExchange("test", durable=False): 1,
        })
        == 2
    )


@pytest.mark.rabbit()
def test_channel_equality() -> None:
    assert (
        len({
            Channel(prefetch_count=10): 0,
            Channel(prefetch_count=10): 1,
        })
        == 1
    )
    assert (
        len({
            Channel(prefetch_count=10): 0,
            Channel(prefetch_count=20): 1,
        })
        == 2
    )
