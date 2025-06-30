from faststream.rabbit import RabbitQueue


def test_same_queue() -> None:
    assert (
        len({
            RabbitQueue("test"): 0,
            RabbitQueue("test"): 1,
        })
        == 1
    )


def test_different_queue_routing_key() -> None:
    assert (
        len({
            RabbitQueue("test", routing_key="binding-1"): 0,
            RabbitQueue("test", routing_key="binding-2"): 1,
        })
        == 1
    )
