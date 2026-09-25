import pytest

from faststream.rabbit import TestApp, TestRabbitBroker


@pytest.mark.rabbit()
def test_url() -> None:
    from docs.docs_src.rabbit.connection.url import broker

    assert broker.specification.url == ["amqp://guest:guest@localhost:5672/my_vhost"]


@pytest.mark.rabbit()
def test_params() -> None:
    from docs.docs_src.rabbit.connection.params import broker

    assert broker.specification.url == [
        "amqp://app:secret@rabbit.internal:5673/my_vhost"  # pragma: allowlist secret
    ]


@pytest.mark.rabbit()
@pytest.mark.asyncio()
async def test_multiple_vhosts() -> None:
    from docs.docs_src.rabbit.connection.multiple_vhosts import (
        app,
        billing_broker,
        handle_order,
        orders_broker,
    )

    assert [orders_broker.specification.url, billing_broker.specification.url] == [
        ["amqp://guest:guest@localhost:5672/orders"],
        ["amqp://guest:guest@localhost:5672/billing"],
    ]

    async with TestRabbitBroker(orders_broker, billing_broker), TestApp(app):
        await orders_broker.publish("order-1", "order-created")

        handle_order.mock.assert_called_once_with("order-1")
