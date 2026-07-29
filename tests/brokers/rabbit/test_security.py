import ssl

import pytest

from faststream.rabbit import RabbitBroker, RabbitExternalAuth


@pytest.mark.rabbit()
def test_external_auth_security() -> None:
    ssl_context = ssl.create_default_context()
    security = RabbitExternalAuth(ssl_context=ssl_context)

    broker = RabbitBroker("amqp://guest:guest@localhost/", security=security)

    assert broker.specification.url == ["amqps://localhost:5671/?auth=EXTERNAL"]
    assert broker._connection_kwargs["url"] == "amqps://localhost:5671/?auth=EXTERNAL"
    assert broker._connection_kwargs["ssl_context"] is ssl_context
