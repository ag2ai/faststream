import pytest


@pytest.mark.mqtt()
def test_security_example() -> None:
    from docs.docs_src.mqtt.security.plaintext import (
        broker,
        broker_from_url,
        security,
    )

    assert broker._connection_kwargs["username"] == security.username
    assert broker._connection_kwargs["password"] == security.password
    assert broker._connection_kwargs["tls"] is True
    assert broker_from_url._connection_kwargs["username"] == "device"
    assert broker_from_url._connection_kwargs["password"] == "secret"
    assert broker_from_url._connection_kwargs["tls"] is True
