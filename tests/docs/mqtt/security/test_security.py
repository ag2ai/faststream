import pytest


@pytest.mark.mqtt()
def test_security_example_imports() -> None:
    from docs.docs_src.mqtt.security.plaintext import broker, security

    assert broker is not None
    assert security is not None
