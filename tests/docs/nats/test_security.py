import pytest
from dirty_equals import IsPartialDict

from docs.docs_src.nats.security.credentials import broker as credentials_broker
from docs.docs_src.nats.security.jwt import broker as jwt_broker
from docs.docs_src.nats.security.nkey import broker as nkey_broker
from docs.docs_src.nats.security.tls import broker as tls_broker
from docs.docs_src.nats.security.token import broker as token_broker
from docs.docs_src.nats.security.user_password import broker as user_password_broker
from faststream.nats import NatsBroker


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("broker", "expected"),
    (
        (user_password_broker, {"user": "user", "password": "password"}),
        (
            credentials_broker,
            {"user_credentials": "/run/secrets/nats/user.creds"},
        ),
        (nkey_broker, {"nkeys_seed": "/run/secrets/nats/user.nk"}),
    ),
)
def test_security_examples(broker: NatsBroker, expected: dict[str, str]) -> None:
    assert broker._connection_kwargs == IsPartialDict(expected)


@pytest.mark.nats()
def test_token_callback_example() -> None:
    assert callable(token_broker._connection_kwargs["token"])


@pytest.mark.nats()
def test_jwt_callback_example() -> None:
    kwargs = jwt_broker._connection_kwargs
    assert (
        callable(kwargs["user_jwt_cb"]),
        callable(kwargs["signature_cb"]),
    ) == (True, True)


@pytest.mark.nats()
def test_tls_example() -> None:
    assert (
        tls_broker._connection_kwargs["tls_hostname"],
        tls_broker._connection_kwargs["tls_handshake_first"],
    ) == ("nats.example.com", True)
