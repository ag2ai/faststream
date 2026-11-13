from ssl import create_default_context
from typing import Any
from unittest.mock import Mock

import pytest
from nats.aio.client import Credentials, RawCredentials

from faststream.exceptions import SetupError
from faststream.nats import (
    NatsBroker,
    NatsCredentials,
    NatsJWT,
    NatsNKey,
    NatsSecurity,
    NatsToken,
    NatsUserPassword,
)
from faststream.security import SASLScram256

RAW_CREDENTIALS = """-----BEGIN NATS USER JWT-----
user-jwt
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
user-seed
------END USER NKEY SEED------
"""


@pytest.mark.nats()
def test_token_callback_is_forwarded_to_connection_options() -> None:
    token = Mock(return_value="secret")

    broker = NatsBroker(security=NatsToken(token))

    token.assert_not_called()
    assert broker._connection_kwargs["token"] is token


@pytest.mark.nats()
def test_user_password_is_forwarded_to_connection_options() -> None:
    broker = NatsBroker(security=NatsUserPassword("user", "password"))

    kwargs = broker._connection_kwargs
    assert (kwargs["user"], kwargs["password"]) == ("user", "password")


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("security", "expected"),
    (
        (NatsCredentials.from_file("user.creds"), "user.creds"),
        (
            NatsCredentials.from_files(jwt="user.jwt", seed="user.nk"),
            ("user.jwt", "user.nk"),
        ),
        (NatsCredentials.from_raw(RAW_CREDENTIALS), RawCredentials(RAW_CREDENTIALS)),
    ),
)
def test_credentials_are_forwarded_to_connection_options(
    security: NatsCredentials,
    expected: Credentials,
) -> None:
    broker = NatsBroker(security=security)

    assert broker._connection_kwargs["user_credentials"] == expected


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("security", "key", "expected"),
    (
        (NatsNKey.from_file("user.nk"), "nkeys_seed", "user.nk"),
        (NatsNKey.from_seed("SU_TEST_SEED"), "nkeys_seed_str", "SU_TEST_SEED"),
    ),
)
def test_nkey_is_forwarded_to_connection_options(
    security: NatsNKey,
    key: str,
    expected: str,
) -> None:
    broker = NatsBroker(security=security)

    assert broker._connection_kwargs[key] == expected


@pytest.mark.nats()
def test_jwt_callbacks_are_forwarded_to_connection_options() -> None:
    jwt = Mock(return_value=b"jwt")
    signature = Mock(return_value=b"signature")
    broker = NatsBroker(security=NatsJWT(jwt, signature))

    jwt.assert_not_called()
    signature.assert_not_called()
    kwargs = broker._connection_kwargs
    assert (kwargs["user_jwt_cb"], kwargs["signature_cb"]) == (jwt, signature)


@pytest.mark.nats()
def test_tls_options_are_forwarded_with_authentication() -> None:
    context = create_default_context()

    broker = NatsBroker(
        "tls://nats.example.com:4222",
        security=NatsToken(
            "secret",
            ssl_context=context,
            tls_hostname="nats.internal",
            tls_handshake_first=True,
        ),
    )
    kwargs = broker._connection_kwargs
    assert (
        kwargs["tls"],
        kwargs["tls_hostname"],
        kwargs["tls_handshake_first"],
    ) == (context, "nats.internal", True)


@pytest.mark.nats()
def test_legacy_token_warns_once_and_keeps_value() -> None:
    with pytest.warns(
        DeprecationWarning,
        match=r"`token`.*will be removed in 1\.0\.0.*NatsToken",
    ) as warnings:
        broker = NatsBroker(token="legacy-secret")

    assert (len(warnings), broker._connection_kwargs["token"]) == (
        1,
        "legacy-secret",
    )


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("legacy", "expected"),
    (
        (
            {"user_credentials": "user.creds"},
            {"user_credentials": "user.creds"},
        ),
        ({"nkeys_seed": "user.nk"}, {"nkeys_seed": "user.nk"}),
        ({"nkeys_seed_str": "SU_TEST"}, {"nkeys_seed_str": "SU_TEST"}),
        (
            {
                "user_jwt_cb": Mock(return_value=b"jwt"),
                "signature_cb": Mock(return_value=b"signature"),
            },
            {},
        ),
        ({"tls_hostname": "nats.internal"}, {"tls_hostname": "nats.internal"}),
    ),
)
def test_legacy_security_arguments_warn_once_and_keep_values(
    legacy: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    if not expected:
        expected = legacy

    with pytest.warns(DeprecationWarning) as warnings:
        broker = NatsBroker(**legacy)

    actual = broker._connection_kwargs
    assert (len(warnings), {key: actual[key] for key in expected}) == (1, expected)


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("legacy", "expected_type", "expected_key", "expected_value"),
    (
        (
            {"token": "legacy-token"},
            NatsToken,
            "token",
            "legacy-token",
        ),
        (
            {"user_credentials": "legacy.creds"},
            NatsCredentials,
            "user_credentials",
            "legacy.creds",
        ),
        (
            {"nkeys_seed": "legacy.nk"},
            NatsNKey,
            "nkeys_seed",
            "legacy.nk",
        ),
        (
            {"nkeys_seed_str": "SU_LEGACY_SEED"},
            NatsNKey,
            "nkeys_seed_str",
            "SU_LEGACY_SEED",
        ),
    ),
)
def test_legacy_authentication_replaces_new_security(
    legacy: dict[str, Any],
    expected_type: type[NatsSecurity],
    expected_key: str,
    expected_value: str,
) -> None:
    with pytest.warns(DeprecationWarning):
        broker = NatsBroker(
            security=NatsToken("new-token"),
            **legacy,
        )

    assert broker._connection_kwargs[expected_key] == expected_value
    if expected_key != "token":
        assert "token" not in broker._connection_kwargs
    assert isinstance(broker.specification.security, expected_type)


@pytest.mark.nats()
def test_legacy_jwt_callbacks_replace_new_security() -> None:
    jwt = Mock(return_value=b"legacy-jwt")
    signature = Mock(return_value=b"legacy-signature")

    with pytest.warns(DeprecationWarning):
        broker = NatsBroker(
            security=NatsToken("new-token"),
            user_jwt_cb=jwt,
            signature_cb=signature,
        )

    kwargs = broker._connection_kwargs
    assert "token" not in kwargs
    assert (kwargs["user_jwt_cb"], kwargs["signature_cb"]) == (jwt, signature)
    assert isinstance(broker.specification.security, NatsJWT)


@pytest.mark.nats()
def test_multiple_legacy_authentication_mechanisms_are_rejected() -> None:
    with (
        pytest.warns(DeprecationWarning),
        pytest.raises(SetupError, match="Only one deprecated NATS authentication"),
    ):
        NatsBroker(token="token", user_credentials="user.creds")


@pytest.mark.nats()
@pytest.mark.parametrize("callback", ("user_jwt_cb", "signature_cb"))
def test_incomplete_legacy_jwt_callbacks_are_rejected(callback: str) -> None:
    with (
        pytest.warns(DeprecationWarning),
        pytest.raises(SetupError, match="must be provided together"),
    ):
        NatsBroker(**{callback: Mock()})


@pytest.mark.nats()
def test_url_and_security_authentication_conflict() -> None:
    with pytest.raises(SetupError, match="URL credentials conflict with `security`"):
        NatsBroker(
            "nats://user:password@localhost:4222",
            security=NatsToken("token"),
        )


@pytest.mark.nats()
def test_unsupported_security_type_is_rejected() -> None:
    with pytest.raises(NotImplementedError, match="does not support"):
        NatsBroker(security=SASLScram256("user", "password"))
