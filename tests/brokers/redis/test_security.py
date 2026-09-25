import ssl
import warnings

import pytest

from faststream.redis.security import parse_security
from faststream.security import BaseSecurity, SASLPlaintext

pytestmark = pytest.mark.redis


def test_no_security() -> None:
    assert parse_security(None) == {}


def test_base_security_without_context_enables_tls() -> None:
    opts = parse_security(BaseSecurity(use_ssl=True))
    connection = opts["connection_class"]()
    assert connection._connection_arguments()["ssl"] is True


def test_base_security_with_context_uses_it() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ssl_context = ssl.create_default_context()

    opts = parse_security(BaseSecurity(ssl_context=ssl_context))
    connection = opts["connection_class"]()
    assert connection._connection_arguments()["ssl"] is ssl_context


def test_base_security_no_ssl() -> None:
    assert parse_security(BaseSecurity(use_ssl=False)) == {}


def test_sasl_plaintext() -> None:
    opts = parse_security(
        SASLPlaintext(username="user", password="pass", use_ssl=False),
    )
    assert opts == {"username": "user", "password": "pass"}
