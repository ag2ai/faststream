import warnings
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from nats.aio.client import Credentials, RawCredentials
from typing_extensions import Self

from faststream.security import BaseSecurity, SASLPlaintext

if TYPE_CHECKING:
    from ssl import SSLContext


_LEGACY_SECURITY_TARGETS: Final[dict[str, str]] = {
    "tls_hostname": "NatsSecurity",
    "token": "NatsToken",
    "signature_cb": "NatsJWT",
    "user_jwt_cb": "NatsJWT",
    "user_credentials": "NatsCredentials",
    "nkeys_seed": "NatsNKey.from_file",
    "nkeys_seed_str": "NatsNKey.from_seed",
}


def warn_deprecated_security_args(*arguments: str) -> None:
    names = ", ".join(f"`{name}`" for name in arguments)
    replacements = ", ".join(
        dict.fromkeys(_LEGACY_SECURITY_TARGETS[name] for name in arguments)
    )
    warnings.warn(
        f"The NATS security arguments {names} are deprecated and will be removed "
        "in FastStream. Use `security=` with "
        f"{replacements} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


class NatsSecurity(BaseSecurity):
    """Base class for NATS security configurations."""

    __slots__ = ("ssl_context", "tls_handshake_first", "tls_hostname", "use_ssl")

    def __init__(
        self,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> None:
        super().__init__(ssl_context=ssl_context, use_ssl=use_ssl)
        self.tls_hostname = tls_hostname
        self.tls_handshake_first = tls_handshake_first


class NatsToken(NatsSecurity):
    """NATS token authentication."""

    __slots__ = ("token",)

    def __init__(
        self,
        token: str | Callable[[], str],
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> None:
        if not callable(token) and not token:
            msg = "NATS token cannot be empty."
            raise ValueError(msg)

        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        self.token = token

    def get_requirement(self) -> list[dict[str, Any]]:
        """Get the AsyncAPI requirement for NATS token authentication."""
        return [{"nats-token": []}]

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the AsyncAPI schema for NATS token authentication."""
        return {
            "nats-token": {
                "type": "apiKey",
                "in": "user",
                "description": "NATS authentication token sent as CONNECT auth_token.",
                "x-nats-auth": "token",
            },
        }


class NatsUserPassword(NatsSecurity):
    """NATS username and password authentication."""

    __slots__ = ("password", "username")

    def __init__(
        self,
        username: str,
        password: str,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        self.username = username
        self.password = password

    def get_requirement(self) -> list[dict[str, Any]]:
        """Get the AsyncAPI requirement for NATS user/password authentication."""
        return [{"nats-user-password": []}]

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the AsyncAPI schema for NATS user/password authentication."""
        return {"nats-user-password": {"type": "userPassword"}}


class NatsCredentials(NatsSecurity):
    """NATS JWT credentials authentication."""

    __slots__ = ("credentials",)

    def __init__(
        self,
        credentials: Credentials,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        self.credentials = credentials

    def get_requirement(self) -> list[dict[str, Any]]:
        """Get the AsyncAPI requirement for NATS credentials authentication."""
        return [{"nats-jwt": []}]

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the AsyncAPI schema for NATS credentials authentication."""
        return {
            "nats-jwt": {
                "type": "asymmetricEncryption",
                "description": (
                    "NATS user JWT authentication with NKey challenge signing."
                ),
                "x-nats-auth": "jwt-nkey",
                "x-nats-credential-source": "credentials-file",
            },
        }

    @classmethod
    def from_file(
        cls,
        credentials_file: str | Path,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> Self:
        """Load a combined NATS credentials file."""
        return cls(
            credentials_file,
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )

    @classmethod
    def from_files(
        cls,
        *,
        jwt: str | Path,
        seed: str | Path,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> Self:
        """Load separate NATS user JWT and seed files."""
        security = cls(
            jwt,
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        security.credentials = (str(jwt), str(seed))
        return security

    @classmethod
    def from_raw(
        cls,
        credentials: str,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> Self:
        """Use raw combined NATS credentials."""
        markers = ("BEGIN NATS USER JWT", "BEGIN USER NKEY SEED")
        if not credentials or any(marker not in credentials for marker in markers):
            msg = "Raw NATS credentials do not contain the required markers."
            raise ValueError(msg)

        security = cls(
            "",
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        security.credentials = RawCredentials(credentials)
        return security


class NatsNKey(NatsSecurity):
    """NATS NKey challenge-response authentication."""

    __slots__ = ("seed", "seed_is_file")

    def __init__(
        self,
        seed_file: str | Path,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> None:
        if not str(seed_file):
            msg = "NATS NKey seed file cannot be empty."
            raise ValueError(msg)

        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        self.seed = str(seed_file)
        self.seed_is_file = True

    def get_requirement(self) -> list[dict[str, Any]]:
        """Get the AsyncAPI requirement for NATS NKey authentication."""
        return [{"nats-nkey": []}]

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the AsyncAPI schema for NATS NKey authentication."""
        return {
            "nats-nkey": {
                "type": "asymmetricEncryption",
                "description": (
                    "NATS NKey challenge-response authentication using an "
                    "Ed25519 signature."
                ),
                "x-nats-auth": "nkey",
                "x-nats-algorithm": "ed25519",
            },
        }

    @classmethod
    def from_file(
        cls,
        seed_file: str | Path,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> Self:
        """Load a NATS NKey seed file."""
        return cls(
            seed_file,
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )

    @classmethod
    def from_seed(
        cls,
        seed: str,
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> Self:
        """Use a raw NATS NKey seed."""
        if not seed:
            msg = "NATS NKey seed cannot be empty."
            raise ValueError(msg)

        security = cls(
            seed,
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        security.seed_is_file = False
        return security


class NatsJWT(NatsSecurity):
    """NATS user JWT authentication with NKey challenge signing."""

    __slots__ = ("jwt_cb", "signature_cb")

    def __init__(
        self,
        jwt_cb: Callable[[], bytes | bytearray],
        signature_cb: Callable[[str], bytes],
        *,
        ssl_context: "SSLContext | None" = None,
        use_ssl: bool | None = None,
        tls_hostname: str | None = None,
        tls_handshake_first: bool = False,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
            tls_hostname=tls_hostname,
            tls_handshake_first=tls_handshake_first,
        )
        self.jwt_cb = jwt_cb
        self.signature_cb = signature_cb

    def get_requirement(self) -> list[dict[str, Any]]:
        """Get the AsyncAPI requirement for NATS JWT authentication."""
        return [{"nats-jwt": []}]

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the AsyncAPI schema for NATS JWT authentication."""
        return {
            "nats-jwt": {
                "type": "asymmetricEncryption",
                "description": (
                    "NATS user JWT authentication with NKey challenge signing."
                ),
                "x-nats-auth": "jwt-nkey",
                "x-nats-credential-source": "callbacks",
            },
        }


def parse_security(security: BaseSecurity | None) -> dict[str, Any]:
    if security is None:
        return {}
    if isinstance(security, NatsToken):
        return _parse_nats_token(security)
    if isinstance(security, NatsUserPassword):
        return _parse_nats_user_password(security)
    if isinstance(security, NatsCredentials):
        return _parse_nats_credentials(security)
    if isinstance(security, NatsNKey):
        return _parse_nats_nkey(security)
    if isinstance(security, NatsJWT):
        return _parse_nats_jwt(security)
    if type(security) is NatsSecurity:
        return _parse_nats_security(security)
    if isinstance(security, SASLPlaintext):
        return _parse_sasl_plaintext(security)
    if type(security) is BaseSecurity:
        return _parse_base_security(security)
    msg = f"NatsBroker does not support {type(security)}"
    raise NotImplementedError(msg)


def _parse_nats_security(security: NatsSecurity) -> dict[str, Any]:
    return {
        "tls": security.ssl_context,
        "tls_hostname": security.tls_hostname,
        "tls_handshake_first": security.tls_handshake_first,
    }


def _parse_nats_token(security: NatsToken) -> dict[str, Any]:
    return {
        **_parse_nats_security(security),
        "token": security.token,
    }


def _parse_nats_user_password(security: NatsUserPassword) -> dict[str, Any]:
    return {
        **_parse_nats_security(security),
        "user": security.username,
        "password": security.password,
    }


def _parse_nats_credentials(security: NatsCredentials) -> dict[str, Any]:
    return {
        **_parse_nats_security(security),
        "user_credentials": security.credentials,
    }


def _parse_nats_nkey(security: NatsNKey) -> dict[str, Any]:
    seed_key = "nkeys_seed" if security.seed_is_file else "nkeys_seed_str"
    return {
        **_parse_nats_security(security),
        seed_key: security.seed,
    }


def _parse_nats_jwt(security: NatsJWT) -> dict[str, Any]:
    return {
        **_parse_nats_security(security),
        "user_jwt_cb": security.jwt_cb,
        "signature_cb": security.signature_cb,
    }


def _parse_base_security(security: BaseSecurity) -> dict[str, Any]:
    return {
        "tls": security.ssl_context,
    }


def _parse_sasl_plaintext(security: SASLPlaintext) -> dict[str, Any]:
    return {
        "tls": security.ssl_context,
        "user": security.username,
        "password": security.password,
    }
