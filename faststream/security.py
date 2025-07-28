from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ssl import SSLContext


class BaseSecurity:
    """Base class for defining security configurations.

    This class provides a base for defining security configurations for communication with a broker. It allows setting
    SSL encryption and provides methods to retrieve security requirements and schemas.
    """

    ssl_context: Optional["SSLContext"]
    use_ssl: bool

    def __init__(
        self,
        ssl_context: Optional["SSLContext"] = None,
        use_ssl: bool | None = None,
    ) -> None:
        if ssl_context is not None:
            use_ssl = True

        if use_ssl is None:
            use_ssl = False

        self.use_ssl = use_ssl
        self.ssl_context = ssl_context

    def get_requirement(self) -> list["dict[str, Any]"]:
        """Get the security requirements."""
        return []

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema."""
        return {}


class SASLPlaintext(BaseSecurity):
    """Security configuration for SASL/PLAINTEXT authentication.

    This class defines security configuration for SASL/PLAINTEXT authentication, which includes a username and password.
    """

    # TODO: mv to SecretStr
    __slots__ = (
        "password",
        "ssl_context",
        "use_ssl",
        "username",
    )

    def __init__(
        self,
        username: str,
        password: str,
        ssl_context: Optional["SSLContext"] = None,
        use_ssl: bool | None = None,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
        )

        self.username = username
        self.password = password

    def get_requirement(self) -> list["dict[str, Any]"]:
        """Get the security requirements for SASL/PLAINTEXT authentication."""
        return [{"user-password": []}]

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema for SASL/PLAINTEXT authentication."""
        return {"user-password": {"type": "userPassword"}}


class SASLScram256(BaseSecurity):
    """Security configuration for SASL/SCRAM-SHA-256 authentication.

    This class defines security configuration for SASL/SCRAM-SHA-256 authentication, which includes a username and password.
    """

    # TODO: mv to SecretStr
    __slots__ = (
        "password",
        "ssl_context",
        "use_ssl",
        "username",
    )

    def __init__(
        self,
        username: str,
        password: str,
        ssl_context: Optional["SSLContext"] = None,
        use_ssl: bool | None = None,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
        )

        self.username = username
        self.password = password

    def get_requirement(self) -> list["dict[str, Any]"]:
        """Get the security requirements for SASL/SCRAM-SHA-256 authentication."""
        return [{"scram256": []}]

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema for SASL/SCRAM-SHA-256 authentication."""
        return {"scram256": {"type": "scramSha256"}}


class SASLScram512(BaseSecurity):
    """Security configuration for SASL/SCRAM-SHA-512 authentication.

    This class defines security configuration for SASL/SCRAM-SHA-512 authentication, which includes a username and password.
    """

    # TODO: mv to SecretStr
    __slots__ = (
        "password",
        "ssl_context",
        "use_ssl",
        "username",
    )

    def __init__(
        self,
        username: str,
        password: str,
        ssl_context: Optional["SSLContext"] = None,
        use_ssl: bool | None = None,
    ) -> None:
        super().__init__(
            ssl_context=ssl_context,
            use_ssl=use_ssl,
        )

        self.username = username
        self.password = password

    def get_requirement(self) -> list["dict[str, Any]"]:
        """Get the security requirements for SASL/SCRAM-SHA-512 authentication."""
        return [{"scram512": []}]

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema for SASL/SCRAM-SHA-512 authentication."""
        return {"scram512": {"type": "scramSha512"}}


class SASLOAuthBearer(BaseSecurity):
    """Security configuration for SASL/OAUTHBEARER authentication.

    This class defines basic security configuration for SASL/OAUTHBEARER authentication.
    """

    __slots__ = ("ssl_context", "use_ssl")

    def get_requirement(self) -> list["dict[str, Any]"]:
        """Get the security requirements for SASL/OAUTHBEARER authentication."""
        return [{"oauthbearer": []}]

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema for SASL/OAUTHBEARER authentication."""
        return {"oauthbearer": {"type": "oauth2", "$ref": ""}}


class SASLGSSAPI(BaseSecurity):
    """Security configuration for SASL/GSSAPI authentication.

    This class defines security configuration for SASL/GSSAPI authentication.
    """

    __slots__ = ("ssl_context", "use_ssl")

    def get_requirement(self) -> list["dict[str, Any]"]:
        """Get the security requirements for SASL/GSSAPI authentication."""
        return [{"gssapi": []}]

    def get_schema(self) -> dict[str, dict[str, str]]:
        """Get the security schema for SASL/GSSAPI authentication."""
        return {"gssapi": {"type": "gssapi"}}
