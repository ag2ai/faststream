import ssl
from asyncio import AbstractEventLoop
from typing import List, Literal, Optional, Union

from aiokafka.abc import AbstractTokenProvider
from typing_extensions import TypedDict


class AdminClientConnectionParams(TypedDict, total=False):
    """A class to represent the connection parameters for an admin client.

    Attributes:
        bootstrap_servers : Required. The bootstrap servers to connect to.
        loop : Optional. The event loop to use for asynchronous operations.
        client_id : The client ID to use for the connection.
        request_timeout_ms : The timeout for network requests in milliseconds.
        retry_backoff_ms : The backoff time in milliseconds for retrying failed requests.
        metadata_max_age_ms : The maximum age of metadata in milliseconds.
        security_protocol : The security protocol to use for the connection. Must be one of "SSL" or "PLAINTEXT".
        api_version : The API version to use for the connection.
        connections_max_idle_ms : The maximum idle time in milliseconds before closing a connection.
        ssl_context : Pre-configured SSLContext for wrapping socket connections.
        sasl_mechanism : The SASL mechanism to use for authentication. Must be one of "PLAIN", "GSSAPI", "SCRAM-SHA-256", "SCRAM-SHA-512", or "OAUTHBEARER".
        sasl_plain_password : The password to use for PLAIN SASL mechanism.
        sasl_plain_username : The username to use for PLAIN SASL mechanism.
        sasl_kerberos_service_name : Service name to include in GSSAPI sasl mechanism handshake.
        sasl_kerberos_domain_name : Kerberos domain name to use in GSSAPI sasl mechanism handshake.
        sasl_oauth_token_provider : OAuthBearer token provider instance.
    """

    bootstrap_servers: Union[str, List[str]]
    loop: Optional[AbstractEventLoop]
    client_id: str
    request_timeout_ms: int
    retry_backoff_ms: int
    metadata_max_age_ms: int
    security_protocol: Literal[
        "SSL",
        "PLAINTEXT",
    ]
    api_version: str
    connections_max_idle_ms: int
    sasl_mechanism: Literal[
        "PLAIN",
        "GSSAPI",
        "SCRAM-SHA-256",
        "SCRAM-SHA-512",
        "OAUTHBEARER",
    ]
    sasl_plain_password: str
    sasl_plain_username: str
    sasl_kerberos_service_name: str
    sasl_kerberos_domain_name: str
    ssl_context: ssl.SSLContext
    sasl_oauth_token_provider: AbstractTokenProvider


class ConsumerConnectionParams(TypedDict, total=False):
    """A class to represent the connection parameters for a consumer.

    Attributes:
        bootstrap_servers : Required. The bootstrap servers to connect to.
        loop : Optional. The event loop to use for asynchronous operations.
        client_id : The client ID to use for the connection.
        request_timeout_ms : The timeout for network requests in milliseconds.
        retry_backoff_ms : The backoff time in milliseconds for retrying failed requests.
        metadata_max_age_ms : The maximum age of metadata in milliseconds.
        security_protocol : The security protocol to use for the connection. Must be one of "SSL" or "PLAINTEXT".
        api_version : The API version to use for the connection.
        connections_max_idle_ms : The maximum idle time in milliseconds before closing a connection.
        ssl_context : Pre-configured SSLContext for wrapping socket connections.
        sasl_mechanism : The SASL mechanism to use for authentication. Must be one of "PLAIN", "GSSAPI", "SCRAM-SHA-256", "SCRAM-SHA-512", or "OAUTHBEARER".
        sasl_plain_password : The password to use for PLAIN SASL mechanism.
        sasl_plain_username : The username to use for PLAIN SASL mechanism.
        sasl_kerberos_service_name : Service name to include in GSSAPI sasl mechanism handshake.
        sasl_kerberos_domain_name : Kerberos domain name to use in GSSAPI sasl mechanism handshake.
        sasl_oauth_token_provider : OAuthBearer token provider instance.
    """

    bootstrap_servers: Union[str, List[str]]
    loop: Optional[AbstractEventLoop]
    client_id: str
    request_timeout_ms: int
    retry_backoff_ms: int
    metadata_max_age_ms: int
    security_protocol: Literal[
        "SSL",
        "PLAINTEXT",
    ]
    api_version: str
    connections_max_idle_ms: int
    sasl_mechanism: Literal[
        "PLAIN",
        "GSSAPI",
        "SCRAM-SHA-256",
        "SCRAM-SHA-512",
        "OAUTHBEARER",
    ]
    sasl_plain_password: str
    sasl_plain_username: str
    sasl_kerberos_service_name: str
    sasl_kerberos_domain_name: str
    ssl_context: ssl.SSLContext
    sasl_oauth_token_provider: AbstractTokenProvider
