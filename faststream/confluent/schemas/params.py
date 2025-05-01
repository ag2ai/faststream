from typing import List, Literal, Union

from typing_extensions import Required, TypedDict

SecurityOptions = TypedDict(
    "SecurityOptions",
    {
        "sasl.mechanism": Required[
            Literal[
                "PLAIN",
                "GSSAPI",
                "SCRAM-SHA-256",
                "SCRAM-SHA-512",
                "OAUTHBEARER",
            ]
        ],
        "sasl.password": str,
        "sasl.username": str,
    },
    total=False,
)


class ConsumerConnectionParams(TypedDict, total=False):
    """A class to represent the connection parameters for a consumer."""

    bootstrap_servers: Union[str, List[str]]
    client_id: str
    retry_backoff_ms: int
    metadata_max_age_ms: int
    security_protocol: Literal[
        "SSL",
        "PLAINTEXT",
    ]
    connections_max_idle_ms: int
    allow_auto_create_topics: bool
    security_config: SecurityOptions
