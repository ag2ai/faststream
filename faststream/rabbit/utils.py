from typing import TYPE_CHECKING, Any, Optional, TypedDict, Union

from aio_pika.connection import make_url

from faststream.rabbit.schemas.constants import ExchangeType

if TYPE_CHECKING:
    from aio_pika.abc import SSLOptions
    from yarl import URL

    from faststream.rabbit.schemas import RabbitExchange


def build_virtual_host(
    url: Union[str, "URL", None],
    virtualhost: str | None,
    path: str,
) -> str:
    if (not url and not virtualhost) or virtualhost == "/":
        return ""
    if virtualhost and virtualhost.startswith("//"):
        return virtualhost.replace("/", "", 1)
    return virtualhost or path.replace("/", "", 1)


def build_url(
    url: Union[str, "URL", None] = None,
    *,
    host: str | None = None,
    port: int | None = None,
    login: str | None = None,
    password: str | None = None,
    virtualhost: str | None = None,
    ssl: bool | None = None,
    ssl_options: Optional["SSLOptions"] = None,
    client_properties: Optional["RabbitClientProperties"] = None,
    auth: str | None = None,
    **kwargs: Any,
) -> "URL":
    """Construct URL object from attributes."""
    original_url = make_url(url)
    query = {
        **kwargs,
        **dict(original_url.query),
    }

    if auth is not None:
        query["auth"] = auth

    external_auth = query.get("auth") == "EXTERNAL"
    login = login or original_url.user or "guest"
    password = password or original_url.password or "guest"

    use_ssl = ssl or original_url.scheme == "amqps"
    default_port = 5671 if use_ssl else 5672

    amqp_url = make_url(
        host=host or original_url.host or "localhost",
        port=port or original_url.port or default_port,
        login=login,
        password=password,
        virtualhost=build_virtual_host(url, virtualhost, original_url.path),
        ssl=use_ssl,
        ssl_options=ssl_options,
        client_properties=client_properties,  # type: ignore[arg-type]
        **query,
    )

    if external_auth:
        return amqp_url.with_user(None).with_password(None)
    return amqp_url


def is_routing_exchange(exchange: Optional["RabbitExchange"]) -> bool:
    """Check if an exchange requires routing_key to deliver message."""
    return not exchange or exchange.type in {
        ExchangeType.DIRECT.value,
        ExchangeType.TOPIC.value,
    }


class RabbitClientProperties(TypedDict, total=False):
    heartbeat: int
    connection_timeout: int
    channel_max: int
