from typing import TYPE_CHECKING, Any, Optional, Union

from aio_pika.connection import make_url

from faststream.rabbit.schemas.constants import ExchangeType

if TYPE_CHECKING:
    from aio_pika.abc import SSLOptions
    from pamqp.common import FieldTable
    from yarl import URL

    from faststream.rabbit.schemas import RabbitExchange


def build_url(
    url: Union[str, "URL", None] = None,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    login: Optional[str] = None,
    password: Optional[str] = None,
    virtualhost: Optional[str] = None,
    ssl: Optional[bool] = None,
    ssl_options: Optional["SSLOptions"] = None,
    client_properties: Optional["FieldTable"] = None,
    **kwargs: Any,
) -> "URL":
    """Construct URL object from attributes."""
    original_url = make_url(url)

    use_ssl = ssl or original_url.scheme == "amqps"
    default_port = 5671 if use_ssl else 5672

    return make_url(
        host=host or original_url.host or "localhost",
        port=port or original_url.port or default_port,
        login=login or original_url.user or "guest",
        password=password or original_url.password or "guest",
        virtualhost=virtualhost or original_url.path.replace("/", "", 1),
        ssl=use_ssl,
        ssl_options=ssl_options,
        client_properties=client_properties,
        **{
            **kwargs,
            **dict(original_url.query),
        },
    )


def is_routing_exchange(exchange: Optional["RabbitExchange"]) -> bool:
    """Check if an exchange requires routing_key to deliver message."""
    return not exchange or exchange.type in (
        ExchangeType.DIRECT.value,
        ExchangeType.TOPIC.value,
    )
