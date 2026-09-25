from dataclasses import dataclass
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True, slots=True, kw_only=True)
class MQTTUrl:
    """Parsed MQTT connection URL."""

    host: str
    port: int
    tls: bool
    username: str | None
    password: str | None
    port_provided: bool


def parse_mqtt_url(url: str) -> MQTTUrl:
    """Parse an MQTT TCP/TLS connection URL."""
    parsed = urlsplit(url if "://" in url else f"//{url}", scheme="mqtt")
    scheme = parsed.scheme.lower()

    if scheme not in {"mqtt", "mqtts"}:
        msg = "MQTT URL scheme must be either 'mqtt' or 'mqtts'."
        raise ValueError(msg)

    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        msg = "MQTT URL must not include a path, query, or fragment."
        raise ValueError(msg)

    host = parsed.hostname
    if host is None:
        msg = "MQTT URL must include a host."
        raise ValueError(msg)

    parsed_port = parsed.port
    tls = scheme == "mqtts"

    return MQTTUrl(
        host=host,
        port=parsed_port if parsed_port is not None else (8883 if tls else 1883),
        tls=tls,
        username=unquote(parsed.username) if parsed.username is not None else None,
        password=unquote(parsed.password) if parsed.password is not None else None,
        port_provided=parsed_port is not None,
    )


def build_mqtt_url(*, host: str, port: int, tls: bool) -> str:
    """Build a credential-free MQTT connection URL."""
    formatted_host = f"[{host}]" if ":" in host else host
    scheme = "mqtts" if tls else "mqtt"
    return f"{scheme}://{formatted_host}:{port}"
