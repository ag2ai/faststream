from faststream._internal.utils.path import PARAM_REGEX, Address, AddressSyntax
from faststream.exceptions import SetupError

MQTT_ADDRESS_SYNTAX = AddressSyntax(
    replace_symbol="+",
    patch_regex=lambda x: (
        x.replace(r"\+", "[^/]+").replace(r"/\#", "(?:/.*)?").replace(r"\#", ".*")
    ),
    param_regex="[^/]+",
)


def build_mqtt_address(topic: str) -> Address:
    """Read an MQTT topic template, rejecting a param that shares a topic level.

    ``{name}`` captures one complete MQTT topic level and subscribes with the
    native ``+`` wildcard. Multi-level ``#`` captures are intentionally not
    supported; use ``MQTTMessage.raw_message.topic`` when the full topic is
    needed.
    """
    for match in PARAM_REGEX.finditer(topic):
        name = match.group(1)
        start, end = match.start(), match.end()
        before = topic[start - 1] if start > 0 else ""
        after = topic[end] if end < len(topic) else ""

        if before not in _TOPIC_BOUNDARIES or after not in _TOPIC_BOUNDARIES:
            msg = (
                f"Param {{{name}}} must occupy a whole topic level "
                f"(surrounded by '/' or string boundaries) in topic {topic!r}"
            )
            raise SetupError(msg)

    return Address(topic, MQTT_ADDRESS_SYNTAX)


_TOPIC_BOUNDARIES = {"", "/"}
