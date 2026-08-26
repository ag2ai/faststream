from re import Pattern

from faststream._internal.utils.path import (
    PARAM_REGEX,
    _ESCAPED_LEFT,
    _ESCAPED_RIGHT,
    compile_path,
)
from faststream.exceptions import SetupError

MQTT_TOPIC_BOUNDARIES = {"", "/"}


def compile_mqtt_path(path: str) -> tuple[Pattern[str] | None, str]:
    """Compile an MQTT topic template with named single-level captures.

    ``{name}`` captures one complete MQTT topic level and subscribes with the
    native ``+`` wildcard. Multi-level ``#`` captures are intentionally not
    supported; use ``MQTTMessage.raw_message.topic`` when the full topic is
    needed.
    """
    for match in PARAM_REGEX.finditer(path):
        name = match.group(1)
        start, end = match.start(), match.end()
        before = path[start - 1] if start > 0 else ""
        after = path[end] if end < len(path) else ""

        if before not in MQTT_TOPIC_BOUNDARIES or after not in MQTT_TOPIC_BOUNDARIES:
            msg = (
                f"Param {{{name}}} must occupy a whole topic level "
                f"(surrounded by '/' or string boundaries) in topic {path!r}"
            )
            raise SetupError(msg)

    path_regex, mqtt_topic = compile_path(
        path,
        replace_symbol="+",
        patch_regex=_patch_mqtt_regex,
        param_regex="[^/]+",
    )
    return path_regex, mqtt_topic


def _patch_mqtt_regex(regex: str) -> str:
    return (
        regex
        .replace(_ESCAPED_LEFT, r"\{")
        .replace(_ESCAPED_RIGHT, r"\}")
        .replace(r"\+", "[^/]+")
        .replace(r"/\#", "(?:/.*)?")
        .replace(r"\#", ".*")
    )
