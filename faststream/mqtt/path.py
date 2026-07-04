import re
from re import Pattern

from faststream._internal.utils.path import compile_path
from faststream.exceptions import SetupError

MQTT_PARAM_REGEX = re.compile(r"(?<!\{)\{([a-zA-Z0-9_]+)\}(?!\})")
MQTT_TOPIC_BOUNDARIES = {"", "/"}
_ESCAPED_LEFT_BRACE = "__faststream_mqtt_escaped_left_brace__"
_ESCAPED_RIGHT_BRACE = "__faststream_mqtt_escaped_right_brace__"


def compile_mqtt_path(path: str) -> tuple[Pattern[str] | None, str]:
    """Compile an MQTT topic template with named single-level captures.

    ``{name}`` captures one complete MQTT topic level and subscribes with the
    native ``+`` wildcard. Multi-level ``#`` captures are intentionally not
    supported; use ``MQTTMessage.raw_message.topic`` when the full topic is
    needed.
    """
    escaped_path = _escape_literal_braces(path)

    for match in MQTT_PARAM_REGEX.finditer(escaped_path):
        name = match.group(1)
        start, end = match.start(), match.end()
        before = escaped_path[start - 1] if start > 0 else ""
        after = escaped_path[end] if end < len(escaped_path) else ""

        if before not in MQTT_TOPIC_BOUNDARIES or after not in MQTT_TOPIC_BOUNDARIES:
            msg = (
                f"Param {{{name}}} must occupy a whole topic level "
                f"(surrounded by '/' or string boundaries) in topic {path!r}"
            )
            raise SetupError(msg)

    path_regex, mqtt_topic = compile_path(
        escaped_path,
        replace_symbol="+",
        patch_regex=_patch_mqtt_regex,
        param_regex="[^/]+",
    )
    return path_regex, _restore_literal_braces(mqtt_topic)


def _escape_literal_braces(path: str) -> str:
    result = ""
    idx = 0
    while idx < len(path):
        if path.startswith("{{", idx):
            result += _ESCAPED_LEFT_BRACE
            idx += 2
        elif path.startswith("}}", idx):
            result += _ESCAPED_RIGHT_BRACE
            idx += 2
        else:
            result += path[idx]
            idx += 1
    return result


def _restore_literal_braces(path: str) -> str:
    return path.replace(_ESCAPED_LEFT_BRACE, "{").replace(_ESCAPED_RIGHT_BRACE, "}")


def _patch_mqtt_regex(regex: str) -> str:
    return (
        regex
        .replace(_ESCAPED_LEFT_BRACE, r"\{")
        .replace(_ESCAPED_RIGHT_BRACE, r"\}")
        .replace(r"\+", "[^/]+")
        .replace(r"/\#", "(?:/.*)?")
        .replace(r"\#", ".*")
    )
