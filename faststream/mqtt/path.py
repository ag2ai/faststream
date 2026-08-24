import re
from dataclasses import dataclass
from re import Pattern

from faststream._internal.utils.path import AddressSyntax
from faststream.exceptions import SetupError

MQTT_PARAM_REGEX = re.compile(r"(?<!\{)\{([a-zA-Z0-9_]+)\}(?!\})")
MQTT_TOPIC_BOUNDARIES = {"", "/"}
_ESCAPED_LEFT_BRACE = "__faststream_mqtt_escaped_left_brace__"
_ESCAPED_RIGHT_BRACE = "__faststream_mqtt_escaped_right_brace__"


@dataclass(frozen=True, slots=True)
class MQTTAddressSyntax(AddressSyntax):
    """MQTT's Address syntax: `+` for a Path parameter, `{{`/`}}` for a literal brace.

    MQTT is the one broker whose topics can carry a literal brace, so the escape
    is resolved before the shared compiler sees the template — and restored in the
    Broker address afterwards.
    """

    def compile(self, template: str) -> tuple[Pattern[str] | None, str]:
        escaped = _escape_literal_braces(template)
        _check_whole_level_params(escaped, template)

        # Named base rather than `super()`: `slots=True` rebuilds the class after
        # this method's `__class__` cell is bound, which breaks the zero-arg form.
        path_regex, mqtt_topic = AddressSyntax.compile(self, escaped)
        return path_regex, _restore_literal_braces(mqtt_topic)


def compile_mqtt_path(path: str) -> tuple[Pattern[str] | None, str]:
    """Compile an MQTT topic template with named single-level captures.

    ``{name}`` captures one complete MQTT topic level and subscribes with the
    native ``+`` wildcard. Multi-level ``#`` captures are intentionally not
    supported; use ``MQTTMessage.raw_message.topic`` when the full topic is
    needed.
    """
    return MQTT_ADDRESS_SYNTAX.compile(path)


def _check_whole_level_params(escaped_path: str, path: str) -> None:
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


MQTT_ADDRESS_SYNTAX = MQTTAddressSyntax(
    replace_symbol="+",
    patch_regex=_patch_mqtt_regex,
    param_regex="[^/]+",
)
