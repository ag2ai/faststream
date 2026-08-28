import re

from faststream.exceptions import SetupError

MAX_TOPIC_NAME_LENGTH = 249


def validate_topic_name(topic: str) -> None:
    """Reject a topic name Kafka will refuse, at the line that declares it.

    Kafka answers an illegal name with `INVALID_TOPIC_EXCEPTION` only once a
    consumer or producer reaches the cluster, which is long after startup and
    nowhere near the declaration responsible. These are the rules Kafka itself
    applies, in `org.apache.kafka.common.internals.Topic`.
    """
    if not topic:
        msg = "A Kafka topic name cannot be empty."
        raise SetupError(msg)

    if topic in _RESERVED_NAMES:
        msg = f"A Kafka topic cannot be named {topic!r}."
        raise SetupError(msg)

    if len(topic) > MAX_TOPIC_NAME_LENGTH:
        msg = (
            f"Kafka topic {topic!r} is {len(topic)} characters long, "
            f"and a topic name may be at most {MAX_TOPIC_NAME_LENGTH}."
        )
        raise SetupError(msg)

    if illegal := sorted(set(_ILLEGAL_CHARS.findall(topic))):
        spelled = ", ".join(repr(char) for char in illegal)
        msg = (
            f"Kafka topic {topic!r} contains {spelled}. A topic name admits "
            "ASCII alphanumerics, '.', '_' and '-' only."
        )
        raise SetupError(msg)


_RESERVED_NAMES = {".", ".."}
_ILLEGAL_CHARS = re.compile(r"[^a-zA-Z0-9._-]")
