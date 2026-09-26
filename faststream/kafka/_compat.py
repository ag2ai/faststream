from importlib.metadata import version

from faststream.exceptions import SetupError

_AIOKAFKA_VERSION = version("aiokafka")

major, minor, *_ = _AIOKAFKA_VERSION.split(".")

_AIOKAFKA_MAJOR, _AIOKAFKA_MINOR = int(major), int(minor)

# 0.13.0 removed `api_version` from every client; 0.14.0 accepts it again as a no-op
AIOKAFKA_V013 = (_AIOKAFKA_MAJOR, _AIOKAFKA_MINOR) >= (0, 13)

# `AIOKafkaConsumer(client_rack=...)` appeared in aiokafka 0.14.0
AIOKAFKA_V014 = (_AIOKAFKA_MAJOR, _AIOKAFKA_MINOR) >= (0, 14)


def validate_client_rack(client_rack: str | None) -> None:
    if client_rack is not None and not AIOKAFKA_V014:
        msg = (
            "`client_rack` requires aiokafka 0.14.0 or newer "
            f"(installed: {_AIOKAFKA_VERSION})"
        )
        raise SetupError(msg)
