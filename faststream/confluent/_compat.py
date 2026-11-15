from importlib.metadata import version

_CONFLUENT_VERSION = version("confluent-kafka")

major, minor, *_ = _CONFLUENT_VERSION.split(".")

_CONFLUENT_MAJOR, _CONFLUENT_MINOR = int(major), int(minor)

# `group.protocol` (KIP-848) appeared in confluent-kafka 2.4.0
CONFLUENT_V240 = (_CONFLUENT_MAJOR, _CONFLUENT_MINOR) >= (2, 4)
