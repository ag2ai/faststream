from importlib.metadata import version

_REDIS_VERSION = version("redis")

major, minor, patch, *_ = _REDIS_VERSION.split(".")

_REDIS_MAJOR, _REDIS_MINOR = int(major), int(minor)

REDIS_V720 = (_REDIS_MAJOR, _REDIS_MINOR) >= (7, 2)
REDIS_V800 = (_REDIS_MAJOR, _REDIS_MINOR) >= (8, 0)
