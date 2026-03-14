from faststream._internal.testing.app import TestApp

try:
    from .annotations import SqlaMessage
    from .broker import SqlaBroker, SqlaPublisher, SqlaRoute, SqlaRouter
    from .response import SqlaPublishCommand

except ImportError as e:
    if "'sqlalchemy'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_SQLA

    raise ImportError(INSTALL_FASTSTREAM_SQLA) from e

__all__ = (
    "SqlaBroker",
    "SqlaMessage",
    "SqlaPublishCommand",
    "SqlaPublisher",
    "SqlaRoute",
    "SqlaRouter",
    "TestApp",
)
