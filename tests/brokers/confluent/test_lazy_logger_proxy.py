import logging
import logging.handlers

import pytest

from faststream._internal.logger.logger_proxy import RealLoggerObject
from faststream._internal.logger.state import LoggerState
from faststream.confluent.helpers.client import _LazyLoggerProxy


@pytest.mark.confluent()
def test_proxy_drops_records_before_setup() -> None:
    """_LazyLoggerProxy must silently drop records while LoggerState is not set up."""
    state = LoggerState()
    proxy = _LazyLoggerProxy(state)

    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname="",
        lineno=0,
        msg="should be dropped",
        args=(),
        exc_info=None,
    )
    # Must not raise
    proxy.handle(record)


@pytest.mark.confluent()
def test_proxy_forwards_after_setup() -> None:
    """After LoggerState is bootstrapped, _LazyLoggerProxy must forward to the real logger."""
    state = LoggerState()
    proxy = _LazyLoggerProxy(state)

    real_logger = logging.getLogger("test.lazy_proxy")
    real_logger.handlers.clear()
    handler = logging.handlers.MemoryHandler(capacity=100)
    real_logger.addHandler(handler)
    real_logger.setLevel(logging.DEBUG)

    # Simulate bootstrapping: set the real logger on the state
    state.logger = RealLoggerObject(real_logger)

    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname="",
        lineno=0,
        msg="should arrive",
        args=(),
        exc_info=None,
    )
    proxy.handle(record)

    assert len(handler.buffer) == 1
    assert handler.buffer[0].msg == "should arrive"

    real_logger.removeHandler(handler)
