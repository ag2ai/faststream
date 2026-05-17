from uuid import uuid4

import pytest

from tests.brokers.mq.utils import disabled_ibmmq_otel_hooks


@pytest.fixture()
def queue() -> str:
    return f"DEV.Q{uuid4().hex[:20].upper()}"


@pytest.fixture(autouse=True)
def disable_ibmmq_otel_hooks() -> None:
    with disabled_ibmmq_otel_hooks():
        yield
