from collections.abc import Iterator
from typing import Any
from unittest.mock import Mock

import pytest

from faststream._internal.endpoint.subscriber.supervisor import (
    TaskCallbackSupervisor,
    TasksSupervisor,
)


@pytest.fixture()
def subscriber_with_task_mixin() -> TasksSupervisor:
    return TasksSupervisor(Mock())


@pytest.fixture(autouse=True)
def disable_supervisor(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    # mypy does not resolve name-mangled attributes
    supervisor: Any = TaskCallbackSupervisor
    cache = supervisor._TaskCallbackSupervisor__cache
    cache.clear()
    monkeypatch.setenv("FASTSTREAM_SUPERVISOR_DISABLED", "0")
    yield
    cache.clear()
