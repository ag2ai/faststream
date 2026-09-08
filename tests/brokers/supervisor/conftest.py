from types import MethodType
from unittest.mock import Mock

import pytest

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.supervisor import TaskCallbackSupervisor


@pytest.fixture()
def subscriber_with_task_mixin():
    mock = Mock(spec=TasksMixin)
    mock._outer_config = Mock()
    mock.tasks = []
    mock.add_task = MethodType(TasksMixin.add_task, mock)

    return mock


@pytest.fixture(autouse=True)
def disable_supervisor(monkeypatch):
    cache = TaskCallbackSupervisor._TaskCallbackSupervisor__cache
    cache.clear()
    monkeypatch.setenv("FASTSTREAM_SUPERVISOR_DISABLED", "0")
    yield
    cache.clear()
