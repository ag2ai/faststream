from types import MethodType
from unittest.mock import Mock

import pytest

from faststream._internal.endpoint.subscriber.mixins import TasksMixin


@pytest.fixture()
def subscriber_with_task_mixin():
    mock = Mock(spec=TasksMixin)
    mock._outer_config = Mock()
    mock.tasks = []
    mock.add_task = MethodType(TasksMixin.add_task, mock)

    return mock


@pytest.fixture(autouse=True)
def disable_supervisor(monkeypatch):
    monkeypatch.setenv("FASTSTREAM_SUPERVISOR_DISABLED", "0")
