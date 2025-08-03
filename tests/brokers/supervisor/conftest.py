from types import MethodType
from unittest.mock import Mock

import pytest

from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.supervisor import (
    SUPERVISOR_DISABLING_ENV_NAME,
)


@pytest.fixture()
def subscriber_with_task_mixin():
    mock = Mock(spec=TasksMixin)
    mock.tasks = []
    mock.add_task = MethodType(TasksMixin.add_task, mock)
    return mock


@pytest.fixture(autouse=True)
def disable_supervisor(monkeypatch):
    monkeypatch.setenv(SUPERVISOR_DISABLING_ENV_NAME, "0")
