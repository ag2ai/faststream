from types import MethodType
from unittest.mock import Mock

import pytest

from faststream.broker.subscriber.mixins import TasksMixin
from faststream.utils.functions import sync_fake_context


@pytest.fixture
def subscriber_with_task_mixin():
    mock = Mock(spec=TasksMixin)
    mock.tasks = set()
    mock.lock = sync_fake_context()
    mock.add_task = MethodType(TasksMixin.add_task, mock)
    return mock
