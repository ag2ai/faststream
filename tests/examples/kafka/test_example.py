import pytest

from examples.kafka.testing import test_handle

pytestmark = pytest.mark.kafka

__all__ = ("test_handle",)
