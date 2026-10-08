import pytest

from docs.docs_src.kafka.raw_publish.example import test_raw_publish

pytestmark = pytest.mark.kafka

__all__ = ["test_raw_publish"]
