import pytest

from docs.docs_src.kafka.publisher_object.example import test_prepared_publish

pytestmark = pytest.mark.kafka

__all__ = ["test_prepared_publish"]
