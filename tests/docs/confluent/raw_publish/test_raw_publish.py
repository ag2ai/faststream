import pytest

from docs.docs_src.confluent.raw_publish.example import test_raw_publish

pytestmark = pytest.mark.confluent

__all__ = ["test_raw_publish"]
