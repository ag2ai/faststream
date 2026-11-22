import pytest

from docs.docs_src.redis.pub_sub.raw_publish import test_raw_publish

pytestmark = pytest.mark.redis

__all__ = ("test_raw_publish",)
