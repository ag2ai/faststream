from docs.docs_src.sqs.index.basic import test_basic
from docs.docs_src.sqs.index.localstack import test_localstack
from docs.docs_src.sqs.index.testing import test_handler

__all__ = ["test_basic", "test_handler", "test_localstack"]
