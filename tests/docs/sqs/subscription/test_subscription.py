from docs.docs_src.sqs.subscription.basic import test_subscription
from docs.docs_src.sqs.subscription.concurrency import test_concurrency
from docs.docs_src.sqs.subscription.declared_queue import test_declared_queue
from docs.docs_src.sqs.subscription.long_running import test_long_running
from docs.docs_src.sqs.subscription.message_info import test_message_info

__all__ = [
    "test_concurrency",
    "test_declared_queue",
    "test_long_running",
    "test_message_info",
    "test_subscription",
]
