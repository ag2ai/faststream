from typing import TYPE_CHECKING, Any, Union

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.sqs.schemas import SQSQueue

from .config import SQSSubscriberConfig, SQSSubscriberSpecificationConfig
from .specification import SQSSubscriberSpecification
from .usecase import ConcurrentSQSSubscriber, SQSSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy
    from faststream.sqs.configs import SQSBrokerConfig


def create_subscriber(
    *,
    queue: Union[str, "SQSQueue"],
    wait_time_seconds: int,
    max_messages: int,
    visibility_timeout: int | None,
    batch: bool,
    request_attempt_id: str | None,
    max_workers: int,
    extend_visibility: bool,
    # Subscriber args
    ack_policy: "AckPolicy",
    no_reply: bool,
    config: "SQSBrokerConfig",
    # AsyncAPI args
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> SQSSubscriber:
    queue_obj = queue if isinstance(queue, SQSQueue) else SQSQueue(name=queue)
    queue_name = queue_obj.queue_name

    # ReceiveRequestAttemptId is only accepted by SQS for FIFO queues; reject it
    # early instead of letting SQS fail the receive call silently.
    if request_attempt_id is not None and not queue_name.endswith(".fifo"):
        msg = (
            f"`request_attempt_id` (ReceiveRequestAttemptId) is only valid for FIFO "
            f"queues, but '{queue_name}' is not a FIFO queue. Use `FifoQueue(...)` "
            "or a '.fifo' queue name."
        )
        raise SetupError(msg)

    if max_workers > 1 and batch:
        msg = (
            "Can't combine `max_workers` with `batch=True` — a batch handler "
            "already receives the whole poll (up to `max_messages` messages)."
        )
        raise SetupError(msg)

    if max_workers > 1 and queue_name.endswith(".fifo"):
        msg = (
            "`max_workers` can't be used with FIFO queues: concurrent processing "
            "would break message-group ordering."
        )
        raise SetupError(msg)

    if extend_visibility and visibility_timeout is None:
        msg = (
            "`extend_visibility=True` requires an explicit `visibility_timeout` "
            "so the heartbeat knows how far to extend it."
        )
        raise SetupError(msg)

    subscriber_config = SQSSubscriberConfig(
        queue=queue_name,
        declare=queue_obj,
        wait_time_seconds=wait_time_seconds,
        max_messages=max_messages,
        visibility_timeout=visibility_timeout,
        batch=batch,
        request_attempt_id=request_attempt_id,
        extend_visibility=extend_visibility,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    specification_config = SQSSubscriberSpecificationConfig(
        queue=queue_name,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    calls = CallsCollection[Any]()

    specification = SQSSubscriberSpecification(
        config,
        specification_config,
        calls,
    )

    if max_workers > 1:
        return ConcurrentSQSSubscriber(
            subscriber_config,
            specification,
            calls,
            max_workers=max_workers,
        )

    return SQSSubscriber(subscriber_config, specification, calls)
