from collections.abc import Awaitable, Sequence
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
    cast,
)

from faststream.exceptions import SetupError

from .specified import SpecificationBatchPublisher, SpecificationDefaultPublisher

if TYPE_CHECKING:
    from confluent_kafka import Message as ConfluentMsg

    from faststream._internal.types import BrokerMiddleware, PublisherMiddleware


def create_publisher(
    *,
    autoflush: bool,
    batch: bool,
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[dict[str, str]],
    reply_to: str,
    # Publisher args
    broker_middlewares: Union[
        Sequence["BrokerMiddleware[ConfluentMsg]"],
        Sequence["BrokerMiddleware[tuple[ConfluentMsg, ...]]"],
    ],
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationBatchPublisher",
    "SpecificationDefaultPublisher",
]:
    if batch:
        if key:
            msg = "You can't setup `key` with batch publisher"
            raise SetupError(msg)

        publisher: Union[
            SpecificationBatchPublisher,
            SpecificationDefaultPublisher,
        ] = SpecificationBatchPublisher(
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=cast(
                "Sequence[BrokerMiddleware[tuple[ConfluentMsg, ...]]]",
                broker_middlewares,
            ),
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
        publish_method = "_basic_publish_batch"

    else:
        publisher = SpecificationDefaultPublisher(
            key=key,
            # basic args
            topic=topic,
            partition=partition,
            headers=headers,
            reply_to=reply_to,
            broker_middlewares=cast(
                "Sequence[BrokerMiddleware[ConfluentMsg]]",
                broker_middlewares,
            ),
            middlewares=middlewares,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
        publish_method = "_basic_publish"

    if autoflush:
        default_publish: Callable[..., Awaitable[Optional[Any]]] = getattr(
            publisher, publish_method
        )

        @wraps(default_publish)
        async def autoflush_wrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
            result = await default_publish(*args, **kwargs)
            await publisher.flush()
            return result

        setattr(publisher, publish_method, autoflush_wrapper)

    return publisher
