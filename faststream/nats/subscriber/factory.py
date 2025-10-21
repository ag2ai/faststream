import warnings
from typing import TYPE_CHECKING, Any, Optional, TypedDict

from nats.aio.subscription import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
)
from nats.js.api import ConsumerConfig, DeliverPolicy
from nats.js.client import (
    DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
)

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

from .config import NatsSubscriberConfig, NatsSubscriberSpecificationConfig
from .specification import NatsSubscriberSpecification, NotIncludeSpecifation
from .usecases import (
    BatchPullStreamSubscriber,
    ConcurrentCoreSubscriber,
    ConcurrentPullStreamSubscriber,
    ConcurrentPushStreamSubscriber,
    CoreSubscriber,
    KeyValueWatchSubscriber,
    LogicSubscriber,
    ObjStoreWatchSubscriber,
    PullStreamSubscriber,
    PushStreamSubscriber,
)

if TYPE_CHECKING:
    from nats.js import api

    from faststream.nats.configs import NatsBrokerConfig
    from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub


class SharedOptions(TypedDict):
    config: NatsSubscriberConfig
    specification: SubscriberSpecification[Any, Any]
    calls: CallsCollection[Any]


def create_subscriber(
    *,
    subject: str,
    queue: str,
    pending_msgs_limit: int | None,
    pending_bytes_limit: int | None,
    # Core args
    max_msgs: int,
    # JS args
    durable: str | None,
    config: Optional["api.ConsumerConfig"],
    ordered_consumer: bool,
    idle_heartbeat: float | None,
    flow_control: bool | None,
    deliver_policy: Optional["api.DeliverPolicy"],
    headers_only: bool | None,
    # pull args
    pull_sub: Optional["PullSub"],
    kv_watch: Optional["KvWatch"],
    obj_watch: Optional["ObjWatch"],
    inbox_prefix: bytes,
    # custom args
    ack_first: bool,
    max_workers: int,
    stream: Optional["JStream"],
    # Subscriber args
    ack_policy: "AckPolicy",
    no_ack: bool,
    no_reply: bool,
    broker_config: "NatsBrokerConfig",
    # Specification information
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> "LogicSubscriber[Any]":
    _validate_input_for_misconfigure(
        subject=subject,
        queue=queue,
        pending_msgs_limit=pending_msgs_limit,
        pending_bytes_limit=pending_bytes_limit,
        max_msgs=max_msgs,
        durable=durable,
        config=config,
        ordered_consumer=ordered_consumer,
        idle_heartbeat=idle_heartbeat,
        flow_control=flow_control,
        deliver_policy=deliver_policy,
        headers_only=headers_only,
        pull_sub=pull_sub,
        ack_policy=ack_policy,
        no_ack=no_ack,
        kv_watch=kv_watch,
        obj_watch=obj_watch,
        ack_first=ack_first,
        max_workers=max_workers,
        stream=stream,
    )

    config = config or ConsumerConfig(filter_subjects=[])
    if config.durable_name is None:
        config.durable_name = durable
    if config.idle_heartbeat is None:
        config.idle_heartbeat = idle_heartbeat
    if config.headers_only is None:
        config.headers_only = headers_only
    if config.deliver_policy is DeliverPolicy.ALL:
        config.deliver_policy = deliver_policy or DeliverPolicy.ALL

    if stream:
        # Both JS Subscribers
        extra_options: dict[str, Any] = {
            "pending_msgs_limit": pending_msgs_limit or DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
            "pending_bytes_limit": pending_bytes_limit
            or DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
            "durable": durable,
            "stream": stream.name,
        }

        if pull_sub is not None:
            # JS Pull Subscriber
            extra_options.update({"inbox_prefix": inbox_prefix})

        else:
            # JS Push Subscriber
            if ack_first or ack_policy is AckPolicy.ACK_FIRST:
                manual_ack = False
                ack_policy = AckPolicy.MANUAL
            else:
                manual_ack = True

            extra_options.update(
                {
                    "ordered_consumer": ordered_consumer,
                    "idle_heartbeat": idle_heartbeat,
                    "flow_control": flow_control,
                    "deliver_policy": deliver_policy,
                    "headers_only": headers_only,
                    "manual_ack": manual_ack,
                },
            )

    else:
        # Core Subscriber
        extra_options = {
            "pending_msgs_limit": pending_msgs_limit or DEFAULT_SUB_PENDING_MSGS_LIMIT,
            "pending_bytes_limit": pending_bytes_limit or DEFAULT_SUB_PENDING_BYTES_LIMIT,
            "max_msgs": max_msgs,
        }

    subscriber_config = NatsSubscriberConfig(
        subject=subject,
        sub_config=config,
        extra_options=extra_options,
        no_reply=no_reply,
        _outer_config=broker_config,
        _ack_first=ack_first,
        _ack_policy=ack_policy,
        _no_ack=no_ack,
    )

    calls = CallsCollection[Any]()

    specification_config = NatsSubscriberSpecificationConfig(
        subject=subject,
        queue=queue or None,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    specification = NatsSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=specification_config,
    )

    not_include_spec = NotIncludeSpecifation(
        _outer_config=broker_config,
        calls=calls,
        specification_config=specification_config,
    )

    subscriber_options: SharedOptions = {
        "config": subscriber_config,
        "specification": specification,
        "calls": calls,
    }

    if obj_watch is not None:
        return ObjStoreWatchSubscriber(
            **(subscriber_options | {"specification": not_include_spec}),
            obj_watch=obj_watch,
        )

    if kv_watch is not None:
        return KeyValueWatchSubscriber(
            **(subscriber_options | {"specification": not_include_spec}),
            kv_watch=kv_watch,
        )

    if stream is None:
        if max_workers > 1:
            return ConcurrentCoreSubscriber(
                **subscriber_options,
                max_workers=max_workers,
                queue=queue,
            )

        return CoreSubscriber(
            **subscriber_options,
            queue=queue,
        )

    if max_workers > 1:
        if pull_sub is not None:
            return ConcurrentPullStreamSubscriber(
                **subscriber_options,
                max_workers=max_workers,
                queue=queue,
                stream=stream,
                pull_sub=pull_sub,
            )

        return ConcurrentPushStreamSubscriber(
            **subscriber_options,
            max_workers=max_workers,
            queue=queue,
            stream=stream,
        )

    if pull_sub is not None:
        if pull_sub.batch:
            return BatchPullStreamSubscriber(
                **subscriber_options,
                pull_sub=pull_sub,
                stream=stream,
            )

        return PullStreamSubscriber(
            **subscriber_options,
            queue=queue,
            pull_sub=pull_sub,
            stream=stream,
        )

    return PushStreamSubscriber(
        **subscriber_options,
        queue=queue,
        stream=stream,
    )


def _validate_input_for_misconfigure(  # noqa: PLR0915
    subject: str,
    queue: str,  # default ""
    pending_msgs_limit: int | None,
    pending_bytes_limit: int | None,
    max_msgs: int,  # default 0
    durable: str | None,
    config: Optional["api.ConsumerConfig"],
    ordered_consumer: bool,  # default False
    idle_heartbeat: float | None,
    flow_control: bool | None,
    deliver_policy: Optional["api.DeliverPolicy"],
    headers_only: bool | None,
    pull_sub: Optional["PullSub"],
    kv_watch: Optional["KvWatch"],
    obj_watch: Optional["ObjWatch"],
    ack_policy: "AckPolicy",  # default EMPTY
    no_ack: bool,  # default EMPTY
    ack_first: bool,  # default EMPTY
    max_workers: int,  # default 1
    stream: Optional["JStream"],
) -> None:
    if ack_policy is not EMPTY:
        if obj_watch is not None:
            warnings.warn(
                "You can't use acknowledgement policy with ObjectStorage watch subscriber.",
                RuntimeWarning,
                stacklevel=4,
            )

        elif kv_watch is not None:
            warnings.warn(
                "You can't use acknowledgement policy with KeyValue watch subscriber.",
                RuntimeWarning,
                stacklevel=4,
            )

        elif stream is None and ack_policy is not AckPolicy.MANUAL:
            warnings.warn(
                (
                    "Core subscriber supports only `ack_policy=AckPolicy.MANUAL` option for very specific cases. "
                    "If you are using different option, probably, you should use JetStream Subscriber instead."
                ),
                RuntimeWarning,
                stacklevel=4,
            )

        if max_msgs > 0 and any((stream, kv_watch, obj_watch)):
            warnings.warn(
                "The `max_msgs` option can be used only with a NATS Core Subscriber.",
                RuntimeWarning,
                stacklevel=4,
            )

    if ack_first is not EMPTY:
        warnings.warn(
            "`ack_first` option was deprecated in prior to `ack_policy=AckPolicy.ACK_FIRST`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `ack_first` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.ACK_FIRST if ack_first else AckPolicy.REJECT_ON_ERROR

    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.MANUAL`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.MANUAL if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.REJECT_ON_ERROR

    if stream and kv_watch:
        msg = "You can't use both the `stream` and `kv_watch` options simultaneously."
        raise SetupError(msg)

    if stream and obj_watch:
        msg = "You can't use both the `stream` and `obj_watch` options simultaneously."
        raise SetupError(msg)

    if kv_watch and obj_watch:
        msg = "You can't use both the `kv_watch` and `obj_watch` options simultaneously."
        raise SetupError(msg)

    if pull_sub and not stream:
        msg = "JetStream Pull Subscriber can only be used with the `stream` option."
        raise SetupError(msg)

    if not subject and not config:
        msg = "You must provide either the `subject` or `config` option."
        raise SetupError(msg)

    if not stream:
        if obj_watch or kv_watch:
            # Obj/Kv Subscriber
            if pending_msgs_limit is not None:
                warnings.warn(
                    message="The `pending_msgs_limit` option can be used only with JetStream (Pull/Push) or Core Subscription.",
                    category=RuntimeWarning,
                    stacklevel=4,
                )

            if pending_bytes_limit is not None:
                warnings.warn(
                    message="The `pending_bytes_limit` option can be used only with JetStream (Pull/Push) or Core Subscription.",
                    category=RuntimeWarning,
                    stacklevel=4,
                )

            if queue:
                warnings.warn(
                    message="The `queue` option can be used only with JetStream Push or Core Subscription.",
                    category=RuntimeWarning,
                    stacklevel=4,
                )

            if max_workers > 1:
                warnings.warn(
                    message="The `max_workers` option can be used only with JetStream (Pull/Push) or Core Subscription.",
                    category=RuntimeWarning,
                    stacklevel=4,
                )

        # Core/Obj/Kv Subscriber
        if durable:
            warnings.warn(
                message="The `durable` option can be used only with JetStream (Pull/Push) Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if config is not None:
            warnings.warn(
                message="The `config` option can be used only with JetStream (Pull/Push) Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if ordered_consumer:
            warnings.warn(
                message="The `ordered_consumer` option can be used only with JetStream (Pull/Push) Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if idle_heartbeat is not None:
            warnings.warn(
                message="The `idle_heartbeat` option can be used only with JetStream (Pull/Push) Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if flow_control:
            warnings.warn(
                message="The `flow_control` option can be used only with JetStream Push Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if deliver_policy:
            warnings.warn(
                message="The `deliver_policy` option can be used only with JetStream (Pull/Push) Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if headers_only:
            warnings.warn(
                message="The `headers_only` option can be used only with JetStream (Pull/Push) Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if ack_policy is AckPolicy.ACK_FIRST:
            warnings.warn(
                message="The `ack_policy=AckPolicy.ACK_FIRST:` option can be used only with JetStream Push Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

    # JetStream Subscribers
    elif pull_sub:
        if queue:
            warnings.warn(
                message="The `queue` option has no effect with JetStream Pull Subscription. You probably wanted to use the `durable` option instead.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if ordered_consumer:
            warnings.warn(
                "The `ordered_consumer` option has no effect with JetStream Pull Subscription. It can only be used with JetStream Push Subscription.",
                RuntimeWarning,
                stacklevel=4,
            )

        if ack_policy is AckPolicy.ACK_FIRST:
            warnings.warn(
                message="The `ack_policy=AckPolicy.ACK_FIRST` option has no effect with JetStream Pull Subscription. It can only be used with JetStream Push Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

        if flow_control:
            warnings.warn(
                message="The `flow_control` option has no effect with JetStream Pull Subscription. It can only be used with JetStream Push Subscription.",
                category=RuntimeWarning,
                stacklevel=4,
            )

    # JS PushSub
    elif durable is not None:
        warnings.warn(
            message="The JetStream Push consumer with the `durable` option can't be scaled horizontally across multiple instances. You probably wanted to use the `queue` option instead. Also, we strongly recommend using the Jetstream PullSubscriber with the `durable` option as the default.",
            category=RuntimeWarning,
            stacklevel=4,
        )
