import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

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
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.nats.subscriber.specified import (
    SpecificationBatchPullStreamSubscriber,
    SpecificationConcurrentCoreSubscriber,
    SpecificationConcurrentPullStreamSubscriber,
    SpecificationConcurrentPushStreamSubscriber,
    SpecificationCoreSubscriber,
    SpecificationKeyValueWatchSubscriber,
    SpecificationObjStoreWatchSubscriber,
    SpecificationPullStreamSubscriber,
    SpecificationPushStreamSubscriber,
)

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from nats.js import api

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
    from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub


def create_subscriber(
    *,
    subject: str,
    queue: str,
    pending_msgs_limit: Optional[int],
    pending_bytes_limit: Optional[int],
    # Core args
    max_msgs: int,
    # JS args
    durable: Optional[str],
    config: Optional["api.ConsumerConfig"],
    ordered_consumer: bool,
    idle_heartbeat: Optional[float],
    flow_control: Optional[bool],
    deliver_policy: Optional["api.DeliverPolicy"],
    headers_only: Optional[bool],
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
    broker_dependencies: Iterable["Dependant"],
    broker_middlewares: Sequence["BrokerMiddleware[Any]"],
    # Specification information
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationCoreSubscriber",
    "SpecificationConcurrentCoreSubscriber",
    "SpecificationPushStreamSubscriber",
    "SpecificationConcurrentPushStreamSubscriber",
    "SpecificationPullStreamSubscriber",
    "SpecificationConcurrentPullStreamSubscriber",
    "SpecificationBatchPullStreamSubscriber",
    "SpecificationKeyValueWatchSubscriber",
    "SpecificationObjStoreWatchSubscriber",
]:
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

    if ack_first is not EMPTY:
        ack_policy = AckPolicy.ACK_FIRST if ack_first else AckPolicy.REJECT_ON_ERROR

    if no_ack is not EMPTY:
        no_ack = AckPolicy.DO_NOTHING if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.REJECT_ON_ERROR

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
        extra_options: AnyDict = {
            "pending_msgs_limit": pending_msgs_limit
            or DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
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
            if ack_policy is AckPolicy.ACK_FIRST:
                manual_ack = False
                ack_policy = AckPolicy.DO_NOTHING
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
            "pending_bytes_limit": pending_bytes_limit
            or DEFAULT_SUB_PENDING_BYTES_LIMIT,
            "max_msgs": max_msgs,
        }

    if obj_watch is not None:
        return SpecificationObjStoreWatchSubscriber(
            subject=subject,
            config=config,
            obj_watch=obj_watch,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if kv_watch is not None:
        return SpecificationKeyValueWatchSubscriber(
            subject=subject,
            config=config,
            kv_watch=kv_watch,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if stream is None:
        if max_workers > 1:
            return SpecificationConcurrentCoreSubscriber(
                max_workers=max_workers,
                subject=subject,
                config=config,
                queue=queue,
                # basic args
                extra_options=extra_options,
                # Subscriber args
                no_reply=no_reply,
                ack_policy=ack_policy,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # Specification
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )

        return SpecificationCoreSubscriber(
            subject=subject,
            config=config,
            queue=queue,
            # basic args
            extra_options=extra_options,
            # Subscriber args
            no_reply=no_reply,
            ack_policy=ack_policy,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # Specification
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if max_workers > 1:
        if pull_sub is not None:
            return SpecificationConcurrentPullStreamSubscriber(
                max_workers=max_workers,
                pull_sub=pull_sub,
                stream=stream,
                subject=subject,
                config=config,
                # basic args
                extra_options=extra_options,
                # Subscriber args
                ack_policy=ack_policy,
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # Specification
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )

        return SpecificationConcurrentPushStreamSubscriber(
            max_workers=max_workers,
            stream=stream,
            subject=subject,
            config=config,
            queue=queue,
            # basic args
            extra_options=extra_options,
            # Subscriber args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # Specification
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    if pull_sub is not None:
        if pull_sub.batch:
            return SpecificationBatchPullStreamSubscriber(
                pull_sub=pull_sub,
                stream=stream,
                subject=subject,
                config=config,
                # basic args
                extra_options=extra_options,
                # Subscriber args
                ack_policy=ack_policy,
                no_reply=no_reply,
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                # Specification
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )

        return SpecificationPullStreamSubscriber(
            pull_sub=pull_sub,
            stream=stream,
            subject=subject,
            config=config,
            # basic args
            extra_options=extra_options,
            # Subscriber args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            # Specification
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    return SpecificationPushStreamSubscriber(
        stream=stream,
        subject=subject,
        queue=queue,
        config=config,
        # basic args
        extra_options=extra_options,
        # Subscriber args
        ack_policy=ack_policy,
        no_reply=no_reply,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        # Specification information
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )


def _validate_input_for_misconfigure(  # noqa: PLR0915
    subject: str,
    queue: str,  # default ""
    pending_msgs_limit: Optional[int],
    pending_bytes_limit: Optional[int],
    max_msgs: int,  # default 0
    durable: Optional[str],
    config: Optional["api.ConsumerConfig"],
    ordered_consumer: bool,  # default False
    idle_heartbeat: Optional[float],
    flow_control: Optional[bool],
    deliver_policy: Optional["api.DeliverPolicy"],
    headers_only: Optional[bool],
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

        elif stream is None and ack_policy is not AckPolicy.DO_NOTHING:
            warnings.warn(
                (
                    "Core subscriber supports only `ack_policy=AckPolicy.DO_NOTHING` option for very specific cases. "
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
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.DO_NOTHING`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        no_ack = AckPolicy.DO_NOTHING if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.REJECT_ON_ERROR

    if not subject and not config:
        msg = "You must provide either the `subject` or `config` option."
        raise SetupError(msg)

    if stream and kv_watch:
        msg = "You can't use both the `stream` and `kv_watch` options simultaneously."
        raise SetupError(msg)

    if stream and obj_watch:
        msg = "You can't use both the `stream` and `obj_watch` options simultaneously."
        raise SetupError(msg)

    if kv_watch and obj_watch:
        msg = (
            "You can't use both the `kv_watch` and `obj_watch` options simultaneously."
        )
        raise SetupError(msg)

    if pull_sub and not stream:
        msg = "JetStream Pull Subscriber can only be used with the `stream` option."
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
            message="The JetStream Push consumer with the `durable` option can't be scaled horizontally across multiple instances. You probably wanted to use the `queue` option instead. Also, we strongly recommend using the Jetstream PullSubsriber with the `durable` option as the default.",
            category=RuntimeWarning,
            stacklevel=4,
        )
