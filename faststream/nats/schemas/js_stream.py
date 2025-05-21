from itertools import zip_longest
from typing import TYPE_CHECKING, Annotated, Optional

from nats.js.api import DiscardPolicy, StreamConfig
from typing_extensions import Doc

from faststream._internal.proto import NameRequired
from faststream._internal.utils.path import compile_path

if TYPE_CHECKING:
    from re import Pattern

    from nats.js.api import (
        Placement,
        RePublish,
        RetentionPolicy,
        StorageType,
        StreamSource,
    )


class JStream(NameRequired):
    """A class to represent a JetStream stream."""

    __slots__ = (
        "config",
        "declare",
        "name",
        "subjects",
    )

    def __init__(
        self,
        name: Annotated[
            str,
            Doc("Stream name to work with."),
        ],
        description: Annotated[
            Optional[str],
            Doc("Stream description if needed."),
        ] = None,
        subjects: Annotated[
            Optional[list[str]],
            Doc(
                "Subjects, used by stream to grab messages from them. Any message sent by NATS Core will be consumed "
                "by stream. Also, stream acknowledge message publisher with message, sent on reply subject of "
                "publisher. Can be single string or list of them. Dots separate tokens of subjects, every token may "
                "be matched with exact same token or wildcards.",
            ),
        ] = None,
        retention: Annotated[
            Optional["RetentionPolicy"],
            Doc(
                "Retention policy for stream to use. Default is Limits, which will delete messages only in case of "
                "resource depletion, if 'DiscardPolicy.OLD' used. In case of 'DisardPolicy.NEW', stream will answer "
                "error for any write request. If 'RetentionPolicy.Interest' is used, message will be deleted as soon "
                "as all active consumers will consume that message. Note: consumers should be bounded to stream! If "
                "no consumers bound, all messages will be deleted, including new messages! If "
                "'RetentionPolicy.WorkQueue' is used, you will be able to bound only one consumer to the stream, "
                "which guarantees message to be consumed only once. Since message acked, it will be deleted from the "
                "stream immediately. Note: Message will be deleted only if limit is reached or message acked "
                "successfully. Message that reached MaxDelivery limit will remain in the stream and should be "
                "manually deleted! Note: All policies will be responsive to Limits.",
            ),
        ] = None,
        max_consumers: Annotated[
            Optional[int],
            Doc("Max number of consumers to be bound with this stream."),
        ] = None,
        max_msgs: Annotated[
            Optional[int],
            Doc(
                "Max number of messages to be stored in the stream. Stream can automatically delete old messages or "
                "stop receiving new messages, look for 'DiscardPolicy'",
            ),
        ] = None,
        max_bytes: Annotated[
            Optional[int],
            Doc(
                "Max bytes of all messages to be stored in the stream. Stream can automatically delete old messages or "
                "stop receiving new messages, look for 'DiscardPolicy'",
            ),
        ] = None,
        discard: Annotated[
            Optional["DiscardPolicy"],
            Doc("Determines stream behavior on messages in case of retention exceeds."),
        ] = DiscardPolicy.OLD,
        max_age: Annotated[
            Optional[float],
            Doc(
                "TTL in seconds for messages. Since message arrive, TTL begun. As soon as TTL exceeds, message will be "
                "deleted.",
            ),
        ] = None,  # in seconds
        max_msgs_per_subject: Annotated[
            int,
            Doc(
                "Limit message count per every unique subject. Stream index subjects to it's pretty fast tho.-",
            ),
        ] = -1,
        max_msg_size: Annotated[
            Optional[int],
            Doc(
                "Limit message size to be received. Note: the whole message can't be larger than NATS Core message "
                "limit.",
            ),
        ] = -1,
        storage: Annotated[
            Optional["StorageType"],
            Doc(
                "Storage type, disk or memory. Disk is more durable, memory is faster. Memory can be better choice "
                "for systems, where new value overrides previous.",
            ),
        ] = None,
        num_replicas: Annotated[
            Optional[int],
            Doc(
                "Replicas of stream to be used. All replicas create RAFT group with leader. In case of losing lesser "
                "than half, cluster will be available to reads and writes. In case of losing slightly more than half, "
                "cluster may be available but for reads only.",
            ),
        ] = None,
        no_ack: Annotated[
            bool,
            Doc(
                "Should stream acknowledge writes or not. Without acks publisher can't determine, does message "
                "received by stream or not."
            ),
        ] = False,
        template_owner: Optional[str] = None,
        duplicate_window: Annotated[
            float,
            Doc(
                "A TTL for keys in implicit TTL-based hashmap of stream. That hashmap allows to early drop duplicate "
                "messages. Essential feature for idempotent writes. Note: disabled by default. Look for 'Nats-Msg-Id' "
                "in NATS documentation for more information.",
            ),
        ] = 0,
        placement: Annotated[
            Optional["Placement"],
            Doc(
                "NATS Cluster for stream to be deployed in. Value is name of that cluster.",
            ),
        ] = None,
        mirror: Annotated[
            Optional["StreamSource"],
            Doc(
                "Should stream be read-only replica of another stream, if so, value is name of that stream.",
            ),
        ] = None,
        sources: Annotated[
            Optional[list["StreamSource"]],
            Doc(
                "Should stream mux multiple streams into single one, if so, values is names of those streams.",
            ),
        ] = None,
        sealed: Annotated[
            bool,
            Doc("Is stream sealed, which means read-only locked."),
        ] = False,
        deny_delete: Annotated[
            bool,
            Doc("Should delete command be blocked."),
        ] = False,
        deny_purge: Annotated[
            bool,
            Doc("Should purge command be blocked."),
        ] = False,
        allow_rollup_hdrs: Annotated[
            bool,
            Doc("Should rollup headers be blocked."),
        ] = False,
        republish: Annotated[
            Optional["RePublish"],
            Doc("Should be messages, received by stream, send to additional subject."),
        ] = None,
        allow_direct: Annotated[
            Optional[bool],
            Doc("Should direct requests be allowed. Note: you can get stale data."),
        ] = None,
        mirror_direct: Annotated[
            Optional[bool],
            Doc("Should direct mirror requests be allowed"),
        ] = None,
        # custom
        declare: Annotated[
            bool,
            Doc("Whether to create stream automatically or just connect to it."),
        ] = True,
    ) -> None:
        super().__init__(name)

        subjects = subjects or []

        self.subjects = subjects
        self.declare = declare

        self.config = StreamConfig(
            name=name,
            description=description,
            retention=retention,
            max_consumers=max_consumers,
            max_msgs=max_msgs,
            max_bytes=max_bytes,
            discard=discard,
            max_age=max_age,
            max_msgs_per_subject=max_msgs_per_subject,
            max_msg_size=max_msg_size,
            storage=storage,
            num_replicas=num_replicas,
            no_ack=no_ack,
            template_owner=template_owner,
            duplicate_window=duplicate_window,
            placement=placement,
            mirror=mirror,
            sources=sources,
            sealed=sealed,
            deny_delete=deny_delete,
            deny_purge=deny_purge,
            allow_rollup_hdrs=allow_rollup_hdrs,
            republish=republish,
            allow_direct=allow_direct,
            mirror_direct=mirror_direct,
            subjects=[],  # use self.subjects in declaration
        )

    def add_subject(self, subject: str) -> None:
        """Add subject to stream params."""
        _, subject = compile_nats_wildcard(subject)

        if not any(is_subject_match_wildcard(subject, x) for x in self.subjects):
            self.subjects.append(subject)


def is_subject_match_wildcard(subject: str, wildcard: str) -> bool:
    """Check is subject suite for the wildcard pattern."""
    if subject == wildcard:
        return True

    call = True

    for current, base in zip_longest(
        subject.split("."),
        wildcard.split("."),
        fillvalue=None,
    ):
        if base == ">":
            break

        if base not in {"*", current}:
            call = False
            break

    return call


def compile_nats_wildcard(pattern: str) -> tuple[Optional["Pattern[str]"], str]:
    return compile_path(
        pattern,
        replace_symbol="*",
        patch_regex=lambda x: x.replace(".>", "..+"),
    )
