from collections import UserList
from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional

from nats.js.api import DiscardPolicy, StreamConfig

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
    )

    def __init__(
        self,
        name: str,
        description: str | None = None,
        subjects: list[str] | None = None,
        retention: Optional["RetentionPolicy"] = None,
        max_consumers: int | None = None,
        max_msgs: int | None = None,
        max_bytes: int | None = None,
        discard: Optional["DiscardPolicy"] = DiscardPolicy.OLD,
        max_age: float | None = None,  # in seconds
        max_msgs_per_subject: int = -1,
        max_msg_size: int | None = -1,
        storage: Optional["StorageType"] = None,
        num_replicas: int | None = None,
        no_ack: bool = False,
        template_owner: str | None = None,
        duplicate_window: float = 0,
        placement: Optional["Placement"] = None,
        mirror: Optional["StreamSource"] = None,
        sources: list["StreamSource"] | None = None,
        sealed: bool = False,
        deny_delete: bool = False,
        deny_purge: bool = False,
        allow_rollup_hdrs: bool = False,
        republish: Optional["RePublish"] = None,
        allow_direct: bool | None = None,
        mirror_direct: bool | None = None,
        allow_msg_schedules: bool | None = None,
        declare: bool = True,
    ) -> None:
        """Initialized JSrream.

        Args:
            name:
                Stream name to work with.
            description:
                Stream description if needed.
            subjects:
                Subjects, used by stream to grab messages from them. Any message sent by NATS Core will be consumed
                by stream. Also, stream acknowledge message publisher with message, sent on reply subject of publisher.
                Can be single string or list of them. Dots separate tokens of subjects, every token may be matched with exact same
                token or wildcards.
            retention:
                Retention policy for stream to use. Default is Limits, which will delete messages only in case of resource depletion,
                if 'DiscardPolicy.OLD' used. In case of 'DiscardPolicy.NEW', stream will answer error for any write request.
                If 'RetentionPolicy.Interest' is used, message will be deleted as soon as all active consumers will consume that message.
                Note: consumers should be bounded to stream! If no consumers bound, all messages will be deleted, including new messages!
                If 'RetentionPolicy.WorkQueue' is used, you will be able to bound only one consumer to the stream, which guarantees
                message to be consumed only once. Since message acked, it will be deleted from the stream immediately.
                Note: Message will be deleted only if limit is reached or message acked successfully. Message that reached MaxDelivery limit will remain in the stream and should be manually deleted!
                Note: All policies will be responsive to Limits.
            max_consumers:
                Max number of consumers to be bound with this stream.
            max_msgs:
                Max number of messages to be stored in the stream. Stream can automatically delete old messages or stop receiving new messages, look for 'DiscardPolicy'.
            max_bytes:
                Max bytes of all messages to be stored in the stream. Stream can automatically delete old messages or stop receiving new messages, look for 'DiscardPolicy'.
            discard:
                Determines stream behavior on messages in case of retention exceeds.
            max_age:
                TTL in seconds for messages. Since message arrive, TTL begun. As soon as TTL exceeds, message will be deleted.
            max_msgs_per_subject:
                Limit message count per every unique subject. Stream index subjects to it's pretty fast tho.
            max_msg_size:
                Limit message size to be received. Note: the whole message can't be larger than NATS Core message limit.
            storage:
                Storage type, disk or memory. Disk is more durable, memory is faster. Memory can be better choice for systems,
                where new value overrides previous.
            num_replicas:
                Replicas of stream to be used. All replicas create RAFT group with leader. In case of losing lesser than half, cluster will be available to reads and writes.
                In case of losing slightly more than half, cluster may be available but for reads only.
            no_ack:
                Should stream acknowledge writes or not. Without acks publisher can't determine, does message received by stream or not.
            template_owner:
                A TTL for keys in implicit TTL-based hashmap of stream. That hashmap allows to early drop duplicate
                messages. Essential feature for idempotent writes. Note: disabled by default. Look for 'Nats-Msg-Id'
                in NATS documentation for more information.
            duplicate_window:
                A TTL for keys in implicit TTL-based hashmap of stream. That hashmap allows to early drop duplicate messages. Essential feature for idempotent writes.
                Note: disabled by default. Look for 'Nats-Msg-Id' in NATS documentation for more information.
            placement:
                NATS Cluster for stream to be deployed in. Value is name of that cluster.
            mirror:
                Should stream be read-only replica of another stream, if so, value is name of that stream.
            sources:
                Should stream mux multiple streams into single one, if so, values is names of those streams.
            sealed:
                Is stream sealed, which means read-only locked.
            deny_delete:
                Should delete command be blocked.
            deny_purge:
                Should purge command be blocked.
            allow_rollup_hdrs:
                Should rollup headers be blocked.
            republish:
                Should be messages, received by stream, send to additional subject.
            allow_direct:
                Should direct requests be allowed. Note: you can get stale data.
            mirror_direct:
                Should direct mirror requests be allowed
            allow_msg_schedules:
                Should allow message schedules.
            declare:
                Whether to create stream automatically or just connect to it.
        """
        super().__init__(name)

        self.declare = declare
        self.subjects = SubjectsCollection(subjects)

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
            allow_msg_schedules=allow_msg_schedules,
            subjects=[],  # use subjects from builder in declaration
        )


class SubjectsCollection(UserList[str]):
    def __init__(self, initlist: Iterable[str] | None = None, /) -> None:
        super().__init__(())
        self.extend(initlist or ())

    def extend(self, subjects: Iterable[str], /) -> None:
        for subj in subjects:
            self.append(subj)

    def append(self, subject: str, /) -> None:
        _, subject = compile_nats_wildcard(subject)

        new_subjects = []
        for old_subject in self.data:
            if is_subject_match_wildcard(subject, old_subject):
                return

            if not is_subject_match_wildcard(old_subject, subject):
                new_subjects.append(old_subject)

        new_subjects.append(subject)
        self.data = new_subjects


def is_subject_match_wildcard(subject: str, pattern: str) -> bool:
    subject_parts = subject.split(".")
    pattern_parts = pattern.split(".")

    for subject_part, pattern_part in zip(
        subject_parts,
        pattern_parts,
        strict=False,
    ):
        if pattern_part == ">":
            return True

        if pattern_part == "*":
            if subject_part == ">":
                return False

        elif subject_part != pattern_part:
            return False

    return len(subject_parts) == len(pattern_parts)


def compile_nats_wildcard(pattern: str) -> tuple[Optional["Pattern[str]"], str]:
    """Compile `logs.{user}.>` to regex and `logs.*.>` subject."""
    return compile_path(
        pattern,
        replace_symbol="*",
        patch_regex=lambda x: x.replace(".>", "..+"),
    )
