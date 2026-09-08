from collections import Counter
from collections.abc import Sequence
from typing import Any

import pytest

from faststream import BaseMiddleware, Context, Response
from faststream.response import ensure_response
from faststream.response.response import (
    BatchPublishCommand,
    PublishCommand,
)


class BasePublishCommandTestcase:
    publish_command_cls: type[PublishCommand]

    def test_simple_response(self) -> None:
        response = ensure_response(1)
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1

    def test_base_response_class(self) -> None:
        response = ensure_response(Response(body=1, headers={"1": 1}))
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1
        assert cmd.headers == {"1": 1}


class BatchPublishCommandTestcase(BasePublishCommandTestcase):
    publish_command_cls: type[BatchPublishCommand]

    @pytest.mark.parametrize(
        ("data", "expected_body"),
        (
            pytest.param(None, (), id="None Response"),
            pytest.param((), (), id="Empty Sequence"),
            pytest.param("123", ("123",), id="String Response"),
            pytest.param("", ("",), id="Empty String Response"),
            pytest.param(b"", (b"",), id="Empty Bytes Response"),
            pytest.param([1, 2, 3], (1, 2, 3), id="Sequence Data"),
            pytest.param(
                [0, 1, 2],
                (0, 1, 2),
                id="Sequence Data with False first element",
            ),
        ),
    )
    def test_batch_response(self, data: Any, expected_body: Any) -> None:
        response = ensure_response(data)
        cmd = self.publish_command_cls.from_cmd(
            response.as_publish_command(),
            batch=True,
        )
        assert cmd.batch_bodies == expected_body

    def test_batch_bodies_setter(self) -> None:
        response = ensure_response(None)
        cmd = self.publish_command_cls.from_cmd(
            response.as_publish_command(),
            batch=True,
        )
        cmd.batch_bodies = (1, 2, 3)

        assert cmd.batch_bodies == (1, 2, 3)
        assert cmd.body == 1
        assert cmd.extra_bodies == (2, 3)

    def test_batch_bodies_empty_setter(self) -> None:
        response = ensure_response((1, 2, 3))
        cmd = self.publish_command_cls.from_cmd(
            response.as_publish_command(),
            batch=True,
        )
        cmd.batch_bodies = ()

        assert cmd.batch_bodies == ()
        assert cmd.body is None
        assert cmd.extra_bodies == ()


BODY_A, BODY_B, BODY_C = "body-A", "body-B", "body-C"


def reverse(bodies: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(reversed(bodies))


def drop_first(bodies: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(bodies[1:])


def keep(bodies: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(bodies)


def dedup(bodies: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(dict.fromkeys(bodies))


SHUFFLED = (
    (BODY_A, b"1"),
    (BODY_C, b"2"),
    (BODY_B, b"3"),
    (BODY_A, b"4"),
    (BODY_B, b"5"),
    (BODY_A, b"6"),
    (BODY_A, b"7"),
)

# (original (body, key) pairs, batch_bodies mutation, expected (body, key) pairs)
KEY_ALIGNMENT_CASES = (
    pytest.param(
        ((BODY_A, b"key-A"), (BODY_B, b"key-B")),
        reverse,
        ((BODY_B, b"key-B"), (BODY_A, b"key-A")),
        id="reversed",
    ),
    pytest.param(
        ((BODY_A, b"key-A"), (BODY_B, b"key-B"), (BODY_C, b"key-C")),
        drop_first,
        ((BODY_B, b"key-B"), (BODY_C, b"key-C")),
        id="first-dropped",
    ),
    pytest.param(
        ((BODY_A, b"key-A"), (BODY_B, b"key-B"), (BODY_B, b"key-B")),
        keep,
        ((BODY_A, b"key-A"), (BODY_B, b"key-B"), (BODY_B, b"key-B")),
        id="untouched-with-duplicates",
    ),
    pytest.param(
        ((BODY_A, b"key-A"), (BODY_B, b"key-B"), (BODY_B, b"key-B")),
        reverse,
        ((BODY_B, b"key-B"), (BODY_B, b"key-B"), (BODY_A, b"key-A")),
        id="reversed-with-duplicates",
    ),
    pytest.param(
        ((BODY_A, b"key-1"), (BODY_A, b"key-2"), (BODY_B, b"key-3")),
        dedup,
        ((BODY_A, b"key-1"), (BODY_B, b"key-3")),
        id="deduplicated",
    ),
    pytest.param(
        ((BODY_A, None), (BODY_B, b"key-B"), (BODY_B, None)),
        reverse,
        ((BODY_B, b""), (BODY_B, b"key-B"), (BODY_A, b"")),
        id="reversed-with-duplicates-and-none-keys",
    ),
    pytest.param(SHUFFLED, keep, SHUFFLED, id="untouched-shuffled"),
    # Every pair below is one of the original ones, but identical bodies keep their
    # keys in the original relative order instead of being reversed along with them --
    # equal bodies are indistinguishable, so any of their keys is a valid match.
    pytest.param(
        SHUFFLED,
        reverse,
        (
            (BODY_A, b"1"),
            (BODY_A, b"4"),
            (BODY_B, b"3"),
            (BODY_A, b"6"),
            (BODY_B, b"5"),
            (BODY_C, b"2"),
            (BODY_A, b"7"),
        ),
        id="reversed-shuffled",
    ),
    pytest.param(
        SHUFFLED,
        drop_first,
        SHUFFLED[1:],
        id="first-dropped-shuffled",
        marks=pytest.mark.xfail(
            reason="Identical bodies can't be traced back to their keys once a body is removed, since keys are not unique.",
        ),
    ),
)


class BatchKeysTestcase:
    """Per-message keys must keep following their bodies when `batch_bodies` is replaced.

    Applies to Kafka-like brokers, where every batch element carries its own key.
    """

    publish_command_cls: type[BatchPublishCommand]
    publish_message_cls: type[Any]

    @staticmethod
    def get_message_key(raw_message: Any) -> bytes:
        return raw_message.key

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("pairs", "mutate", "expected"),
        KEY_ALIGNMENT_CASES,
    )
    async def test_publish_middleware_keeps_keys_aligned(
        self,
        queue: str,
        pairs,
        mutate,
        expected,
    ) -> None:
        publish_command_cls = self.publish_command_cls

        class MutatingMiddleware(BaseMiddleware):
            async def publish_scope(self, call_next, cmd):
                if isinstance(cmd, publish_command_cls):
                    cmd.batch_bodies = mutate(cmd.batch_bodies)
                return await call_next(cmd)

        broker = self.get_broker(apply_types=True, middlewares=(MutatingMiddleware,))

        received = []

        @broker.subscriber(queue, batch=True)
        async def handler(msgs, raw=Context("message")) -> None:
            received.extend(
                zip(
                    msgs,
                    (self.get_message_key(m) for m in raw.raw_message),
                    strict=True,
                ),
            )

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish_batch(
                *(self.publish_message_cls(body, key=key) for body, key in pairs),
                topic=queue,
            )
        assert Counter(received) == Counter(expected)
