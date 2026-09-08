import pytest

from faststream.exceptions import SetupError
from faststream.redis import StreamSub
from tests.marks import require_redis_v710


@pytest.mark.redis()
def test_stream_group() -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        StreamSub("test", group="group")

    with pytest.raises(ValueError):  # noqa: PT011
        StreamSub("test", consumer="consumer")

    StreamSub("test", group="group", consumer="consumer")


@pytest.mark.redis()
@require_redis_v710
@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        pytest.param(
            {
                "group": "group",
                "consumer": "consumer",
                "min_idle_time": 1000,
                "claim_min_idle_time": 1000,
            },
            "mutually exclusive",
            id="mutually exclusive with min_idle_time",
        ),
        pytest.param(
            {"claim_min_idle_time": 1000},
            "requires `group` and `consumer`",
            id="requires group and consumer",
        ),
        pytest.param(
            {
                "group": "group",
                "consumer": "consumer",
                "last_id": "0",
                "claim_min_idle_time": 1000,
            },
            "requires last_id `>`",
            id="requires last_id `>`",
        ),
        pytest.param(
            {
                "group": "group",
                "consumer": "consumer",
                "no_ack": True,
                "claim_min_idle_time": 1000,
            },
            "infinite redelivery",
            id="incompatible with no_ack",
        ),
    ),
)
def test_stream_claim_min_idle_time_misconfiguration(kwargs: dict, match: str) -> None:
    with pytest.raises(SetupError, match=match):
        StreamSub("test", **kwargs)


@pytest.mark.redis()
def test_stream_claim_min_idle_time_requires_redis_py_710(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "faststream.redis.schemas.stream_sub.REDIS_V710",
        False,
    )

    with pytest.raises(SetupError, match=r"redis-py 7\.1\.0"):
        StreamSub(
            "test",
            group="group",
            consumer="consumer",
            claim_min_idle_time=1000,
        )


@pytest.mark.redis()
@require_redis_v710
def test_stream_claim_min_idle_time() -> None:
    stream = StreamSub(
        "test",
        group="group",
        consumer="consumer",
        claim_min_idle_time=1000,
    )

    assert stream.claim_min_idle_time == 1000
