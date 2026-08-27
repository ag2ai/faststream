import importlib
import re
from typing import Annotated, get_args

import pytest

from faststream._internal.di import FastDependsConfig, register_context_annotations
from faststream._internal.di.hints import _CONTEXT_ANNOTATIONS
from faststream.exceptions import SetupError
from faststream.params import Context
from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)


class FakeDriverClient:
    pass


register_context_annotations(__name__, {FakeDriverClient: "FakeClient"})

FakeClient = Annotated[FakeDriverClient, Context("fake_client")]


def test_driver_class_annotation_names_the_import_to_use() -> None:
    async def handler(client: FakeDriverClient) -> None: ...

    with pytest.raises(SetupError) as excinfo:
        FastDependsConfig().build_call(handler)

    assert str(excinfo.value) == (
        f"`handler` parameter `client` is annotated with `{__name__}.FakeDriverClient`,"
        " which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        f"\n    from {__name__} import FakeClient\n"
    )


def test_every_wrong_argument_is_reported() -> None:
    async def handler(first: FakeDriverClient, second: FakeDriverClient) -> None: ...

    with pytest.raises(SetupError) as excinfo:
        FastDependsConfig().build_call(handler)

    assert "parameter `first`" in str(excinfo.value)
    assert "parameter `second`" in str(excinfo.value)


def test_context_annotation_is_accepted() -> None:
    async def handler(client: FakeClient) -> None: ...

    FastDependsConfig().build_call(handler)


def test_no_check_without_type_casting() -> None:
    async def handler(client: FakeDriverClient) -> None: ...

    FastDependsConfig(use_fastdepends=False).build_call(handler)


@pytest.mark.parametrize(
    "module",
    (
        pytest.param(
            "faststream.redis.annotations",
            marks=(require_redis, pytest.mark.redis()),
        ),
        pytest.param(
            "faststream.rabbit.annotations",
            marks=(require_aiopika, pytest.mark.rabbit()),
        ),
        pytest.param(
            "faststream.kafka.annotations",
            marks=(require_aiokafka, pytest.mark.kafka()),
        ),
        pytest.param(
            "faststream.confluent.annotations",
            marks=(require_confluent, pytest.mark.confluent()),
        ),
        pytest.param(
            "faststream.nats.annotations",
            marks=(require_nats, pytest.mark.nats()),
        ),
        pytest.param(
            "faststream.mqtt.annotations",
            marks=(require_mqtt, pytest.mark.mqtt()),
        ),
    ),
)
def test_broker_rows_point_at_a_matching_annotation(module: str) -> None:
    annotations = importlib.import_module(module)

    rows = {
        driver_type: name
        for driver_type, (registry_module, name) in _CONTEXT_ANNOTATIONS.items()
        if registry_module == module
    }
    assert rows, f"{module} registered nothing"

    for driver_type, name in rows.items():
        assert get_args(getattr(annotations, name))[0] is driver_type


@require_redis
@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_raises_before_the_first_message() -> None:
    """Fixes https://github.com/ag2ai/faststream/issues/3065.

    The driver class used to reach pydantic as a message field, so the failure
    arrived per message and read as a broken message schema.
    """
    from redis.asyncio import Redis

    from faststream.redis import RedisBroker, TestRedisBroker

    broker = RedisBroker()

    @broker.subscriber("channel")
    async def handler(redis: Redis) -> None: ...

    with pytest.raises(
        SetupError,
        match=re.escape("from faststream.redis.annotations import Redis"),
    ):
        async with TestRedisBroker(broker):
            pass
