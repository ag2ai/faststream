from typing import Any

import pydantic
import pytest
from fast_depends import Depends
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer

from faststream import Context, Header
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema
from tests.marks import pydantic_v2


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@pytest.mark.parametrize("serializer", (PydanticSerializer(), MsgSpecSerializer()))
@pytest.mark.parametrize("schema_version", ("2.6.0", "3.0.0"))
@pytest.mark.parametrize("extra", (False, True))
async def test_injected_name_does_not_hide_dependency_body(
    serializer: Any, schema_version: str, extra: bool
) -> None:
    broker = RabbitBroker(serializer=serializer)
    seen: list[tuple[str, int | None]] = []
    dependencies: list[int] = []

    def dependency(value: int) -> int:
        dependencies.append(value)
        return value + 1

    async def handler(
        value: str = Header("token"),
        raw: StreamMessage[Any] = Context("message"),
        result: int = Depends(dependency),
    ) -> None:
        assert raw.headers["token"] == value
        seen.append((value, result))

    async def with_extra(
        value: str = Header("token"),
        raw: StreamMessage[Any] = Context("message"),
    ) -> None:
        assert raw.headers["token"] == value
        seen.append((value, None))

    broker.subscriber("input", dependencies=[Depends(dependency)] if extra else [])(
        with_extra if extra else handler
    )
    get_schema = get_2_6_0_schema if schema_version == "2.6.0" else get_3_0_0_schema
    schema = get_schema(broker)
    payloads = schema["components"]["schemas"]

    assert len(payloads) == 1
    assert next(iter(payloads.values()))["type"] == "integer"
    assert get_schema(broker) == schema
    assert dependencies == []
    assert seen == []

    async with TestRabbitBroker(broker) as test_broker:
        await test_broker.publish(41, "input", headers={"token": "secret"})

    assert seen == [("secret", None if extra else 42)]
    assert dependencies == [41]


@pytest.mark.rabbit()
@pytest.mark.asyncio()
@pytest.mark.parametrize("schema_version", ("2.6.0", "3.0.0"))
@pydantic_v2
async def test_dependency_validation_alias_keeps_single_body(schema_version: str) -> None:
    broker = RabbitBroker()
    seen: list[tuple[str, int]] = []

    def dependency(
        value: int = pydantic.Field(
            ..., validation_alias=pydantic.AliasChoices("external", "value")
        ),
    ) -> int:
        return value + 1

    @broker.subscriber("input")
    async def handler(
        value: str = Header("token"), result: int = Depends(dependency)
    ) -> None:
        seen.append((value, result))

    get_schema = get_2_6_0_schema if schema_version == "2.6.0" else get_3_0_0_schema
    payloads = get_schema(broker)["components"]["schemas"]
    assert len(payloads) == 1
    assert next(iter(payloads.values()))["type"] == "integer"
    assert seen == []

    async with TestRabbitBroker(broker) as test_broker:
        await test_broker.publish(41, "input", headers={"token": "secret"})

    assert seen == [("secret", 42)]
