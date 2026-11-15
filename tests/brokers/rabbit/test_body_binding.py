from typing import Any
from unittest.mock import MagicMock

import pydantic
import pytest
from fast_depends import Depends
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer

from faststream import Context, Header
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from tests.marks import pydantic_v2


@pytest.mark.rabbit()
@pytest.mark.asyncio()
class TestBodyBinding:
    @pytest.mark.parametrize(
        "serializer",
        (
            pytest.param(PydanticSerializer(), id="pydantic"),
            pytest.param(MsgSpecSerializer(), id="msgspec"),
        ),
    )
    async def test_dependency_receives_body_when_handler_injects_same_name(
        self, serializer: Any, queue: str, mock: MagicMock
    ) -> None:
        broker = RabbitBroker(serializer=serializer)

        def dependency(value: int) -> int:
            mock.dependency(value)
            return value + 1

        @broker.subscriber(queue)
        async def handler(
            value: str = Header("token"),
            raw: StreamMessage[Any] = Context("message"),
            result: int = Depends(dependency),
        ) -> None:
            assert raw.headers["token"] == value
            mock(value, result)

        async with TestRabbitBroker(broker) as test_broker:
            await test_broker.publish(41, queue, headers={"token": "secret"})

        mock.assert_called_once_with("secret", 42)
        mock.dependency.assert_called_once_with(41)

    @pytest.mark.parametrize(
        "serializer",
        (
            pytest.param(PydanticSerializer(), id="pydantic"),
            pytest.param(MsgSpecSerializer(), id="msgspec"),
        ),
    )
    async def test_extra_dependency_receives_body_when_handler_injects_same_name(
        self, serializer: Any, queue: str, mock: MagicMock
    ) -> None:
        broker = RabbitBroker(serializer=serializer)

        def dependency(value: int) -> int:
            mock.dependency(value)
            return value + 1

        @broker.subscriber(queue, dependencies=[Depends(dependency)])
        async def handler(
            value: str = Header("token"),
            raw: StreamMessage[Any] = Context("message"),
        ) -> None:
            assert raw.headers["token"] == value
            mock(value)

        async with TestRabbitBroker(broker) as test_broker:
            await test_broker.publish(41, queue, headers={"token": "secret"})

        mock.assert_called_once_with("secret")
        mock.dependency.assert_called_once_with(41)

    @pydantic_v2
    async def test_dependency_validation_alias_keeps_single_body(
        self, queue: str, mock: MagicMock
    ) -> None:
        broker = RabbitBroker()

        def dependency(
            value: int = pydantic.Field(
                ..., validation_alias=pydantic.AliasChoices("external", "value")
            ),
        ) -> int:
            return value + 1

        @broker.subscriber(queue)
        async def handler(
            value: str = Header("token"), result: int = Depends(dependency)
        ) -> None:
            mock(value, result)

        async with TestRabbitBroker(broker) as test_broker:
            await test_broker.publish(41, queue, headers={"token": "secret"})

        mock.assert_called_once_with("secret", 42)
