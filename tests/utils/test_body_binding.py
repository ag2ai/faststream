import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from fast_depends import Depends, Provider
from pydantic import Field

from faststream._internal.di import FastDependsConfig
from faststream._internal.di.config import _unwrap_message_to_fast_depends_decorator
from faststream.message import StreamMessage, decode_message

if TYPE_CHECKING:
    from collections.abc import Callable


async def decode(message: StreamMessage[Any]) -> Any:
    return decode_message(message)


def message(value: Any) -> StreamMessage[Any]:
    result = StreamMessage(
        None, json.dumps(value).encode(), content_type="application/json"
    )
    result.set_decoder(decode)
    return result


@pytest.mark.asyncio()
class TestBodyShapes:
    @pytest.mark.parametrize(
        "payload",
        (
            pytest.param(None, id="null"),
            pytest.param({}, id="empty-mapping"),
            pytest.param([], id="empty-sequence"),
        ),
    )
    async def test_whole_body_is_preserved(self, payload: Any) -> None:
        async def handler(value: Any) -> Any:
            return value

        built = FastDependsConfig().build_call(handler)

        assert await built.wrapped_call(message(payload)) == payload

    async def test_multiple_parameters_preserve_aliases_and_defaults(self) -> None:
        async def handler(
            a: int = Field(..., alias="external"), b: int = 7
        ) -> tuple[int, int]:
            return a, b

        built = FastDependsConfig().build_call(handler)
        assert await built.wrapped_call(message({"external": 1})) == (1, 7)


@pytest.mark.asyncio()
class TestDependencyBodyBinding:
    async def test_provider_override_inputs_define_body(self) -> None:
        def dependency(value: int) -> int:
            return value

        def replacement(a: int, b: int) -> int:
            return a * 2 + b

        async def handler(result: int = Depends(dependency)) -> int:
            return result

        provider = Provider()
        provider.override(dependency, replacement)
        built = FastDependsConfig(provider=provider).build_call(handler)

        assert await built.wrapped_call(message({"a": 10, "b": 22})) == 42


@pytest.mark.asyncio()
class TestBodyUnpacking:
    @pytest.mark.parametrize(
        ("signature", "payload", "expected_args", "expected_kwargs"),
        (
            pytest.param("positional", 42, (42,), {}, id="positional-only"),
            pytest.param("star", [1, 2], (1, 2), {}, id="var-positional"),
            pytest.param("double_star", {"a": 1}, ({"a": 1},), {}, id="var-keyword"),
            pytest.param("mixed", {"a": 1, "b": 2}, (), {"a": 1, "b": 2}, id="mixed"),
        ),
    )
    async def test_argument_kinds_preserve_unwrapping(
        self,
        signature: str,
        payload: Any,
        expected_args: tuple[Any, ...],
        expected_kwargs: dict[str, Any],
        mock: MagicMock,
    ) -> None:
        async def positional(value: int, /) -> None: ...

        async def star(*items: tuple[int, ...]) -> None: ...

        async def double_star(**values: Any) -> None: ...

        async def mixed(a: int, **values: Any) -> None: ...

        async def receive(*args: Any, **kwargs: Any) -> None:
            mock(*args, **kwargs)

        handlers: dict[str, Callable[..., Any]] = {
            "positional": positional,
            "star": star,
            "double_star": double_star,
            "mixed": mixed,
        }
        built = FastDependsConfig().build_call(handlers[signature])
        wrapper = _unwrap_message_to_fast_depends_decorator(receive, built.dependent)
        await wrapper(message(payload))

        mock.assert_called_once_with(*expected_args, **expected_kwargs)

    async def test_non_container_body_keeps_unpacking_error(self) -> None:
        async def handler(a: int, b: int) -> None: ...

        built = FastDependsConfig().build_call(handler)

        with pytest.raises(ValueError, match="Couldn't unpack `42` to multiple values"):
            await built.wrapped_call(message(42))
