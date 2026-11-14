import json
from collections.abc import Callable
from typing import Any

import pytest
from fast_depends import Depends, Provider
from pydantic import BaseModel, Field

from faststream._internal.di import FastDependsConfig
from faststream._internal.di.config import _unwrap_message_to_fast_depends_decorator
from faststream.message import StreamMessage, decode_message
from faststream.specification.asyncapi.message import parse_handler_params


class User(BaseModel):
    name: str


async def scalar(value: int) -> int:
    return value


async def mapping(value: dict[str, int]) -> dict[str, int]:
    return value


async def sequence(value: list[int]) -> list[int]:
    return value


async def model(user: User) -> dict[str, str]:
    return {"name": user.name}


async def pair(a: int, b: int) -> tuple[int, int]:
    return a, b


async def varargs(*items: tuple[int, ...]) -> tuple[Any, ...]:
    return items


async def mixed(a: int, *rest: tuple[int, ...]) -> tuple[int, tuple[Any, ...]]:
    return a, rest


async def keyword_only(*, value: int) -> int:
    return value


async def empty() -> str:
    return "empty"


async def decode(message: StreamMessage[Any]) -> Any:
    return decode_message(message)


def message(value: Any) -> StreamMessage[Any]:
    result = StreamMessage(
        None, json.dumps(value).encode(), content_type="application/json"
    )
    result.set_decoder(decode)
    return result


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("handler", "payload", "expected", "schema_type"),
    (
        (scalar, "12", 12, "integer"),
        (mapping, {"n": "2"}, {"n": 2}, "object"),
        (sequence, [1, "2"], [1, 2], "array"),
        (model, {"name": "Ada"}, {"name": "Ada"}, "object"),
        (pair, {"a": "1", "b": 2}, (1, 2), "object"),
        (pair, [1, "2"], (1, 2), "object"),
        (varargs, [1, "2"], (1, 2), "array"),
        (mixed, [1, "2", 3], (1, (2, 3)), "object"),
        (keyword_only, "12", 12, "integer"),
        (empty, None, "empty", "null"),
        (empty, {}, "empty", "null"),
    ),
)
async def test_existing_body_shapes(
    handler: Callable[..., Any], payload: Any, expected: Any, schema_type: str
) -> None:
    built = FastDependsConfig().build_call(handler)

    assert parse_handler_params(built.dependent)["type"] == schema_type
    assert await built.wrapped_call(message(payload)) == expected


@pytest.mark.asyncio()
@pytest.mark.parametrize("payload", (None, {}, []))
async def test_whole_body_and_null_remain_distinct(payload: Any) -> None:
    async def handler(value: Any) -> Any:
        return value

    built = FastDependsConfig().build_call(handler)

    assert await built.wrapped_call(message(payload)) == payload
    assert parse_handler_params(built.dependent)["title"] != "EmptyPayload"


@pytest.mark.asyncio()
@pytest.mark.parametrize("extra", (False, True))
async def test_dependency_inputs_define_body_without_running_dependencies(
    extra: bool,
) -> None:
    seen: list[int] = []

    def dependency(value: int) -> int:
        seen.append(value)
        return value + 1

    async def handler(result: int = Depends(dependency)) -> int:
        return result

    built = FastDependsConfig().build_call(
        empty if extra else handler,
        dependencies=(Depends(dependency),) if extra else (),
    )
    schema = parse_handler_params(built.dependent)

    assert schema["type"] == "integer"
    assert seen == []
    assert await built.wrapped_call(message(41)) == ("empty" if extra else 42)
    assert seen == [41]


@pytest.mark.asyncio()
async def test_provider_override_inputs_define_body() -> None:
    def dependency(value: int) -> int:
        return value

    def replacement(a: int, b: int) -> int:
        return a * 2 + b

    async def handler(result: int = Depends(dependency)) -> int:
        return result

    provider = Provider()
    provider.override(dependency, replacement)
    built = FastDependsConfig(provider=provider).build_call(handler)

    assert set(parse_handler_params(built.dependent)["properties"]) == {"a", "b"}
    assert await built.wrapped_call(message({"a": 10, "b": 22})) == 42


@pytest.mark.asyncio()
async def test_multiple_parameters_preserve_aliases_and_defaults() -> None:
    async def handler(
        a: int = Field(..., alias="external"), b: int = 7
    ) -> tuple[int, int]:
        return a, b

    built = FastDependsConfig().build_call(handler)
    schema = parse_handler_params(built.dependent)

    assert set(schema["properties"]) == {"external", "b"}
    assert schema["required"] == ["external"]
    assert schema["properties"]["b"]["default"] == 7
    assert await built.wrapped_call(message({"external": 1})) == (1, 7)


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("signature", "payload", "expected_args", "expected_kwargs"),
    (
        ("positional", 42, (42,), {}),
        ("star", [1, 2], (1, 2), {}),
        ("double_star", {"a": 1}, ({"a": 1},), {}),
        ("mixed", {"a": 1, "b": 2}, (), {"a": 1, "b": 2}),
    ),
)
async def test_argument_kinds_preserve_unwrapping(
    signature: str,
    payload: Any,
    expected_args: tuple[Any, ...],
    expected_kwargs: dict[str, Any],
) -> None:
    async def positional(value: int, /) -> None: ...

    async def star(*items: tuple[int, ...]) -> None: ...

    async def double_star(**values: Any) -> None: ...

    async def mixed(a: int, **values: Any) -> None: ...

    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def receive(*args: Any, **kwargs: Any) -> None:
        calls.append((args, kwargs))

    handlers: dict[str, Callable[..., Any]] = {
        "positional": positional,
        "star": star,
        "double_star": double_star,
        "mixed": mixed,
    }
    built = FastDependsConfig().build_call(handlers[signature])
    wrapper = _unwrap_message_to_fast_depends_decorator(receive, built.dependent)
    await wrapper(message(payload))

    assert calls == [(expected_args, expected_kwargs)]


@pytest.mark.asyncio()
async def test_non_container_body_keeps_unpacking_error() -> None:
    built = FastDependsConfig().build_call(pair)

    with pytest.raises(ValueError, match="Couldn't unpack `42` to multiple values"):
        await built.wrapped_call(message(42))
