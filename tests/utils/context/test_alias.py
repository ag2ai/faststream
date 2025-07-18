from typing import Annotated, Any

import pytest

from faststream import Context, ContextRepo
from faststream._internal.utils import apply_types


@pytest.mark.asyncio()
async def test_base_context_alias(context: ContextRepo) -> None:
    key = 1000
    context.set_global("key", key)

    @apply_types(context__=context)
    async def func(k=Context("key")):
        return k is key

    assert await func()


@pytest.mark.asyncio()
async def test_context_cast(context: ContextRepo) -> None:
    key = 1000
    context.set_global("key", key)

    @apply_types(context__=context)
    async def func(k: float = Context("key", cast=True)):
        return isinstance(k, float)

    assert await func()


@pytest.mark.asyncio()
async def test_nested_context_alias(context: ContextRepo) -> None:
    model = SomeModel(field=SomeModel(field=1000))
    context.set_global("model", model)

    @apply_types(context__=context)
    async def func(
        m=Context("model.field.field"),
        m2=Context("model.not_existed", default=None),
        m3=Context("model.not_existed.not_existed", default=None),
        m4=Context("model.another_field"),
        m5=Context("model.another_field.f", default=1),
    ):
        return (
            m is model.field.field
            and m2 is None
            and m3 is None
            and m4 is None
            and m5 == 1
        )

    assert await func(model=model)


@pytest.mark.asyncio()
async def test_annotated_alias(context: ContextRepo) -> None:
    model = SomeModel(field=SomeModel(field=1000))
    context.set_global("model", model)

    @apply_types(context__=context)
    async def func(m: Annotated[int, Context("model.field.field")]):
        return m is model.field.field

    assert await func(model=model)


class SomeModel:
    field: Any = ""
    another_field: Any = None

    def __init__(self, field) -> None:
        self.field = field
