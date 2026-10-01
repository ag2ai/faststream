from typing import Any

import pytest
from fast_depends import ValidationError

from faststream import Context, ContextRepo
from faststream._internal.utils import apply_types


def test_context_getattr(context: ContextRepo) -> None:
    a = 1000
    context.set_global("key", a)

    assert context.key is a
    assert context.key2 is None


@pytest.mark.asyncio()
async def test_context_apply(context: ContextRepo) -> None:
    a = 1000
    context.set_global("key", a)

    @apply_types(context__=context)
    async def use(key: Any = Context()) -> Any:
        return key is a

    assert await use()


@pytest.mark.asyncio()
async def test_context_ignore(context: ContextRepo) -> None:
    a = 3
    context.set_global("key", a)

    @apply_types(context__=context)
    async def use() -> None:
        return None

    assert await use() is None


@pytest.mark.asyncio()
async def test_context_apply_multi(context: ContextRepo) -> None:
    a = 1001
    context.set_global("key_a", a)

    b = 1000
    context.set_global("key_b", b)

    @apply_types(context__=context)
    async def use1(key_a: Any = Context()) -> Any:
        return key_a is a

    assert await use1()

    @apply_types(context__=context)
    async def use2(key_b: Any = Context()) -> Any:
        return key_b is b

    assert await use2()

    @apply_types(context__=context)
    async def use3(key_a: Any = Context(), key_b: Any = Context()) -> Any:
        return key_a is a and key_b is b

    assert await use3()


@pytest.mark.asyncio()
async def test_context_overrides(context: ContextRepo) -> None:
    a = 1001
    context.set_global("test", a)

    b = 1000
    context.set_global("test", b)

    @apply_types(context__=context)
    async def use(test: Any = Context()) -> Any:
        return test is b

    assert await use()


@pytest.mark.asyncio()
async def test_context_nested_apply(context: ContextRepo) -> None:
    a = 1000
    context.set_global("key", a)

    @apply_types(context__=context)
    def use_nested(key: Any = Context()) -> Any:
        return key

    @apply_types(context__=context)
    async def use(key: Any = Context()) -> Any:
        return key is use_nested() is a

    assert await use()


@pytest.mark.asyncio()
async def test_reset_global(context: ContextRepo) -> None:
    a = 1000
    context.set_global("key", a)
    context.reset_global("key")

    @apply_types(context__=context)
    async def use(key: Any = Context()) -> None: ...

    with pytest.raises(ValidationError):
        await use()


@pytest.mark.asyncio()
async def test_clear_context(context: ContextRepo) -> None:
    a = 1000
    context.set_global("key", a)
    context.clear()

    @apply_types(context__=context)
    async def use(key: Any = Context(default=None)) -> Any:
        return key is None

    assert await use()


def test_scope(context: ContextRepo) -> None:
    @apply_types(context__=context)
    def use(key: Any = Context(), key2: Any = Context()) -> None:
        assert key == 1
        assert key2 == 1

    with context.scope("key", 1), context.scope("key2", 1):
        use()

    assert context.get("key") is None
    assert context.get("key2") is None


def test_scopes(context: ContextRepo) -> None:
    @apply_types(context__=context)
    def use(key: Any = Context(), key2: Any = Context()) -> None:
        assert key == 1
        assert key2 == 2

    with context.scopes((("key", 1), ("key2", 2))):
        use()

    assert context.get("key") is None
    assert context.get("key2") is None


def test_scopes_restores_a_repeated_key(context: ContextRepo) -> None:
    with context.scopes((("key", "first"), ("key", "second"))):
        assert context.get("key") == "second"

    assert context.get("key") is None


def test_default(context: ContextRepo) -> None:
    @apply_types(context__=context)
    def use(
        key: Any = Context(),
        key2: Any = Context(),
        key3: Any = Context(default=1),
        key4: Any = Context("key.key4", default=1),
        key5: Any = Context("key5.key6"),
    ) -> None:
        assert key == 0
        assert key2 is True
        assert key3 == 1
        assert key4 == 1
        assert key5 is False

    with (
        context.scope("key", 0),
        context.scope("key2", True),
        context.scope(
            "key5",
            {"key6": False},
        ),
    ):
        use()


def test_local_default(context: ContextRepo) -> None:
    key = "some-key"

    tag = context.set_local(key, "useless")
    context.reset_local(key, tag)

    assert context.get_local(key, 1) == 1


def test_initial(context: ContextRepo) -> None:
    @apply_types(context__=context)
    def use(
        a: Any,
        key: Any = Context(initial=list),
    ) -> Any:
        key.append(a)
        return key

    assert use(1) == [1]
    assert use(2) == [1, 2]


@pytest.mark.asyncio()
async def test_context_with_custom_object_implementing_comparison(
    context: ContextRepo,
) -> None:
    class User:  # noqa: PLW1641 - a user object that defines only `__eq__`
        def __init__(self, user_id: int) -> None:
            self.user_id = user_id

        def __eq__(self, other: object) -> Any:
            if not isinstance(other, User):
                return NotImplemented
            return self.user_id == other.user_id

        def __ne__(self, other: object) -> Any:
            return not self.__eq__(other)

    user2 = User(user_id=2)
    user3 = User(user_id=3)

    @apply_types(context__=context)
    async def use(
        key1: Any = Context("user1"),
        key2: Any = Context("user2", default=user2),
        key3: Any = Context("user3", default=user3),
    ) -> Any:
        return (
            key1 == User(user_id=1)
            and key2 == User(user_id=2)
            and key3 == User(user_id=4)
        )

    with (
        context.scope("user1", User(user_id=1)),
        context.scope("user3", User(user_id=4)),
    ):
        assert await use()
