from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream.exceptions import ContextError

if TYPE_CHECKING:
    from contextvars import Token

T = TypeVar("T")


class ContextRepoComposition(ContextRepo):
    def __init__(self, *contexts: ContextRepo) -> None:
        super().__init__()

        self._inner_context = ContextRepo()
        self._inner_context.set_global("context", self)

        self._contexts = (self._inner_context, *contexts)

    @property
    @override
    def context(self) -> dict[str, Any]:
        result_context: dict[str, Any] = {}
        for context in reversed(self._contexts):
            result_context |= context.context

        return result_context

    @override
    def set_global(self, key: str, v: Any) -> None:
        self._inner_context.set_global(key, v)

    @override
    def reset_global(self, key: str) -> None:
        self._inner_context.reset_global(key)

    @override
    def set_local(self, key: str, value: T) -> "Token[T]":
        return self._inner_context.set_local(key, value)

    @override
    def reset_local(self, key: str, tag: "Token[Any]") -> None:
        self._inner_context.reset_local(key, tag)

    @override
    def get_local(self, key: str, default: Any = None) -> Any:
        for context in self._contexts:
            variable = context.get_local(key, EMPTY)
            if variable != EMPTY:
                return variable
        return default

    @contextmanager
    @override
    def scope(self, key: str, value: Any) -> Generator[None, None, None]:
        with self._inner_context.scope(key, value):
            yield

    @override
    def get(self, key: str, default: Any = None) -> Any:
        for context in self._contexts:
            variable = context.get(key, EMPTY)
            if variable != EMPTY:
                return variable
        return default

    @override
    def __getattr__(self, name: str, /) -> Any:
        for context in self._contexts:
            variable = getattr(context, name)
            if variable is not None:
                return variable
        return None

    @override
    def resolve(self, argument: str) -> Any:
        first, *_ = argument.split(".")

        for context in self._contexts:
            try:
                return context.resolve(argument)
            except ContextError:  # noqa: PERF203
                pass

        raise ContextError(self.context, first)

    @override
    def clear(self) -> None:
        self._inner_context.clear()
        self._inner_context.set_global("context", self)
