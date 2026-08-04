from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar, Token
from typing import Any, TypeVar

from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream.exceptions import ContextError

T = TypeVar("T")


class ContextRepoComposition(ContextRepo):
    def __init__(
        self,
        lgh_context: ContextRepo,
        rgh_context: ContextRepo,
    ) -> None:
        self._lgh_context = lgh_context
        self._rgh_context = rgh_context

        self._set_context()

        super().__init__()

    @property
    @override
    def context(self) -> dict[str, Any]:
        return {
            **self._lgh_context.context,
            **self._rgh_context.context,
        }

    def _set_context(self) -> None:
        self._lgh_context.set_global("context", self)
        self._rgh_context.set_global("context", self)

    @override
    def set_global(self, key: str, v: Any) -> None:
        """Sets a value in the global context.

        Args:
            key: The key to set in the global context.
            v: The value to set.

        Returns:
            None.
        """
        self._lgh_context.set_global(key, v)
        self._rgh_context.set_global(key, v)

    @override
    def reset_global(self, key: str) -> None:
        """Resets a key in the global context.

        Args:
            key (str): The key to reset in the global context.

        Returns:
            None
        """
        self._lgh_context.reset_global(key)
        self._rgh_context.reset_global(key)

    @override
    def set_local(self, key: str, value: T) -> "Token[T]":
        """Set a local context variable.

        Args:
            key (str): The key for the context variable.
            value (T): The value to set for the context variable.

        Returns:
            Token[T]: A token representing the context variable.
        """
        context_var = self._lgh_context._scope_context.get(key)
        if context_var is None:
            context_var = self._rgh_context._scope_context.get(key)

        if context_var is None:
            context_var = ContextVar(key, default=EMPTY)

            self._lgh_context._scope_context[key] = context_var
            self._rgh_context._scope_context[key] = context_var

        return context_var.set(value)

    @override
    def reset_local(self, key: str, tag: "Token[Any]") -> None:
        """Resets the local context for a given key.

        Args:
            key (str): The key to reset the local context for.
            tag (Token[Any]): The tag associated with the local context.

        Returns:
            None
        """
        variable = self._lgh_context._scope_context.get(key)

        if variable is not None:
            variable.reset(tag)

    @override
    def get_local(self, key: str, default: Any = None) -> Any:
        """Get the value of a local variable.

        Args:
            key: The key of the local variable to retrieve.
            default: The default value to return if the local variable is not found.

        Returns:
            The value of the local variable.
        """
        variable = self._lgh_context.get_local(key, default)
        if variable is default:
            variable = self._rgh_context.get_local(key, default)

        return variable

    @contextmanager
    @override
    def scope(self, key: str, value: Any) -> Generator[None, None, None]:
        """Sets a local variable and yields control to the caller. After the caller is done, the local variable is reset.

        Args:
            key: The key of the local variable
            value: The value to set the local variable to

        Yields:
            None

        Returns:
            An iterator that yields None
        """
        with ExitStack() as stack:
            stack.enter_context(self._lgh_context.scope(key, value))
            stack.enter_context(self._rgh_context.scope(key, value))

            yield

    @override
    def get(self, key: str, default: Any = None) -> Any:
        """Get the value associated with a key.

        Args:
            key: The key to retrieve the value for.
            default: The default value to return if the key is not found.

        Returns:
            The value associated with the key.
        """
        variable = self._lgh_context.get(key, default)
        if variable is default:
            variable = self._rgh_context.get(key, default)

        return variable

    @override
    def __getattr__(self, name: str, /) -> Any:
        """This is a function that is part of a class. It is used to get an attribute value using the `__getattr__` method.

        Args:
            name: The name of the attribute to get.

        Returns:
            The value of the attribute.
        """
        variable = getattr(self._lgh_context, name)
        if variable is None:
            variable = getattr(self._rgh_context, name)

        return variable

    @override
    def resolve(self, argument: str) -> Any:
        """Resolve the context of an argument.

        Args:
            argument: A string representing the argument.

        Returns:
            The resolved context of the argument.

        Raises:
            AttributeError, KeyError: If the argument does not exist in the context.
        """
        try:
            return self._lgh_context.resolve(argument)
        except ContextError:
            pass

        try:
            return self._rgh_context.resolve(argument)
        except ContextError as error:
            field = error.field

        raise ContextError(self.context, field)

    @override
    def clear(self) -> None:
        self._lgh_context.clear()
        self._rgh_context.clear()

        self._set_context()
