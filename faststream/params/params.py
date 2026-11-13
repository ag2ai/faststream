from collections.abc import Callable
from typing import Any

from faststream._internal.constants import EMPTY, PATH_CONTEXT_PREFIX
from faststream._internal.context import Context as Context_


def Context(  # ruff: ignore[invalid-function-name]
    real_name: str = "",
    *,
    cast: bool = False,
    default: Any = EMPTY,
    initial: Callable[..., Any] | None = None,
) -> Any:
    return Context_(
        real_name=real_name,
        cast=cast,
        default=default,
        initial=initial,
    )


def Header(  # ruff: ignore[invalid-function-name]
    real_name: str = "",
    *,
    cast: bool = True,
    default: Any = EMPTY,
) -> Any:
    return Context_(
        real_name=real_name,
        cast=cast,
        default=default,
        prefix="message.headers.",
    )


def Path(  # ruff: ignore[invalid-function-name]
    real_name: str = "",
    *,
    cast: bool = True,
    default: Any = EMPTY,
    description: str | None = None,
    examples: list[Any] | None = None,
) -> Any:
    """Declare a broker-address path parameter and its AsyncAPI metadata.

    Args:
        real_name: Address parameter name when it differs from the handler argument.
        cast: Whether FastDepends should cast the received value.
        default: Fallback value when the address parameter is missing.
        description: Optional AsyncAPI parameter description.
        examples: Optional AsyncAPI parameter examples.
    """
    return Context_(
        real_name=real_name,
        cast=cast,
        default=default,
        prefix=PATH_CONTEXT_PREFIX,
        description=description,
        examples=examples,
    )
