from collections.abc import Callable
from typing import Any

from faststream._internal.constants import EMPTY
from faststream._internal.context import Context as Context_


def Context(  # noqa: N802
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


def Header(  # noqa: N802
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


def Path(  # noqa: N802
    real_name: str = "",
    *,
    cast: bool = True,
    default: Any = EMPTY,
) -> Any:
    return Context_(
        real_name=real_name,
        cast=cast,
        default=default,
        prefix="message.path.",
    )
