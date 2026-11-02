from typing import Any

from faststream._internal.constants import EMPTY
from faststream._internal.context import Context as Context_


def Header(  # ruff: ignore[invalid-function-name]
    real_name: str = "",
    *,
    cast: bool = False,
    default: Any = EMPTY,
) -> Any:
    return Context_(
        real_name=real_name.lower(),
        cast=cast,
        default=default,
        prefix="request.headers.",
    )


def Query(  # ruff: ignore[invalid-function-name]
    real_name: str = "",
    *,
    cast: bool = False,
    default: Any = EMPTY,
) -> Any:
    return Context_(
        real_name=real_name,
        cast=cast,
        default=default,
        prefix="request.query_params.",
    )
