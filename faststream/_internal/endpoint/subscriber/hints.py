from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, get_type_hints

from faststream._internal._compat import ExceptionGroup
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping


def check_context_annotations(
    call: Callable[..., Any],
    annotations: "Mapping[type[Any], str]",
    module: str,
) -> None:
    """Reject handler arguments annotated with a broker's own driver class.

    A working argument is `Annotated[...]` rather than a class, so the context
    annotations a broker wraps around these same classes are never matched.

    Raises an `ExceptionGroup` of one `SetupError` per offending argument.

    Args:
        call: the decorated handler.
        annotations: driver class to the name of the annotation wrapping it.
        module: where those annotations live.
    """
    errors = [
        SetupError(_format_hint(call, field_name, driver_type, name, module))
        for field_name, driver_type in _iter_class_hints(call)
        if (name := annotations.get(driver_type)) is not None
    ]

    if errors:
        call_name = getattr(call, "__name__", str(call))
        msg = f"`{call_name}` has arguments FastStream cannot inject."
        raise ExceptionGroup(msg, errors)


def _iter_class_hints(call: Callable[..., Any]) -> Iterator[tuple[str, type[Any]]]:
    try:
        hints = get_type_hints(call, include_extras=True)
    except Exception:
        # A hint that only exists under TYPE_CHECKING with PEP 563 cannot be
        # resolved here. Building the call model reports it as it does today.
        return

    for field_name, hint in hints.items():
        if field_name != "return" and isinstance(hint, type):
            yield field_name, hint


def _format_hint(
    call: Callable[..., Any],
    field_name: str,
    driver_type: type[Any],
    name: str,
    module: str,
) -> str:
    call_name = getattr(call, "__name__", str(call))
    driver_path = f"{driver_type.__module__}.{driver_type.__qualname__}"

    return (
        f"`{call_name}` parameter `{field_name}` is annotated with "
        f"`{driver_path}`, which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        f"\n    from {module} import {name}\n"
    )
