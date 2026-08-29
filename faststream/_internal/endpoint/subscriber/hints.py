from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, get_type_hints

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

    Args:
        call: the decorated handler.
        annotations: driver class to the name of the annotation wrapping it.
        module: where those annotations live.
    """
    hints = [
        _format_hint(call, field_name, driver_type, name, module)
        for field_name, driver_type in _iter_class_hints(call)
        if (name := annotations.get(driver_type)) is not None
    ]

    if hints:
        raise SetupError("\n".join(hints))


def _iter_class_hints(call: Callable[..., Any]) -> Iterator[tuple[str, type[Any]]]:
    try:
        hints = get_type_hints(call, include_extras=True)
    except Exception:
        # Building the call model resolves the same hints and reports the same
        # failure, so decoration stays silent about it.
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
