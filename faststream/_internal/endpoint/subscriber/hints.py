from collections.abc import Callable
from inspect import unwrap
from typing import TYPE_CHECKING, Any, get_args, get_type_hints

from faststream._internal._compat import ExceptionGroup
from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping


def check_context_annotations(
    call: Callable[..., Any],
    annotations: "Mapping[type[Any], Any]",
) -> None:
    """Reject handler arguments annotated with a broker's own driver class.

    A working argument is `Annotated[...]` rather than a class, so the context
    annotations a broker wraps around these same classes are never matched.

    Args:
        call: the decorated handler.
        annotations: driver class to the context annotation wrapping it.
    """
    if not annotations:
        return

    # A publisher decorator applied first hands us its wrapper, and the wrapper
    # class carries annotations of its own that are not the handler's.
    if isinstance(call, HandlerCallWrapper):
        call = call._original_call

    handler = unwrap(call)

    hints = get_type_hints(handler, include_extras=True)

    errors = [
        SetupError(_format_hint(field_name, hint, annotation))
        for field_name, hint in hints.items()
        if field_name != "return"
        and isinstance(hint, type)
        and (annotation := annotations.get(hint)) is not None
    ]

    if errors:
        call_name = getattr(handler, "__name__", str(handler))
        msg = f"`{call_name}` has arguments FastStream cannot inject."
        raise ExceptionGroup(msg, errors)


def _format_hint(field_name: str, driver_type: type[Any], annotation: Any) -> str:
    driver_path = f"{driver_type.__module__}.{driver_type.__qualname__}"

    message = (
        f"`{field_name}` is annotated with `{driver_path}`, "
        "which FastStream cannot inject.\n"
    )

    if context_key := _context_key(annotation):
        return (
            f"{message}Use the context annotation instead:\n"
            f'\n    Annotated[{driver_type.__qualname__}, Context("{context_key}")]\n'
        )

    return f"{message}Use the context annotation FastStream provides for it instead."


def _context_key(annotation: Any) -> str | None:
    for metadata in get_args(annotation)[1:]:
        if name := getattr(metadata, "name", None):
            return str(name)

    return None
