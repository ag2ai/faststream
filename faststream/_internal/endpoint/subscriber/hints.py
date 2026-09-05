from collections.abc import Callable
from inspect import unwrap
from typing import TYPE_CHECKING, Any, get_type_hints

from faststream._internal._compat import ExceptionGroup
from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping


def check_context_annotations(
    call: Callable[..., Any],
    annotations: "Mapping[str, str]",
) -> None:
    """Reject handler arguments annotated with a broker's own driver class.

    A working argument is `Annotated[...]` rather than a class, so the context
    annotations a broker wraps around these same classes are never matched.

    Args:
        call: the decorated handler.
        annotations: driver class to context annotation, both as import paths.
    """
    if not annotations:
        return

    # A publisher decorator applied first hands us its wrapper, and the wrapper
    # class carries annotations of its own that are not the handler's.
    if isinstance(call, HandlerCallWrapper):
        call = call._original_call

    handler = unwrap(call)

    errors = [
        SetupError(_format_hint(field_name, hint, annotation))
        for field_name, hint in get_type_hints(handler, include_extras=True).items()
        if field_name != "return"
        and isinstance(hint, type)
        and (annotation := annotations.get(_path(hint))) is not None
    ]

    if errors:
        call_name = getattr(handler, "__name__", str(handler))
        msg = f"`{call_name}` has arguments FastStream cannot inject."
        raise ExceptionGroup(msg, errors)


def _path(driver_type: type[Any]) -> str:
    return f"{driver_type.__module__}.{driver_type.__qualname__}"


def _format_hint(field_name: str, driver_type: type[Any], annotation: str) -> str:
    module, _, name = annotation.rpartition(".")

    return (
        f"`{field_name}` is annotated with `{_path(driver_type)}`, "
        "which FastStream cannot inject.\n"
        "Use the context annotation instead:\n"
        f"\n    from {module} import {name}\n"
    )
