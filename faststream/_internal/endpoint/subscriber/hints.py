from collections.abc import Callable
from inspect import unwrap
from typing import TYPE_CHECKING, Any, get_type_hints

from faststream._internal._compat import ExceptionGroup
from faststream._internal.configs import UnderlyingDriverAnnotation
from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Mapping


def check_context_annotations(
    call: Callable[..., Any],
    annotations: "Mapping[Any, Any]",
) -> None:
    """Reject handler arguments annotated with a broker's own driver class.

    A working argument is `Annotated[...]` rather than a class, so the context
    annotations a broker wraps around these same classes are never matched.

    Args:
        call: the decorated handler.
        annotations: driver type hint to the context annotation replacing it. An
            `UnderlyingDriverAnnotation` value also names the import to suggest.
    """
    if not annotations:
        return

    # A publisher decorator applied first hands us its wrapper, and the wrapper
    # class carries annotations of its own that are not the handler's.
    if isinstance(call, HandlerCallWrapper):
        call = call._declared_call

    handler = unwrap(call)

    errors = [
        SetupError(_format_hint(field_name, hint, annotations[hint]))
        for field_name, hint in get_type_hints(handler, include_extras=True).items()
        if field_name != "return" and _is_mapped(hint, annotations)
    ]

    if errors:
        call_name = getattr(handler, "__name__", str(handler))
        msg = f"`{call_name}` has arguments FastStream cannot inject."
        raise ExceptionGroup(msg, errors)


def _is_mapped(hint: Any, annotations: "Mapping[Any, Any]") -> bool:
    try:
        return hint in annotations
    except TypeError:
        # An unhashable hint cannot be a key, so it cannot be mapped.
        return False


def _format_hint(field_name: str, hint: Any, annotation: Any) -> str:
    message = (
        f"`{field_name}` is annotated with `{_describe(hint)}`, "
        "which FastStream cannot inject.\n"
    )

    if isinstance(annotation, UnderlyingDriverAnnotation):
        return (
            f"{message}Use the context annotation instead:\n"
            f"\n    from {annotation.module} import {annotation.name}\n"
        )

    return f"{message}Use the context annotation FastStream provides for it instead."


def _describe(hint: Any) -> str:
    if isinstance(hint, type):
        return f"{hint.__module__}.{hint.__qualname__}"

    return str(hint)
