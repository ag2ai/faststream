"""What Preparation asks of a Subscriber's addresses before any message arrives.

An Address template promises two things: that the Broker address it compiles to is
the one the endpoint subscribes with, and that every `Path()` parameter the handler
declares is captured out of an incoming message's address. Neither promise can be
checked at declaration time, because a Config value is not known then — so both are
checked once, in Preparation, against the resolved addresses.

Preparation rather than `connect()` so that the check reaches a Subscriber registered
after its Broker connected, which has no `connect()` of its own to hang it on and
prepares at its own `start()` instead.

Checking here rather than per message is the point: a `Path()` parameter that can
never be filled is a misconfigured deployment, and a misconfigured deployment should
refuse to boot rather than fail on part of its traffic.
"""

from collections.abc import Iterable, Iterator
from inspect import Parameter, signature, unwrap
from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY, PATH_CONTEXT_PREFIX
from faststream._internal.context import Context
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from re import Pattern

    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream._internal.utils.path import Address


def check_subscription_addresses(
    addresses: Iterable["Address"],
    calls: "CallsCollection[Any]",
) -> None:
    """Refuse a Subscriber whose addresses cannot deliver what was declared."""
    declared = [a for a in addresses if a]

    if not declared:
        return

    # Compiling is itself a check: an address whose braces do not spell out Path
    # parameters is refused here rather than subscribed to.
    regexes = [a.regex for a in declared]

    for name in sorted(_required_path_parameters(calls)):
        missing = [
            address
            for address, regex in zip(declared, regexes, strict=True)
            if not _captures(regex, name)
        ]

        if missing:
            raise SetupError(_unsatisfiable_path_message(name, missing))


def _captures(regex: "Pattern[str] | None", name: str) -> bool:
    return regex is not None and name in regex.groupindex


def _unsatisfiable_path_message(name: str, missing: list["Address"]) -> str:
    addresses = ", ".join(address.describe() for address in missing)
    subject = "addresses" if len(missing) > 1 else "address"
    verb = "hold" if len(missing) > 1 else "holds"

    return (
        f"`Path()` parameter {name!r} has no default, but the {subject} "
        f"{addresses} this Subscriber listens on {verb} no {{{name}}} to fill it "
        f"from. Add {{{name}}} to the address, or give the parameter a default."
    )


def _required_path_parameters(calls: "CallsCollection[Any]") -> set[str]:
    """Every `Path()` parameter that must be filled for the handlers to be callable."""
    return {
        name
        for call in calls
        for name, field in _path_parameters(call.handler._composed_call)
        if field.default is EMPTY
    }


def _path_parameters(call: Any) -> Iterator[tuple[str, Context]]:
    func = unwrap(call)

    try:
        params = signature(func).parameters
    except (TypeError, ValueError):  # pragma: no cover
        # A builtin or a C callable has no inspectable signature.
        return

    for param in params.values():
        if (field := _path_field(param)) is not None:
            yield field.name or param.name, field


def _path_field(param: Parameter) -> Context | None:
    """The `Path()` marker on a parameter, however it was spelled."""
    annotated = getattr(param.annotation, "__metadata__", ())

    for candidate in (param.default, *annotated):
        if isinstance(candidate, Context) and candidate.prefix == PATH_CONTEXT_PREFIX:
            return candidate

    return None
