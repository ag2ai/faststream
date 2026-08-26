import re
from collections.abc import Callable
from dataclasses import dataclass
from re import Pattern
from typing import Any

from faststream.exceptions import SetupError

PARAM_REGEX = re.compile(r"{([a-zA-Z0-9_]+)}")


@dataclass(frozen=True)
class AddressSyntax:
    """How one broker spells a wildcard where an Address template has a Path parameter.

    Attributes:
        replace_symbol: What each `{param}` becomes in the Broker address.
        patch_regex: Broker-specific fixups applied to the compiled capture regex.
    """

    replace_symbol: str
    patch_regex: Callable[[str], str]


class Address:
    """An Address template together with the Broker address it compiles to.

    `logs.{level}` written at a declaration site produces two values: the template
    itself, which documents the contract and which a Publisher formats arguments
    into, and the address actually handed to the infrastructure broker (`logs.*`).
    Both are read from here under distinct names, so neither can overwrite the other.

    Compilation is lazy and cached, so a template can still be decorated with a
    Router prefix after this object is built and nothing is compiled until a Broker
    address or a capture regex is asked for.
    """

    __slots__ = ("_compiled", "_syntax", "template")

    def __init__(self, template: str, syntax: AddressSyntax) -> None:
        self.template = template
        """The address as it was declared, e.g. `logs.{level}`."""

        self._syntax = syntax
        self._compiled: tuple[Pattern[str] | None, str] | None = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.template!r})"

    def __bool__(self) -> bool:
        """Whether an address was declared at all."""
        return bool(self.template)

    @property
    def broker_address(self) -> str:
        """The address as it reaches the broker, e.g. `logs.*` for `logs.{level}`."""
        return self._compile()[1]

    @property
    def regex(self) -> Pattern[str] | None:
        """Captures each Path parameter out of an incoming message's address."""
        return self._compile()[0]

    def add_prefix(self, prefix: str) -> "Address":
        """Decorate the template with a Router prefix; the Broker address follows."""
        if not prefix:
            return self

        return Address(f"{prefix}{self.template}", self._syntax)

    def _compile(self) -> tuple[Pattern[str] | None, str]:
        if self._compiled is None:
            self._compiled = compile_path(
                self.template,
                replace_symbol=self._syntax.replace_symbol,
                patch_regex=self._syntax.patch_regex,
            )

        return self._compiled


def compile_path(
    path: str,
    replace_symbol: str,
    patch_regex: Callable[[str], str] = lambda x: x,
    *,
    param_regex: str = "[^.]+",
) -> tuple[Pattern[str] | None, str]:
    path_regex = "^.*?"
    original_path = ""

    idx = 0
    params = set()
    duplicated_params = set()
    for match in PARAM_REGEX.finditer(path):
        param_name = match.groups("str")[0]

        path_regex += re.escape(path[idx : match.start()])
        path_regex += f"(?P<{param_name.replace('+', '')}>{param_regex})"

        original_path += path[idx : match.start()]
        original_path += replace_symbol

        if param_name in params:
            duplicated_params.add(param_name)
        else:
            params.add(param_name)

        idx = match.end()

    if duplicated_params:
        names = ", ".join(sorted(duplicated_params))
        ending = "s" if len(duplicated_params) > 1 else ""
        msg = f"Duplicated param name{ending} {names} at path {path}"
        raise SetupError(msg)

    if idx == 0:
        regex = None
    else:
        path_regex += re.escape(path[idx:]) + "$"
        regex = re.compile(patch_regex(path_regex))

    original_path += path[idx:]
    return regex, original_path


def match_path(pattern: Pattern[str] | None, subject: str) -> dict[str, Any]:
    """Match subject against pattern and return named groups, or {} if no match."""
    if pattern is not None and (match := pattern.match(subject)):
        return match.groupdict()
    return {}
