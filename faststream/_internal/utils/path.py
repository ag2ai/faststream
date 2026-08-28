import re
from collections.abc import Callable
from dataclasses import dataclass
from re import Pattern
from typing import Any

from faststream.exceptions import SetupError

PARAM_REGEX = re.compile(r"(?<!\{)\{([a-zA-Z0-9_]+)\}(?!\})")


ESCAPED_LEFT_BRACE = "__faststream_escaped_left__"
ESCAPED_RIGHT_BRACE = "__faststream_escaped_right__"


def _escape_literal_braces(path: str) -> str:
    result = ""
    idx = 0
    while idx < len(path):
        if path.startswith("{{", idx):
            result += ESCAPED_LEFT_BRACE
            idx += 2
        elif path.startswith("}}", idx):
            result += ESCAPED_RIGHT_BRACE
            idx += 2
        else:
            result += path[idx]
            idx += 1
    return result


def _restore_braces(fragment: str) -> str:
    """Put a declaration's literal braces back into an address."""
    return fragment.replace(ESCAPED_LEFT_BRACE, "{").replace(ESCAPED_RIGHT_BRACE, "}")


def _restore_braces_in_regex(fragment: str) -> str:
    """Put a declaration's literal braces back into a capture regex.

    Escaped, unlike an address: a bare `{2}` in a pattern is a quantifier on
    whatever precedes it, not the two characters somebody wrote.
    """
    return fragment.replace(ESCAPED_LEFT_BRACE, r"\{").replace(
        ESCAPED_RIGHT_BRACE,
        r"\}",
    )


def restore_literal_braces(path: str) -> str:
    """Undo a declaration's `{{`/`}}` escaping, leaving each `{param}` alone.

    Goes through the same scan the parameter parser uses rather than looking for
    the pairs a second time, so one function decides what an escape is.
    """
    return _restore_braces(_escape_literal_braces(path))


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

    A declaration escapes a literal brace as `{{`, which is syntax for the parameter
    parser and for nothing else. It is undone once, on the way in, so that nobody
    reading a template afterwards has to know the escape exists.

    Compilation is lazy and cached, so a template can still be decorated with a
    Router prefix after this object is built and nothing is compiled until a Broker
    address or a capture regex is asked for.
    """

    __slots__ = ("_compiled", "_declaration", "_syntax", "template")

    def __init__(self, declaration: str, syntax: AddressSyntax) -> None:
        self._declaration = declaration
        """The declaration verbatim, escapes and all: what the parser reads."""

        self.template = restore_literal_braces(declaration)
        """The address as it was declared, e.g. `logs.{level}`."""

        self._syntax = syntax
        self._compiled: tuple[Pattern[str] | None, str] | None = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._declaration!r})"

    def __bool__(self) -> bool:
        """Whether an address was declared at all."""
        return bool(self._declaration)

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

        return Address(f"{prefix}{self._declaration}", self._syntax)

    def _compile(self) -> tuple[Pattern[str] | None, str]:
        if self._compiled is None:
            self._compiled = compile_path(
                self._declaration,
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
    path = _escape_literal_braces(path)
    path_regex = "^.*?"
    original_path = ""

    idx = 0
    params = set()
    duplicated_params = set()
    for match in PARAM_REGEX.finditer(path):
        param_name = match.groups("str")[0]

        path_regex += _restore_braces_in_regex(re.escape(path[idx : match.start()]))
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
        path_regex += _restore_braces_in_regex(re.escape(path[idx:])) + "$"
        regex = re.compile(patch_regex(path_regex))

    original_path += path[idx:]
    return regex, _restore_braces(original_path)


def match_path(pattern: Pattern[str] | None, subject: str) -> dict[str, Any]:
    """Match subject against pattern and return named groups, or {} if no match."""
    if pattern is not None and (match := pattern.match(subject)):
        return match.groupdict()
    return {}
