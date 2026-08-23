import re
from collections.abc import Callable
from dataclasses import dataclass
from re import Pattern
from typing import Any

from faststream.exceptions import SetupError

PARAM_REGEX = re.compile(r"{([a-zA-Z0-9_]+)}")


@dataclass(frozen=True, slots=True)
class AddressSyntax:
    """How one broker spells a wildcard where an Address template has a Path parameter.

    Attributes:
        replace_symbol: What each `{param}` becomes in the Broker address.
        patch_regex: Broker-specific fixups applied to the compiled capture regex.
        param_regex: What a single Path parameter is allowed to capture.
        verbatim: Whether addresses in this syntax are read as characters rather
            than as templates, so that a `{` in one is a `{` and nothing more.
    """

    replace_symbol: str
    patch_regex: Callable[[str], str]
    param_regex: str = "[^.]+"
    verbatim: bool = False

    def compile(self, template: str) -> tuple[Pattern[str] | None, str]:
        """Turn an Address template into its capture regex and its Broker address."""
        if self.verbatim:
            return None, template

        return compile_path(
            template,
            replace_symbol=self.replace_symbol,
            patch_regex=self.patch_regex,
            param_regex=self.param_regex,
        )


VERBATIM_ADDRESS_SYNTAX = AddressSyntax(
    replace_symbol="",
    patch_regex=str,
    verbatim=True,
)
"""The syntax of an address that is not a template: what it says is what it names."""


class Address:
    """An Address template together with the Broker address it compiles to.

    `logs.{level}` written at a declaration site produces two values: the template
    itself, which documents the contract and which a Publisher formats arguments
    into, and the address actually handed to the infrastructure broker (`logs.*`).
    Both are read from here under distinct names, so neither can overwrite the other.

    Compilation is lazy, so an address that is only ever declared is never compiled:
    `add_prefix` builds a second Address out of the first, and Preparation is what
    decides which of the two an endpoint ends up holding. It is also one-shot rather
    than change-tracking, which is safe for the same reason — the Address an endpoint
    holds was built out of a template that was already final, and it is thrown away
    again when Preparation is undone.
    """

    __slots__ = ("_compiled", "_syntax", "config_key", "template")

    def __init__(
        self,
        template: str,
        syntax: AddressSyntax,
        config_key: str | None = None,
    ) -> None:
        self.template = template
        """The address as it was declared, e.g. `logs.{level}`."""

        self.config_key = config_key
        """The Config value this address was resolved from, if it was one.

        A resolved value is otherwise indistinguishable from a literal one, and by
        the time compilation fails there is nothing left to point the user at. The
        key travels with the address so that the failure can name what to fix.
        """

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

    def describe(self) -> str:
        """Name this address the way an error message should, source included."""
        if self.config_key is None:
            return repr(self.template)

        return f"{self.template!r} (Config value {self.config_key!r})"

    @classmethod
    def literal(cls, value: str, config_key: str | None = None) -> "Address":
        """An Address read as characters: what it says is what it subscribes to.

        Kafka topics are the case this exists for. They carry no Address template
        — a topic is handed to the broker verbatim — so reading one as a template
        would report capture groups that nothing ever fills, and a `Path()`
        parameter naming one would be accepted at `connect()` and then never
        supplied a value. The verbatim syntax travels with the address, so a
        Router prefix decorating it later leaves it verbatim too.
        """
        return cls(value, VERBATIM_ADDRESS_SYNTAX, config_key)

    def add_prefix(self, prefix: str) -> "Address":
        """Decorate the template with a Router prefix; the Broker address follows."""
        if not prefix:
            return self

        return Address(f"{prefix}{self.template}", self._syntax, self.config_key)

    def _compile(self) -> tuple[Pattern[str] | None, str]:
        if self._compiled is None:
            try:
                self._compiled = self._syntax.compile(self.template)
            except SetupError as e:
                if self.config_key is None:
                    raise
                msg = f"{e} It was supplied as Config value {self.config_key!r}."
                raise SetupError(msg) from e

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

    check_template_braces(path)

    if idx == 0:
        regex = None
    else:
        path_regex += re.escape(path[idx:]) + "$"
        regex = re.compile(patch_regex(path_regex))

    original_path += path[idx:]
    return regex, original_path


def check_template_braces(path: str) -> None:
    """Refuse an address whose braces do not spell out Path parameters.

    A half-rendered `test.${ENV` — out of the environment, or out of a Config
    value — would otherwise compile into a literal address and silently subscribe
    somewhere nobody publishes to. A literal brace is not expressible here, which
    is what makes rejecting one safe.
    """
    leftovers = PARAM_REGEX.sub("", path)

    if "{" in leftovers or "}" in leftovers:
        msg = (
            f"Address {path!r} is not a valid Address template: a `{{` or `}}` in it "
            f"is not part of a `{{param}}` Path parameter."
        )
        raise SetupError(msg)


def match_path(pattern: Pattern[str] | None, subject: str) -> dict[str, Any]:
    """Match subject against pattern and return named groups, or {} if no match."""
    if pattern is not None and (match := pattern.match(subject)):
        return match.groupdict()
    return {}
