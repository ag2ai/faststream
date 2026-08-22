import pytest

from faststream._internal.utils.path import Address, AddressSyntax
from faststream.exceptions import SetupError

SYNTAX = AddressSyntax(
    replace_symbol="*",
    patch_regex=lambda x: x.replace(r"\*", ".*"),
)


def test_template_and_broker_address_are_separate_reads() -> None:
    address = Address("logs.{level}", SYNTAX)

    assert address.template == "logs.{level}"
    assert address.broker_address == "logs.*"


def test_a_literal_address_compiles_to_itself() -> None:
    address = Address("logs.info", SYNTAX)

    assert address.template == "logs.info"
    assert address.broker_address == "logs.info"
    assert address.regex is None


def test_regex_captures_each_path_parameter() -> None:
    regex = Address("logs.{level}", SYNTAX).regex

    assert regex is not None
    assert regex.match("logs.info").groupdict() == {"level": "info"}


def test_a_prefix_decorates_the_template_and_the_broker_address_follows() -> None:
    address = Address("logs.{level}", SYNTAX).add_prefix("prefix_")

    assert address.template == "prefix_logs.{level}"
    assert address.broker_address == "prefix_logs.*"


def test_an_empty_prefix_leaves_the_address_alone() -> None:
    address = Address("logs.{level}", SYNTAX)

    assert address.add_prefix("") is address


def test_an_address_is_falsy_when_nothing_was_declared() -> None:
    assert not Address("", SYNTAX)
    assert Address("logs.info", SYNTAX)


def test_a_brace_that_is_not_a_path_parameter_is_not_an_address() -> None:
    address = Address("test.${ENV", SYNTAX)

    with pytest.raises(SetupError, match=r"test\.\$\{ENV"):
        assert address.broker_address


def test_a_half_written_placeholder_is_not_an_address() -> None:
    with pytest.raises(SetupError):
        assert Address("logs.{level", SYNTAX).regex

    with pytest.raises(SetupError):
        assert Address("logs.level}", SYNTAX).regex


def test_a_failure_names_the_config_value_the_address_came_from() -> None:
    address = Address("test.${ENV", SYNTAX, config_key="SUBJ")

    with pytest.raises(SetupError, match="SUBJ"):
        assert address.broker_address


def test_a_prefix_carries_the_config_value_along() -> None:
    address = Address("logs.{level}", SYNTAX, config_key="SUBJ")

    assert address.add_prefix("prefix_").config_key == "SUBJ"


def test_compilation_happens_once_and_is_not_repeated() -> None:
    calls = 0

    def counting_patch(regex: str) -> str:
        nonlocal calls
        calls += 1
        return regex

    address = Address(
        "logs.{level}",
        AddressSyntax(replace_symbol="*", patch_regex=counting_patch),
    )

    assert address.broker_address
    assert address.regex
    assert address.broker_address

    assert calls == 1
