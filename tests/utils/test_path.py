from faststream._internal.utils.path import (
    Address,
    AddressSyntax,
    restore_literal_braces,
)

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


def test_escaped_braces_are_literal() -> None:
    address = Address("cache{{shard}}", SYNTAX)

    assert address.template == "cache{shard}"
    assert address.broker_address == "cache{shard}"
    assert address.regex is None


def test_escaped_braces_with_parameters() -> None:
    address = Address("cache{{shard}}.logs.{level}", SYNTAX)

    assert address.template == "cache{shard}.logs.{level}"
    assert address.broker_address == "cache{shard}.logs.*"
    assert address.regex is not None
    assert address.regex.match("cache{shard}.logs.info").groupdict() == {"level": "info"}


def test_restore_literal_braces_leaves_parameters_alone() -> None:
    assert (
        restore_literal_braces("cache{{shard}}.logs.{level}")
        == "cache{shard}.logs.{level}"
    )


def test_restore_literal_braces_without_escaped_braces() -> None:
    assert restore_literal_braces("logs.{level}") == "logs.{level}"


def test_a_prefix_decorates_the_declaration_not_the_template() -> None:
    """Prefixing a restored `{shard}` would let the parser read it as a parameter."""
    address = Address("cache{{shard}}", SYNTAX).add_prefix("prefix_")

    assert address.template == "prefix_cache{shard}"
    assert address.broker_address == "prefix_cache{shard}"
    assert address.regex is None
