from faststream._internal.utils.path import Address, AddressSyntax

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
