from faststream._internal.utils.path import Address, AddressSyntax

SYNTAX = AddressSyntax(
    replace_symbol="*",
    patch_regex=lambda x: x.replace(r"\*", ".*"),
)


def test_a_literal_address_stays_characters_under_a_router_prefix() -> None:
    address = Address.literal("logs.{{2}}.{level}").add_prefix("prefix_")

    assert (address.template, address.broker_address, address.regex) == (
        "prefix_logs.{{2}}.{level}",
        "prefix_logs.{{2}}.{level}",
        None,
    )


def test_describe_names_the_template_as_it_was_declared() -> None:
    assert Address("logs.{level}", SYNTAX).describe() == "'logs.{level}'"
