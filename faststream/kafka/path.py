from faststream._internal.utils.path import AddressSyntax

KAFKA_ADDRESS_SYNTAX = AddressSyntax(
    replace_symbol=".*",
    patch_regex=lambda x: x.replace(r"\*", ".*"),
)
"""How a Kafka `pattern=` spells a Path parameter.

`pattern=` is the one Kafka argument that is compiled; a topic is a literal
and never meets the parameter parser.
"""
