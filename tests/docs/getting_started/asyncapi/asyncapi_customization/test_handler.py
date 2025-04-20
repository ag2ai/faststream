from dirty_equals import IsPartialDict

from docs.docs_src.getting_started.asyncapi.asyncapi_customization.custom_handler import (
    docs_obj,
)


def test_handler_customization() -> None:
    schema = docs_obj.to_jsonable()

    (subscriber_key, subscriber_value), (publisher_key, publisher_value) = schema[
        "channels"
    ].items()

    assert subscriber_key == "input_data:Consume", subscriber_key
    assert subscriber_value == IsPartialDict({
        "servers": ["development"],
        "bindings": {"kafka": {"topic": "input_data", "bindingVersion": "0.4.0"}},
        "publish": {
            "message": {"$ref": "#/components/messages/input_data:Consume:Message"},
        },
    }), subscriber_value
    desc = subscriber_value["description"]
    assert (  # noqa: PT018
        "Consumer function\n\n" in desc
        and "Args:\n" in desc
        and "    msg: input msg" in desc
    ), desc

    assert publisher_key == "output_data:Produce", publisher_key
    assert publisher_value == {
        "description": "My publisher description",
        "servers": ["development"],
        "bindings": {"kafka": {"topic": "output_data", "bindingVersion": "0.4.0"}},
        "subscribe": {
            "message": {"$ref": "#/components/messages/output_data:Produce:Message"}
        },
    }
