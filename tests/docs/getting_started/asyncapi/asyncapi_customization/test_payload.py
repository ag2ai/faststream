from docs.docs_src.getting_started.asyncapi.asyncapi_customization.payload_info import (
    docs_obj,
)


def test_payload_customization() -> None:
    schema = docs_obj.to_jsonable()

    assert schema["components"]["schemas"] == {
        "DataBasic": {
            "properties": {
                "data": {
                    "description": "Float data example",
                    "examples": [0.5],
                    "minimum": 0,
                    "title": "Data",
                    "type": "number",
                },
            },
            "required": ["data"],
            "title": "DataBasic",
            "type": "object",
        },
    }
