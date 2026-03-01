from docs.docs_src.getting_started.asyncapi.asyncapi_customization.payload_info import (
    app,
)


def test_payload_customization() -> None:
    app.schema.schema_version = "2.6.0"
    schema = app.schema.to_specification().to_jsonable()

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
