from docs.docs_src.getting_started.asyncapi.asyncapi_customization.custom_broker import (
    app,
)


def test_broker_customization() -> None:
    app.schema.schema_version = "2.6.0"
    schema = app.schema.to_specification().to_jsonable()

    assert schema["servers"] == {
        "development": {
            "url": "non-sensitive-url:9092",
            "protocol": "kafka",
            "description": "Kafka broker running locally",
            "protocolVersion": "auto",
        },
    }
