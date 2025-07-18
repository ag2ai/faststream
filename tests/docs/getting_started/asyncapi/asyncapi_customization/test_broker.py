from docs.docs_src.getting_started.asyncapi.asyncapi_customization.custom_broker import (
    asyncapi,
)


def test_broker_customization() -> None:
    schema = asyncapi.to_jsonable()

    assert schema["servers"] == {
        "development": {
            "url": "non-sensitive-url:9092",
            "protocol": "kafka",
            "description": "Kafka broker running locally",
            "protocolVersion": "auto",
        },
    }
