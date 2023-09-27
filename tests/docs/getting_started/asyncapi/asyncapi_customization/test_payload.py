from docs.docs_src.getting_started.asyncapi.asyncapi_customization.payload_info import (
    app,
)
from faststream.asyncapi.generate import get_app_schema


def test_broker_customization():
    schema = get_app_schema(app).to_jsonable()

    assert schema == {
        "asyncapi": "2.6.0",
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0", "description": ""},
        "servers": {
            "development": {
                "url": "localhost:9092",
                "protocol": "kafka",
                "protocolVersion": "auto",
            }
        },
        "channels": {
            "OnInputData": {
                "servers": ["development"],
                "bindings": {
                    "kafka": {"topic": "input_data", "bindingVersion": "0.4.0"}
                },
                "subscribe": {
                    "message": {"$ref": "#/components/messages/OnInputDataMessage"}
                },
            },
            "Output_DataPublisher": {
                "servers": ["development"],
                "bindings": {
                    "kafka": {"topic": "output_data", "bindingVersion": "0.4.0"}
                },
                "publish": {
                    "message": {
                        "$ref": "#/components/messages/Output_DataPublisherMessage"
                    }
                },
            },
        },
        "components": {
            "messages": {
                "OnInputDataMessage": {
                    "title": "OnInputDataMessage",
                    "correlationId": {"location": "$message.header#/correlation_id"},
                    "payload": {"$ref": "#/components/schemas/DataBasic"},
                },
                "Output_DataPublisherMessage": {
                    "title": "Output_DataPublisherMessage",
                    "correlationId": {"location": "$message.header#/correlation_id"},
                    "payload": {"$ref": "#/components/schemas/DataBasic"},
                },
            },
            "schemas": {
                "DataBasic": {
                    "properties": {
                        "data": {
                            "description": "Float data example",
                            "examples": [0.5],
                            "minimum": 0,
                            "title": "Data",
                            "type": "number",
                        }
                    },
                    "required": ["data"],
                    "title": "DataBasic",
                    "type": "object",
                }
            },
        },
    }
