import json
from collections.abc import Callable
from typing import Any, TextIO

import httpx
import pytest
import yaml

from faststream._internal._compat import IS_WINDOWS
from tests.cli.conftest import FastStreamCLIFactory, GenerateTemplateFactory
from tests.marks import require_aiokafka, skip_windows

json_asyncapi_doc = """
{
  "asyncapi": "2.6.0",
  "defaultContentType": "application/json",
  "info": {
    "title": "FastStream",
    "version": "0.1.0"
  },
  "servers": {
    "development": {
      "url": "localhost:9092",
      "protocol": "kafka",
      "protocolVersion": "auto"
    }
  },
  "channels": {
    "input_data:OnInputData": {
      "servers": [
        "development"
      ],
      "bindings": {
        "kafka": {
          "topic": "input_data",
          "bindingVersion": "0.4.0"
        }
      },
      "subscribe": {
        "message": {
          "$ref": "#/components/messages/input_data:OnInputData:Message"
        }
      }
    }
  },
  "components": {
    "messages": {
      "input_data:OnInputData:Message": {
        "title": "input_data:OnInputData:Message",
        "correlationId": {
          "location": "$message.header#/correlation_id"
        },
        "payload": {
          "$ref": "#/components/schemas/DataBasic"
        }
      }
    },
    "schemas": {
      "DataBasic": {
        "properties": {
          "data": {
            "type": "number"
          }
        },
        "required": [
          "data"
        ],
        "title": "DataBasic",
        "type": "object"
      }
    }
  }
}
"""

yaml_asyncapi_doc = """
asyncapi: 2.6.0
defaultContentType: application/json
info:
  title: FastStream
  version: 0.1.0
  description: ''
servers:
  development:
    url: 'localhost:9092'
    protocol: kafka
    protocolVersion: auto
channels:
  'input_data:OnInputData':
    servers:
      - development
    bindings: null
    kafka:
      topic: input_data
      bindingVersion: 0.4.0
    subscribe: null
    message:
      $ref: '#/components/messages/input_data:OnInputData:Message'
components:
  messages:
    'input_data:OnInputData:Message':
      title: 'input_data:OnInputData:Message'
      correlationId:
        location: '$message.header#/correlation_id'
      payload:
        $ref: '#/components/schemas/DataBasic'
  schemas:
    DataBasic:
      properties:
        data: null
        title: Data
        type: number
      required:
        - data
      title: DataBasic
      type: object
"""


app_code = """
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.specification import AsyncAPI
from faststream.kafka import KafkaBroker


class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = KafkaBroker("localhost:9092")
doc = AsyncAPI(broker)
app = FastStream(broker)


@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: DataBasic, logger: Logger) -> DataBasic:
    logger.info(msg)
    return DataBasic(data=msg.data + 1.0)
"""


@pytest.mark.slow()
@require_aiokafka
@pytest.mark.parametrize(
    ("commands", "load_schema"),
    (
        pytest.param(
            [],
            json.load,
            id="json",
        ),
        pytest.param(
            ["--yaml"],
            lambda f: yaml.load(f, Loader=yaml.BaseLoader),
            id="yaml",
        ),
    ),
)
def test_gen_asyncapi_for_kafka_app(
    commands: list[str],
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
    load_schema: Callable[[TextIO], Any],
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "docs",
            "gen",
            f"{app_path.stem}:doc",
            "--out",
            str(app_path.parent / "schema.json"),
            *commands,
        ) as cli_thread,
    ):
        assert cli_thread.process

        schema_path = app_path.parent / "schema.json"
        assert schema_path.exists()

        with schema_path.open() as f:
            schema = load_schema(f)

        assert schema
        schema_path.unlink()


@pytest.mark.slow()
def test_gen_wrong_path(faststream_cli: FastStreamCLIFactory) -> None:
    with faststream_cli("faststream", "docs", "gen", "non_existent:doc") as cli:
        assert cli.wait_for_stderr("No such file or directory")


@pytest.mark.slow()
@skip_windows
@require_aiokafka
def test_serve_asyncapi_docs_from_app(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli("faststream", "docs", "serve", f"{app_path.stem}:doc"),
    ):
        response = httpx.get("http://localhost:8000")
        assert "<title>FastStream AsyncAPI</title>" in response.text
        assert response.status_code == 200


@pytest.mark.slow()
@skip_windows
@require_aiokafka
@pytest.mark.parametrize(
    ("doc_filename", "doc"),
    (
        pytest.param("asyncapi.json", json_asyncapi_doc, id="json_schema"),
        pytest.param("asyncapi.yaml", yaml_asyncapi_doc, id="yaml_schema"),
    ),
)
def test_serve_asyncapi_docs_from_file(
    doc_filename: str,
    doc: str,
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
) -> None:
    with (
        generate_template(doc, filename=doc_filename) as doc_path,
        faststream_cli("faststream", "docs", "serve", str(doc_path)),
    ):
        response = httpx.get("http://localhost:8000")
        assert "<title>FastStream AsyncAPI</title>" in response.text
        assert response.status_code == 200
