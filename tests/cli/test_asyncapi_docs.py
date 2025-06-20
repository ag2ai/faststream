import json
import urllib.request

import pytest
import yaml

from faststream._compat import IS_WINDOWS
from tests.marks import python310, require_aiokafka

pytestmark = [
    pytest.mark.slow,
    python310,
    pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows"),
]

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
from faststream.kafka import KafkaBroker


class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: DataBasic, logger: Logger) -> DataBasic:
    logger.info(msg)
    return DataBasic(data=msg.data + 1.0)

"""


@require_aiokafka
@pytest.mark.parametrize(
    ("doc_flag", "load_schema"),
    [
        pytest.param([], lambda f: json.load(f), id="json"),
        pytest.param(
            ["--yaml"], lambda f: yaml.load(f, Loader=yaml.BaseLoader), id="yaml"
        ),
    ],
)
def test_gen_asyncapi_for_kafka_app(
    generate_template, faststream_cli, doc_flag, load_schema
):
    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "docs",
            "gen",
            *doc_flag,
            f"{app_path.stem}:app",
            "--out",
            str(app_path.parent / "schema.json"),
        ],
    ) as cli_thread:
        pass
    assert cli_thread.process.returncode == 0

    schema_path = app_path.parent / "schema.json"
    assert schema_path.exists()

    with schema_path.open() as f:
        schema = load_schema(f)

    assert schema
    schema_path.unlink()


def test_gen_wrong_path(faststream_cli) -> None:
    with faststream_cli(
        [
            "faststream",
            "docs",
            "gen",
            "non_existent:app",
        ],
    ) as cli_thread:
        pass
    assert cli_thread.process.returncode == 2
    assert "No such file or directory" in cli_thread.process.stderr.read()


@require_aiokafka
def test_serve_asyncapi_docs_from_app(
    generate_template,
    faststream_cli,
):
    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "docs",
            "serve",
            f"{app_path.stem}:app",
        ],
    ), urllib.request.urlopen("http://localhost:8000") as response:
        assert "<title>FastStream AsyncAPI</title>" in response.read().decode()
        assert response.getcode() == 200


@pytest.mark.parametrize(
    ("doc_filename", "doc"),
    [
        pytest.param("asyncapi.json", json_asyncapi_doc, id="json_schema"),
        pytest.param("asyncapi.yaml", yaml_asyncapi_doc, id="yaml_schema"),
    ],
)
@require_aiokafka
def test_serve_asyncapi_docs_from_file(
    doc_filename,
    doc,
    generate_template,
    faststream_cli,
):
    with generate_template(doc, filename=doc_filename) as doc_path, faststream_cli(
        [
            "faststream",
            "docs",
            "serve",
            str(doc_path),
        ],
    ), urllib.request.urlopen("http://localhost:8000") as response:
        assert "<title>FastStream AsyncAPI</title>" in response.read().decode()
        assert response.getcode() == 200
