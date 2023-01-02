FastKafkaAPI
================

<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->

## Install

``` sh
pip install fast-kafka-api
```

## How to use

We’ll create a simple example of using FastKafkaAPI to consume input
data from a topic and produce predictions using it into an another
topic.

First, we define message types used in Kafka topic as follows:

``` python
from typing import List
from pydantic import BaseModel, Field, NonNegativeInt

class InputData(BaseModel):
    user_id: NonNegativeInt = Field(
        ..., example=202020, description="ID of a user"
    )
    feature_1: List[float] = Field(
        ...,
        example=[1.2, 2.3, 4.5, 6.7, 0.1],
        description="input feature 1",
    )
    feature_2: List[int] = Field(
        ...,
        example=[2, 4, 3, 1, 0],
        description="input feature 2",
    )

class Prediction(BaseModel):
    user_id: NonNegativeInt = Field(
        ..., example=202020, description="ID of a user"
    )
    score: float = Field(
        ...,
        example=0.4321,
        description="Prediction score (e.g. the probability of churn in the next 28 days)",
        ge=0.0,
        le=1.0,
    )
```

Next, we create an FastKafkaAPI object initialized to use Kafka brokers:

``` python
from os import environ
from fast_kafka_api.application import FastKafkaAPI

title = "Example for FastKafkaAPI"
description = "A simple example on how to use FastKafkaAPI"
version = "0.0.1"
openapi_url = "/openapi.json"

contact = dict(name="airt.ai", url="https://airt.ai", email="info@airt.ai")

kafka_brokers = {
    "localhost": {
        "url": "kafka",
        "description": "local development kafka",
        "port": 9092,
    },
    "production": {
        "url": "kafka.acme.com",
        "description": "production kafka",
        "port": 9092,
        "protocol": "kafka-secure",
        "security": {"type": "plain"},
    },
}

kafka_server_url = environ["KAFKA_HOSTNAME"]
kafka_server_port = environ["KAFKA_PORT"]
kafka_config = {
    "bootstrap_servers": f"{kafka_server_url}:{kafka_server_port}",
}

app = FastKafkaAPI(
    title=title,
    contact=contact,
    version=version,
    kafka_brokers=kafka_brokers,
    kafka_config=kafka_config,
    description=description,
    docs_url=None,
    redoc_url=None,
)
```

    [INFO] fast_kafka_api._components.asyncapi: ok

Next, we use decorators to define which functions are called in input
data (consuming data) and which functions are producing messages
(producing data):

``` python
import unittest

@app.consumes(topic="input_data")
async def on_input_data(msg: InputData):
    print(f"msg={msg}")
    score = await model.predict(feature_1=msg.feature_1, feature_2=msg.feature_2)
    await to_predictions(user_id=msg.user_id, score=score)

@app.produces(topic="predictions")
async def to_predictions(user_id: int, score: float) -> Prediction:
    prediction = Prediction(user_id=user_id, score=score)
    print(f"prediction={prediction}")
    return prediction
```

Finally, start the service using uvicorn:

``` python
import uvicorn

uvicorn.run(app, host="0.0.0.0", port=4000)
```

    INFO:     Started server process [2296]
    INFO:     Waiting for application startup.

    [INFO] fast_kafka_api._components.asyncapi: Async specifications generated at: 'asyncapi/spec/asyncapi.yml'
    [INFO] fast_kafka_api._components.asyncapi: Async docs generated at 'asyncapi/docs'
    [INFO] fast_kafka_api._components.asyncapi: Output of '$ npx -y -p @asyncapi/generator ag asyncapi/spec/asyncapi.yml @asyncapi/html-template -o asyncapi/docs --force-write'

    Done! ✨
    Check out your shiny new generated files at /work/fast-kafka-api/nbs/asyncapi/docs.


    [INFO] fast_kafka_api._components.aiokafka_loop: Consumer created.

    INFO:     Application startup complete.
    INFO:     Uvicorn running on http://0.0.0.0:4000 (Press CTRL+C to quit)

    [INFO] fast_kafka_api._components.aiokafka_loop: Consumer started.
    [INFO] aiokafka.consumer.subscription_state: Updating subscribed topics to: frozenset({'input_data'})
    [INFO] aiokafka.consumer.consumer: Subscribed to topic(s): {'input_data'}
    [INFO] fast_kafka_api._components.aiokafka_loop: Consumer subscribed.
    [INFO] aiokafka.consumer.group_coordinator: Metadata for topic has changed from {} to {'input_data': 1}. 

    INFO:     Shutting down
    INFO:     Waiting for application shutdown.

    [INFO] fast_kafka_api._components.aiokafka_loop: Consumer stopped.

    INFO:     Application shutdown complete.
    INFO:     Finished server process [2296]
