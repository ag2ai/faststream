# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/099_Test_Service.ipynb.

# %% auto 0
__all__ = ['logger', 'ModelType', 'ModelTrainingRequest', 'EventData', 'RealtimeData', 'TrainingDataStatus',
           'TrainingModelStatus', 'ModelMetrics', 'Prediction', 'create_ws_server']

# %% ../nbs/099_Test_Service.ipynb 1
from typing import *

import os
import json
import yaml
from pathlib import Path
from copy import deepcopy
from datetime import datetime
from os import environ
from enum import Enum
import httpx

from confluent_kafka import Producer, Consumer
from fastapi import status, Depends, HTTPException, Request, Response
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from pydantic import validator, BaseModel, Field, HttpUrl, EmailStr, NonNegativeInt

from fast_kafka_api.application import (
    FastKafkaAPI,
    KafkaMessage,
    ConsumeCallable,
    ProduceCallable,
)

# from fast_kafka_api.asyncapi import KafkaMessage, ConsumeCallable, ProduceCallable
from ._components.logger import get_logger

# %% ../nbs/099_Test_Service.ipynb 3
logger = get_logger(__name__)

# %% ../nbs/099_Test_Service.ipynb 5
class ModelType(str, Enum):
    churn = "churn"
    propensity_to_buy = "propensity_to_buy"


class ModelTrainingRequest(BaseModel):
    AccountId: NonNegativeInt = Field(
        ..., example=202020, description="ID of an account"
    )
    ModelName: ModelType = Field(..., example="churn", description="ID of an account")
    total_no_of_records: NonNegativeInt = Field(
        ...,
        example=1_000_000,
        description="total number of records (rows) to be ingested",
    )


class EventData(BaseModel):
    """
    A sequence of events for a fixed account_id
    """

    AccountId: NonNegativeInt = Field(
        ..., example=202020, description="ID of an account"
    )
    Application: Optional[str] = Field(
        None,
        example="DriverApp",
        description="Name of the application in case there is more than one for the AccountId",
    )
    DefinitionId: str = Field(
        ...,
        example="appLaunch",
        description="name of the event",
        min_length=1,
    )
    OccurredTime: datetime = Field(
        ...,
        example="2021-03-28T00:34:08",
        description="local time of the event",
    )
    OccurredTimeTicks: NonNegativeInt = Field(
        ...,
        example=1616891648496,
        description="local time of the event as the number of ticks",
    )
    PersonId: NonNegativeInt = Field(
        ..., example=12345678, description="ID of a person"
    )


class RealtimeData(BaseModel):
    event_data: EventData = Field(
        ...,
        example=dict(
            AccountId=202020,
            Application="DriverApp",
            DefinitionId="appLaunch",
            OccurredTime="2021-03-28T00:34:08",
            OccurredTimeTicks=1616891648496,
            PersonId=12345678,
        ),
        description="realtime event data",
    )
    make_prediction: bool = Field(
        ..., example=True, description="trigger prediction message in prediction topic"
    )


class TrainingDataStatus(BaseModel):
    AccountId: NonNegativeInt = Field(
        ..., example=202020, description="ID of an account"
    )
    no_of_records: NonNegativeInt = Field(
        ...,
        example=12_345,
        description="number of records (rows) ingested",
    )
    total_no_of_records: NonNegativeInt = Field(
        ...,
        example=1_000_000,
        description="total number of records (rows) to be ingested",
    )


class TrainingModelStatus(BaseModel):
    AccountId: NonNegativeInt = Field(
        ..., example=202020, description="ID of an account"
    )
    current_step: NonNegativeInt = Field(
        ...,
        example=0,
        description="number of records (rows) ingested",
    )
    current_step_percentage: float = Field(
        ...,
        example=0.21,
        description="the percentage of the current step completed",
    )
    total_no_of_steps: NonNegativeInt = Field(
        ...,
        example=1_000_000,
        description="total number of steps for training the model",
    )


class ModelMetrics(BaseModel):
    """The standard metrics for classification models.

    The most important metrics is AUC for unbalanced classes such as churn. Metrics such as
    accuracy are not very useful since they are easily maximized by outputting the most common
    class all the time.
    """

    AccountId: NonNegativeInt = Field(
        ..., example=202020, description="ID of an account"
    )
    Application: Optional[str] = Field(
        None,
        example="DriverApp",
        description="Name of the application in case there is more than one for the AccountId",
    )
    timestamp: datetime = Field(
        ...,
        example="2021-03-28T00:34:08",
        description="UTC time when the model was trained",
    )
    model_type: ModelType = Field(
        ...,
        example="churn",
        description="Name of the model used (churn, propensity to buy)",
    )
    auc: float = Field(
        ..., example=0.91, description="Area under ROC curve", ge=0.0, le=1.0
    )
    f1: float = Field(..., example=0.89, description="F-1 score", ge=0.0, le=1.0)
    precission: float = Field(
        ..., example=0.84, description="precission", ge=0.0, le=1.0
    )
    recall: float = Field(..., example=0.82, description="recall", ge=0.0, le=1.0)
    accuracy: float = Field(..., example=0.82, description="accuracy", ge=0.0, le=1.0)


class Prediction(BaseModel):
    AccountId: NonNegativeInt = Field(
        ..., example=202020, description="ID of an account"
    )
    Application: Optional[str] = Field(
        None,
        example="DriverApp",
        description="Name of the application in case there is more than one for the AccountId",
    )
    PersonId: NonNegativeInt = Field(
        ..., example=12345678, description="ID of a person"
    )
    prediction_time: datetime = Field(
        ...,
        example="2021-03-28T00:34:08",
        description="UTC time of prediction",
    )
    model_type: ModelType = Field(
        ...,
        example="churn",
        description="Name of the model used (churn, propensity to buy)",
    )
    score: float = Field(
        ...,
        example=0.4321,
        description="Prediction score (e.g. the probability of churn in the next 28 days)",
        ge=0.0,
        le=1.0,
    )

# %% ../nbs/099_Test_Service.ipynb 6
_total_no_of_records = 0
_no_of_records_received = 0


def create_ws_server(assets_path: Path = Path("./assets")) -> FastKafkaAPI:
    title = "Example for FastKafkaAPI"
    description = "A simple example on how to use FastKafkaAPI"
    version = "0.0.1"
    openapi_url = "/openapi.json"
    favicon_url = "/assets/images/favicon.ico"

    contact = dict(name="airt.ai", url="https://airt.ai", email="info@airt.ai")

    kafka_brokers = {
        "localhost": {
            "url": "kafka",
            "description": "local development kafka",
            "port": 9092,
        },
        "staging": {
            "url": "kafka.staging.acme.com",
            "description": "staging kafka",
            "port": 9092,
            "protocol": "kafka-secure",
            "security": {"type": "plain"},
        },
        "production": {
            "url": "kafka.infobip.acme.com",
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
        "group_id": f"{kafka_server_url}:{kafka_server_port}_group",
        "auto_offset_reset": "earliest",
    }
    if "KAFKA_API_KEY" in environ:
        kafka_config = {
            **kafka_config,
            **{
                "security_protocol": "SASL_SSL",
                "sasl_mechanisms": "PLAIN",
                "sasl_username": environ["KAFKA_API_KEY"],
                "sasl_password": environ["KAFKA_API_SECRET"],
            },
        }

    app = FastKafkaAPI(
        title=title,
        contact=contact,
        kafka_brokers=kafka_brokers,
        kafka_config=kafka_config,
        description=description,
        version=version,
        docs_url=None,
        redoc_url=None,
    )

    @app.get("/docs", include_in_schema=False)
    def overridden_swagger():
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=title,
            swagger_favicon_url=favicon_url,
        )

    @app.get("/redoc", include_in_schema=False)
    def overridden_redoc():
        return get_redoc_html(
            openapi_url=openapi_url,
            title=title,
            redoc_favicon_url=favicon_url,
        )

    @app.post("/from_kafka_start")
    async def from_kafka_start(training_request: ModelTrainingRequest):
        global _total_no_of_records
        global _no_of_records_received

        _total_no_of_records = training_request.total_no_of_records
        _no_of_records_received = 0

    @app.get("/from_kafka_end")
    async def from_kafka_end():
        pass

    @app.consumes()  # type: ignore
    async def on_training_data(msg: EventData):
        # ToDo: this is not showing up in logs
        logger.debug(f"msg={msg}")
        global _total_no_of_records
        global _no_of_records_received
        _no_of_records_received = _no_of_records_received + 1

        if _no_of_records_received % 100 == 0:
            training_data_status = TrainingDataStatus(
                AccountId=EventData.AccountId,
                no_of_records=_no_of_records_received,
                total_no_of_records=_total_no_of_records,
            )
            app.produce("training_data_status", training_data_status)

    @app.consumes()  # type: ignore
    async def on_realitime_data(msg: RealtimeData):
        pass

    @app.produces()  # type: ignore
    async def to_training_data_status(msg: TrainingDataStatus) -> TrainingDataStatus:
        logger.debug(f"on_training_data_status(msg={msg}, kafka_msg={kafka_msg})")
        return msg

    @app.produces()  # type: ignore
    async def to_training_model_status(msg: str) -> TrainingModelStatus:
        logger.debug(f"on_training_model_status(msg={msg}, kafka_msg={kafka_msg})")
        return TrainingModelStatus()

    @app.produces()  # type: ignore
    async def to_model_metrics(msg: ModelMetrics) -> ModelMetrics:
        logger.debug(f"on_training_model_status(msg={msg}, kafka_msg={kafka_msg})")
        return msg

    @app.produces()  # type: ignore
    async def to_prediction(msg: Prediction) -> Prediction:
        logger.debug(f"on_realtime_data_status(msg={msg},, kafka_msg={kafka_msg})")
        return msg

    return app
