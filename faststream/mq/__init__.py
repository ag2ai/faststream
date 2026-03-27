from faststream._internal.testing.app import TestApp

from .annotations import MQBroker, MQMessage, MQProducer
from .broker import MQBroker as MQBrokerUsecase
from .broker import MQPublisher, MQRoute, MQRouter
from .message import MQMessage as MQMessageClass
from .response import MQPublishCommand, MQPublishMessage, MQResponse
from .schemas import MQQueue
from .testing import TestMQBroker

MQBroker = MQBrokerUsecase
MQMessage = MQMessageClass

__all__ = (
    "MQBroker",
    "MQMessage",
    "MQPublishCommand",
    "MQPublishMessage",
    "MQPublisher",
    "MQProducer",
    "MQQueue",
    "MQResponse",
    "MQRoute",
    "MQRouter",
    "TestApp",
    "TestMQBroker",
)
