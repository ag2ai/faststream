from typing import TypeAlias

from faststream._internal.basic_types import SendableMessage
from faststream.kafka.response import KafkaPublishMessage

KafkaSendableMessage: TypeAlias = KafkaPublishMessage | SendableMessage
