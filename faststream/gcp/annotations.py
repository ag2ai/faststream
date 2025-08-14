"""GCP Pub/Sub type annotations."""

from typing import TYPE_CHECKING, Annotated, TypeAlias

from faststream import Depends
from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.gcp.broker.broker import GCPBroker as GCPBrokerType
from faststream.gcp.message import GCPMessage as GCPMessageType
from faststream.params import NoCast

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from gcloud.aio.pubsub import PublisherClient, PubsubMessage, SubscriberClient


# Topic and Subscription types
Topic: TypeAlias = str
Subscription: TypeAlias = str

# Message types
NativeMessage: TypeAlias = "PubsubMessage"

# Client types
Publisher: TypeAlias = "PublisherClient"
Subscriber: TypeAlias = "SubscriberClient"
Session: TypeAlias = "ClientSession"

# FastStream message type
StreamMessage: TypeAlias = GCPMessageType

# Context annotations for dependency injection
GCPMessage = Annotated[GCPMessageType, Context("message")]
GCPBroker = Annotated[GCPBrokerType, Context("broker")]

# Direct message attribute access
MessageAttributes = Annotated[dict[str, str], Context("message.attributes")]
OrderingKey = Annotated[str | None, Context("message.ordering_key")]
PublishTime = Annotated[str | None, Context("message.publish_time")]
MessageId = Annotated[str | None, Context("message.message_id")]


# Dependency functions for more complex attribute processing
async def get_attributes(message: GCPMessage) -> dict[str, str]:
    """Extract message attributes."""
    return message.attributes or {}


async def get_ordering_key(message: GCPMessage) -> str | None:
    """Extract message ordering key."""
    return message.ordering_key


async def get_publish_time(message: GCPMessage) -> str | None:
    """Extract message publish time."""
    return message.publish_time


async def get_message_id(message: GCPMessage) -> str | None:
    """Extract message ID."""
    return message.message_id


# Alternative dependency-based annotations (for more complex processing)
Attributes = Annotated[dict[str, str], Depends(get_attributes, cast=False)]

# Export additional annotations
__all__ = (
    "Attributes",
    "ContextRepo",
    "GCPBroker",
    "GCPMessage",
    "Logger",
    "MessageAttributes",
    "MessageId",
    "NativeMessage",
    "NoCast",
    "OrderingKey",
    "PublishTime",
    "Publisher",
    "Session",
    "StreamMessage",
    "Subscriber",
    "Subscription",
    "Topic",
)
