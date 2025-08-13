"""GCP Pub/Sub type annotations."""

from typing import TYPE_CHECKING, Annotated, TypeAlias

from faststream._internal.context import Context
from faststream.gcppubsub.message import GCPPubSubMessage as Gm

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
StreamMessage: TypeAlias = Gm

# Context annotations for dependency injection
GCPPubSubMessage = Annotated[Gm, Context("message")]
