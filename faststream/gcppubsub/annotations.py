"""GCP Pub/Sub type annotations."""

from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from gcloud.aio.pubsub import PublisherClient, PubsubMessage, SubscriberClient

    from faststream.gcppubsub.message import GCPPubSubMessage

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
StreamMessage: TypeAlias = "GCPPubSubMessage"
