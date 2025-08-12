"""GCP Pub/Sub integration for FastStream."""

from faststream._internal.testing.app import TestApp

try:
    from gcloud.aio.pubsub import PubsubMessage

    from faststream.gcppubsub.annotations import (
        NativeMessage,
        Publisher,
        StreamMessage,
        Subscriber,
        Subscription,
        Topic,
    )
    from faststream.gcppubsub.broker import GCPPubSubBroker, GCPPubSubRouter
    from faststream.gcppubsub.message import GCPPubSubMessage
    from faststream.gcppubsub.security import GCPPubSubSecurity
    from faststream.gcppubsub.testing import TestGCPPubSubBroker

except ImportError as e:
    if "'gcloud'" not in str(e):
        raise
    
    from faststream.exceptions import INSTALL_FASTSTREAM
    
    raise ImportError(INSTALL_FASTSTREAM + "[gcppubsub]") from e

__all__ = (
    # Main classes
    "GCPPubSubBroker",
    "GCPPubSubMessage",
    "GCPPubSubRouter", 
    "GCPPubSubSecurity",
    # Testing
    "TestApp",
    "TestGCPPubSubBroker",
    # External types
    "PubsubMessage",
    # Type annotations
    "Topic",
    "Subscription",
    "NativeMessage",
    "StreamMessage",
    "Publisher",
    "Subscriber",
)