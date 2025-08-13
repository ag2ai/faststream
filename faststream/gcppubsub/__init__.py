"""GCP Pub/Sub integration for FastStream."""

from faststream._internal.testing.app import TestApp

try:
    from gcloud.aio.pubsub import PubsubMessage

    from faststream.gcppubsub.annotations import (
        GCPPubSubMessage,
        NativeMessage,
        Publisher,
        StreamMessage,
        Subscriber,
        Subscription,
        Topic,
    )
    from faststream.gcppubsub.broker import GCPPubSubBroker, GCPPubSubRouter
    from faststream.gcppubsub.security import GCPPubSubSecurity
    from faststream.gcppubsub.testing import TestGCPPubSubBroker

except ImportError as e:
    if "'gcloud'" not in str(e):
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_GCPPUBSUB

    raise ImportError(INSTALL_FASTSTREAM_GCPPUBSUB + "[gcppubsub]") from e

__all__ = (
    "INSTALL_FASTSTREAM_GCPPUBSUB",
    "GCPPubSubBroker",
    "GCPPubSubMessage",
    "GCPPubSubRouter",
    "GCPPubSubSecurity",
    "NativeMessage",
    "Publisher",
    "PubsubMessage",
    "StreamMessage",
    "Subscriber",
    "Subscription",
    "TestApp",
    "TestGCPPubSubBroker",
    "Topic",
)
