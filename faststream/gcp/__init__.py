"""GCP Pub/Sub integration for FastStream."""

from faststream._internal.testing.app import TestApp

try:
    from gcloud.aio.pubsub import PubsubMessage

    from faststream.gcp.annotations import (
        GCPMessage,
        NativeMessage,
        Publisher,
        StreamMessage,
        Subscriber,
        Subscription,
        Topic,
    )
    from faststream.gcp.broker import GCPBroker, GCPRouter
    from faststream.gcp.configs import (
        PublisherConfig,
        RetryConfig,
        SubscriberConfig,
    )
    from faststream.gcp.security import GCPSecurity
    from faststream.gcp.testing import TestGCPBroker

except ImportError as e:
    if "'gcloud'" not in str(e):
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_GCPPUBSUB

    raise ImportError(INSTALL_FASTSTREAM_GCPPUBSUB + "[gcp]") from e

__all__ = (
    "INSTALL_FASTSTREAM_GCPPUBSUB",
    "GCPBroker",
    "GCPMessage",
    "GCPRouter",
    "GCPSecurity",
    "NativeMessage",
    "Publisher",
    "PublisherConfig",
    "PubsubMessage",
    "RetryConfig",
    "StreamMessage",
    "Subscriber",
    "SubscriberConfig",
    "Subscription",
    "TestApp",
    "TestGCPBroker",
    "Topic",
)
