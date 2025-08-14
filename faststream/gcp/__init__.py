"""GCP Pub/Sub integration for FastStream."""

from faststream._internal.testing.app import TestApp

try:
    from gcloud.aio.pubsub import PubsubMessage

    from faststream.gcp.annotations import (
        Attributes,
        GCPMessage,
        MessageAttributes,
        MessageId,
        NativeMessage,
        OrderingKey,
        PublishTime,
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
    from faststream.gcp.response import GCPResponse
    from faststream.gcp.response_types import ResponseAttributes, ResponseOrderingKey
    from faststream.gcp.response_utils import ensure_gcp_response
    from faststream.gcp.security import GCPSecurity
    from faststream.gcp.testing import TestGCPBroker

except ImportError as e:
    if "'gcloud'" not in str(e):
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_GCPPUBSUB

    raise ImportError(INSTALL_FASTSTREAM_GCPPUBSUB + "[gcp]") from e

__all__ = (
    "INSTALL_FASTSTREAM_GCPPUBSUB",
    "Attributes",
    "GCPBroker",
    "GCPMessage",
    "GCPResponse",
    "GCPRouter",
    "GCPSecurity",
    "MessageAttributes",
    "MessageId",
    "NativeMessage",
    "OrderingKey",
    "PublishTime",
    "Publisher",
    "PublisherConfig",
    "PubsubMessage",
    "ResponseAttributes",
    "ResponseOrderingKey",
    "RetryConfig",
    "StreamMessage",
    "Subscriber",
    "SubscriberConfig",
    "Subscription",
    "TestApp",
    "TestGCPBroker",
    "Topic",
    "ensure_gcp_response",
)
