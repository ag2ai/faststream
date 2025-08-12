"""GCP Pub/Sub broker router."""

from typing import Any

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.broker.router import BrokerRouter
from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


class GCPPubSubRouter(GCPPubSubRegistrator, BrokerRouter[PubsubMessage]):
    """GCP Pub/Sub message router."""

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        """Initialize GCP Pub/Sub router.

        Args:
            **kwargs: Additional router options
        """
        super().__init__(**kwargs)
