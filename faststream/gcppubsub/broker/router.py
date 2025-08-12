"""GCP Pub/Sub broker router."""

from typing import Any

from faststream._internal.broker.router import BrokerRouter
from faststream.gcppubsub.broker.registrator import GCPPubSubRegistrator


class GCPPubSubRouter(GCPPubSubRegistrator, BrokerRouter["PubsubMessage"]):
    """GCP Pub/Sub message router."""

    def __init__(
        self,
        prefix: str = "",
        **kwargs: Any,
    ) -> None:
        """Initialize GCP Pub/Sub router.

        Args:
            prefix: Topic/subscription prefix
            **kwargs: Additional router options
        """
        super().__init__(prefix=prefix, **kwargs)
        self._prefix = prefix
