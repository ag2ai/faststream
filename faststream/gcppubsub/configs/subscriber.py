"""GCP Pub/Sub subscriber configuration."""

from dataclasses import dataclass


@dataclass
class SubscriberConfig:
    """Configuration for GCP Pub/Sub subscriber settings."""

    max_messages: int = 1000
    ack_deadline: int = 600  # 10 minutes
    max_extension: int = 600  # 10 minutes
