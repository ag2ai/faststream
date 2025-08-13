"""GCP Pub/Sub retry configuration."""

from dataclasses import dataclass


@dataclass
class RetryConfig:
    """Configuration for GCP Pub/Sub retry settings."""

    max_attempts: int = 5
    max_delay: float = 60.0
    multiplier: float = 2.0
    min_delay: float = 1.0
