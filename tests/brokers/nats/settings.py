from dataclasses import dataclass


@dataclass
class Settings:
    url: str = "nats://localhost:4222"
