from dataclasses import dataclass


@dataclass
class Settings:
    url: str = "redis://localhost:6379"
    host: str = "localhost"
    port: int = 6379
