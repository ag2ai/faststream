from dataclasses import dataclass


@dataclass
class Settings:
    url: str = "localhost:9092"
