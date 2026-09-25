from dataclasses import dataclass


@dataclass
class Settings:
    host: str = "localhost"
    port: int = 1883
