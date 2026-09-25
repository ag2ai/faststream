from dataclasses import dataclass, field


@dataclass
class SettingsCluster:
    url: str = "redis://127.0.0.1:7001"
    host: str = "127.0.0.1"
    port: int = 7001
    startup_nodes: list[tuple[str, int]] = field(
        default_factory=lambda: [("127.0.0.1", 7002), ("127.0.0.1", 7003)],
    )
