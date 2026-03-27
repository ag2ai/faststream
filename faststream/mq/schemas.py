from dataclasses import dataclass


@dataclass(frozen=True)
class MQQueue:
    name: str

    @classmethod
    def validate(cls, value: "MQQueue | str") -> "MQQueue":
        if isinstance(value, cls):
            return value
        return cls(name=value)

    def add_prefix(self, prefix: str) -> "MQQueue":
        return MQQueue(name=f"{prefix}{self.name}")

    def routing(self) -> str:
        return self.name
