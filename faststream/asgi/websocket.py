from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .types import Receive, Scope, Send


class WebSocketClose:
    def __init__(
        self,
        code: int,
        reason: Optional[str],
    ) -> None:
        self.code = code
        self.reason = reason or ""

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        await send(
            {"type": "websocket.close", "code": self.code, "reason": self.reason}
        )
