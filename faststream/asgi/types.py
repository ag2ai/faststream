from collections.abc import Awaitable, MutableMapping
from typing import Any, Callable

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
UserApp = Callable[[Scope], Awaitable[ASGIApp]]
