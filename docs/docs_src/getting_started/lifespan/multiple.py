from unittest.mock import AsyncMock, MagicMock

from faststream import Context, ContextRepo, FastStream

app = FastStream()

# override original app for testing purposes
# cut this block out of documentation
mock_broker = MagicMock()
mock_broker._update_fd_config = MagicMock(return_value=None)
mock_broker.start = AsyncMock()
mock_broker.stop = AsyncMock()
mock_broker.config = MagicMock()
mock_broker.config.fd_config = MagicMock()
app = FastStream(mock_broker)


@app.on_startup
async def setup(context: ContextRepo):
    context.set_global("field", 1)


@app.on_startup
async def setup_later(field: int = Context()):
    assert field == 1
