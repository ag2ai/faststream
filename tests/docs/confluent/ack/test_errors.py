from unittest.mock import MagicMock, patch

import pytest

from faststream.confluent import TestApp, TestKafkaBroker
from faststream.confluent.message import KafkaMessage


@pytest.mark.asyncio()
async def test_ack_exc(mock: MagicMock) -> None:
    from docs.docs_src.confluent.ack.errors import app, broker

    with patch.object(KafkaMessage, "ack", mock):
        async with TestKafkaBroker(broker), TestApp(app):
            mock.assert_called_once()
