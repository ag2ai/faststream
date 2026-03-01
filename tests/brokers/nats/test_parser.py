from unittest.mock import MagicMock

import anyio
import pytest

from tests.brokers.base.parser import CustomParserTestcase

from .basic import NatsTestcaseConfig


@pytest.mark.connected()
@pytest.mark.nats()
class TestCustomParser(NatsTestcaseConfig, CustomParserTestcase):
    async def test_request_respect_decoder(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        async def custom_decoder(msg, original):
            mock()
            return await original(msg)

        broker = self.get_broker(decoder=custom_decoder)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            await br.start()

            with anyio.fail_after(self.timeout):
                msg = await br.request("Hi!", queue)

        assert mock.call_count == 1
        await msg.decode()
        assert mock.call_count == 2
