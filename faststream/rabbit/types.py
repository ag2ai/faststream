from typing import TypeAlias

import aio_pika

from faststream.types import SendableMessage

AioPikaSendableMessage: TypeAlias = aio_pika.Message | SendableMessage
