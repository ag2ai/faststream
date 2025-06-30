from typing import TypeAlias

import aio_pika

from faststream._internal.basic_types import SendableMessage

AioPikaSendableMessage: TypeAlias = aio_pika.Message | SendableMessage
