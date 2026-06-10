from typing import TYPE_CHECKING, Any, cast

from faststream.message import StreamMessage

if TYPE_CHECKING:
    from types_aiobotocore_sqs import SQSClient
    from types_aiobotocore_sqs.type_defs import MessageTypeDef

    SQSRawMessage = MessageTypeDef
else:
    SQSRawMessage = dict


class SQSMessage(StreamMessage["SQSRawMessage"]):
    """A message consumed from an SQS queue.

    Acknowledgement maps onto the SQS API:

    * ``ack``    -> ``DeleteMessage`` (message handled, remove it)
    * ``nack``   -> ``ChangeMessageVisibility(0)`` (return for immediate redelivery)
    * ``reject`` -> ``DeleteMessage`` (give up; DLQ is handled via RedrivePolicy)
    """

    # Set by SQSParser at parse time so ack/nack/reject can reach the queue.
    sqs_client: "SQSClient | None" = None
    queue_url: str = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # SQS system attributes (Attributes), set by SQSParser at parse time.
        self.system_attributes: dict[str, str] = {}

    @property
    def receipt_handle(self) -> str:
        return self.raw_message.get("ReceiptHandle", "")

    @property
    def approximate_receive_count(self) -> int:
        """How many times SQS has delivered this message (1 on first receive).

        Useful for poison-message detection; pair it with a queue ``RedrivePolicy``
        so exhausted messages move to a dead-letter queue.
        """
        return int(self.system_attributes.get("ApproximateReceiveCount", 0))

    @property
    def sent_timestamp(self) -> int | None:
        value = self.system_attributes.get("SentTimestamp")
        return int(value) if value is not None else None

    @property
    def group_id(self) -> str | None:
        """``MessageGroupId`` for FIFO queues."""
        return self.system_attributes.get("MessageGroupId")

    @property
    def sequence_number(self) -> str | None:
        """``SequenceNumber`` assigned by a FIFO queue."""
        return self.system_attributes.get("SequenceNumber")

    async def ack(self) -> None:
        if self.committed is None and self.sqs_client is not None and self.receipt_handle:
            await self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
            )
        await super().ack()

    async def nack(self, visibility_timeout: int | None = None) -> None:
        """Return the message for redelivery via ``ChangeMessageVisibility``.

        ``visibility_timeout=0`` (default) redelivers immediately. Pass a larger
        value for a backoff before the message becomes visible again — the SQS
        analogue of NATS' ``nack(delay=...)``.
        """
        if self.committed is None and self.sqs_client is not None and self.receipt_handle:
            await self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
                VisibilityTimeout=visibility_timeout or 0,
            )
        await super().nack()

    async def reject(self) -> None:
        if self.committed is None and self.sqs_client is not None and self.receipt_handle:
            await self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
            )
        await super().reject()


class SQSBatchMessage(SQSMessage):
    """A batch of SQS messages handed to a ``batch=True`` subscriber.

    ``raw_message`` is the list of raw SQS messages; ack/nack/reject act on all
    of them at once using the SQS batch APIs (chunked to the 10-entry limit).

    ``system_attributes`` (and the scalar properties derived from it) reflect the
    first message of the batch — the same "first message wins" convention used
    for ``content_type``/``correlation_id``; ``batch_system_attributes`` keeps
    the per-message attributes.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Per-message SQS system attributes, set by SQSParser at parse time.
        self.batch_system_attributes: list[dict[str, str]] = []
        # Per-message parses kept by SQSParser so decode_batch needn't re-parse.
        self.parsed_messages: list[SQSMessage] = []

    @property
    def receipt_handles(self) -> list[str]:
        messages = cast("list[Any]", self.raw_message)
        return [m.get("ReceiptHandle", "") for m in messages if m.get("ReceiptHandle")]

    @staticmethod
    def _chunked(handles: list[str], size: int = 10) -> "list[list[str]]":
        return [handles[i : i + size] for i in range(0, len(handles), size)]

    async def _delete_all(self) -> None:
        if self.sqs_client is None:
            return
        for chunk in self._chunked(self.receipt_handles):
            await self.sqs_client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=[{"Id": str(i), "ReceiptHandle": h} for i, h in enumerate(chunk)],
            )

    async def ack(self) -> None:
        if self.committed is None and self.sqs_client is not None:
            await self._delete_all()
        await super(SQSMessage, self).ack()

    async def nack(self, visibility_timeout: int | None = None) -> None:
        if self.committed is None and self.sqs_client is not None:
            for chunk in self._chunked(self.receipt_handles):
                await self.sqs_client.change_message_visibility_batch(
                    QueueUrl=self.queue_url,
                    Entries=[
                        {
                            "Id": str(i),
                            "ReceiptHandle": h,
                            "VisibilityTimeout": visibility_timeout or 0,
                        }
                        for i, h in enumerate(chunk)
                    ],
                )
        await super(SQSMessage, self).nack()

    async def reject(self) -> None:
        if self.committed is None and self.sqs_client is not None:
            await self._delete_all()
        await super(SQSMessage, self).reject()
