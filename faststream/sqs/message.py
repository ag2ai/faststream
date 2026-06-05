from typing import TYPE_CHECKING

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

    @property
    def receipt_handle(self) -> str:
        return self.raw_message.get("ReceiptHandle", "")

    async def ack(self) -> None:
        if self.committed is None and self.sqs_client is not None and self.receipt_handle:
            await self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
            )
        await super().ack()

    async def nack(self) -> None:
        if self.committed is None and self.sqs_client is not None and self.receipt_handle:
            await self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
                VisibilityTimeout=0,
            )
        await super().nack()

    async def reject(self) -> None:
        if self.committed is None and self.sqs_client is not None and self.receipt_handle:
            await self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
            )
        await super().reject()
