from typing import Literal

from pydantic import BaseModel, Field

from faststream._internal._compat import dump_json


class RedrivePolicy(BaseModel):
    """SQS redrive policy — routes failed messages to a dead-letter queue."""

    dead_letter_target_arn: str = Field(alias="deadLetterTargetArn")
    max_receive_count: int = Field(default=10, alias="maxReceiveCount", gt=0)

    model_config = {"populate_by_name": True}


class RedriveAllowPolicy(BaseModel):
    """SQS redrive-allow policy — which source queues may use this DLQ."""

    redrive_permission: Literal["allowAll", "denyAll", "byQueue"] = Field(
        default="allowAll",
        alias="redrivePermission",
    )
    source_queue_arns: list[str] | None = Field(
        default=None,
        alias="sourceQueueArns",
        max_length=10,
    )

    model_config = {"populate_by_name": True}


class SQSQueue(BaseModel):
    """A standard SQS queue declaration.

    Can be passed to ``@broker.subscriber`` / ``broker.publish`` in place of a
    plain queue name. ``to_attributes()`` renders the AWS ``Attributes`` dict
    used by ``create_queue``.
    """

    name: str
    # whether this is a FIFO queue; excluded from the attributes dump
    fifo: bool = Field(default=False, exclude=True)

    delay_seconds: int | None = Field(default=None, alias="DelaySeconds", ge=0, le=900)
    maximum_message_size: int | None = Field(
        default=None, alias="MaximumMessageSize", ge=1024, le=262144
    )
    message_retention_period: int | None = Field(
        default=None, alias="MessageRetentionPeriod", ge=60, le=1209600
    )
    receive_message_wait_time_seconds: int | None = Field(
        default=None, alias="ReceiveMessageWaitTimeSeconds", ge=0, le=20
    )
    visibility_timeout: int | None = Field(
        default=None, alias="VisibilityTimeout", ge=0, le=43200
    )
    redrive_policy: RedrivePolicy | None = Field(default=None, alias="RedrivePolicy")
    redrive_allow_policy: RedriveAllowPolicy | None = Field(
        default=None, alias="RedriveAllowPolicy"
    )
    kms_master_key_id: str | None = Field(default=None, alias="KmsMasterKeyId")
    sqs_managed_sse_enabled: bool | None = Field(
        default=None, alias="SqsManagedSseEnabled"
    )

    model_config = {"populate_by_name": True}

    @property
    def queue_name(self) -> str:
        """The full queue name AWS expects (FIFO queues require a .fifo suffix)."""
        if self.fifo and not self.name.endswith(".fifo"):
            return f"{self.name}.fifo"
        return self.name

    def to_attributes(self) -> dict[str, str]:
        """Render the AWS ``Attributes`` dict for ``create_queue``."""
        raw = self.model_dump(by_alias=True, exclude_none=True, exclude={"name"})
        attributes: dict[str, str] = {}
        for key, value in raw.items():
            if isinstance(value, bool):
                attributes[key] = str(value).lower()
            elif isinstance(value, (dict, list)):
                attributes[key] = dump_json(value).decode()
            else:
                attributes[key] = str(value)
        if self.fifo:
            attributes["FifoQueue"] = "true"
        return attributes


class FifoQueue(SQSQueue):
    """A FIFO SQS queue declaration (``.fifo`` suffix is added automatically)."""

    fifo: bool = Field(default=True, exclude=True)

    content_based_deduplication: bool | None = Field(
        default=None, alias="ContentBasedDeduplication"
    )
    deduplication_scope: Literal["messageGroup", "queue"] | None = Field(
        default=None, alias="DeduplicationScope"
    )
    fifo_throughput_limit: Literal["perQueue", "perMessageGroupId"] | None = Field(
        default=None, alias="FifoThroughputLimit"
    )
