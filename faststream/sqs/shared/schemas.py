from typing import Any, Dict, Optional, Sequence

from pydantic import BaseModel, Field, PositiveInt
from typing_extensions import Literal

from faststream._compat import PYDANTIC_V2
from faststream.broker.schemas import NameRequired


class RedrivePolicy(BaseModel):
    """SQS Queue RedrivePolicy attribute details"""

    dead_letter_target: str = Field(
        default="",
        alias="deadLetterTargetArn",
    )
    max_receive_count: PositiveInt = Field(
        default=10,
        alias="deadLetterTargetArn",
    )


class RedriveAllowPolicy(BaseModel):
    """SQS Queue RedriveAllowPolicy attribute details"""

    redrive_permission: Literal["allowAll", "denyAll", "byQueue"] = Field(
        default="allowAll",
        alias="redrivePermission",
    )
    source_queue_arns: Sequence[str] = Field(
        default_factory=tuple,
        alias="sourceQueueArns",
        max_length=10,
    )


class SQSQueue(NameRequired):
    """SQS Basic Queue initialization attributes"""

    fifo: bool = Field(
        default=False,
        alias="FifoQueue",
    )

    delay_seconds: int = Field(
        default=0,
        # alias="DelaySeconds",
        ge=0,
        le=900,
    )
    max_message_size: int = Field(
        default=262_144,
        alias="MaximumMessageSize",
        ge=1024,
        le=262_144,
    )
    retention_period_sec: int = Field(
        345_600,
        alias="MessageRetentionPeriod",
        ge=60,
        le=1_209_600,
    )
    receive_wait_time_sec: int = Field(
        default=0,
        alias="ReceiveMessageWaitTimeSeconds",
        ge=0,
        le=20,
    )
    visibility_timeout_sec: int = Field(
        default=30,
        alias="VisibilityTimeout",
        ge=0,
        le=43_200,
    )
    redrive_policy: RedrivePolicy = Field(
        default_factory=RedrivePolicy,
        alias="RedrivePolicy",
    )
    redrive_allow_policy: RedriveAllowPolicy = Field(
        default_factory=RedrivePolicy,
        alias="RedriveAllowPolicy",
    )

    kms_master_key_id: str = Field(default="", alias="KmsMasterKeyId")
    kms_data_key_reuse_period_sec: int = Field(
        default=300,
        alias="KmsDataKeyReusePeriodSeconds",
        ge=60,
        le=86_400,
    )
    sse_enabled: bool = Field(
        default=False,
        alias="SqsManagedSseEnabled",
    )
    tags: Dict[str, str] = Field(
        default_factory=dict,
    )

    def __init__(
        self,
        name: str,
        fifo: bool = False,
        delay_seconds: int = 0,
        max_message_size: int = 262_144,
        visibility_timeout_sec: int = 0,
        receive_wait_time_sec: int = 0,
        retention_period_sec: int = 345_600,
        redrive_policy: Optional[RedrivePolicy] = None,
        redrive_allow_policy: Optional[RedriveAllowPolicy] = None,
        kms_master_key_id: str = "",
        kms_data_key_reuse_period_sec: int = 300,
        sse_enabled: bool = False,
        tags: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            name=name,
            fifo=fifo,
            visibility_timeout_sec=visibility_timeout_sec,
            receive_wait_time_sec=receive_wait_time_sec,
            retention_period_sec=retention_period_sec,
            max_message_size=max_message_size,
            delay_seconds=delay_seconds,
            redrive_policy=redrive_policy or RedrivePolicy(),
            redrive_allow_policy=redrive_allow_policy or RedriveAllowPolicy(),
            kms_master_key_id=kms_master_key_id,
            kms_data_key_reuse_period_sec=kms_data_key_reuse_period_sec,
            sse_enabled=sse_enabled,
            tags=tags or {},
            **kwargs,
        )

    if PYDANTIC_V2:
        model_config = {"arbitrary_types_allowed": True}
    else:

        class Config:
            arbitrary_types_allowed = True


class FifoQueue(SQSQueue):
    """SQS FIFO Queue initialization attributes"""

    fifo: bool = Field(
        default=True,
        alias="FifoQueue",
    )
    content_based_deduplication: bool = Field(
        default=True,
        alias="ContentBasedDeduplication",
    )
    deduplication_scope: Optional[Literal["messageGroup", "queue"]] = Field(
        default=None,
        alias="DeduplicationScope",
    )

    # TODO: pydantic validation and test
    # allow perMessageGroup only for messageGroup deduplication_scope
    throughput_limit: Optional[Literal["perMessageGroup", "perQueue"]] = Field(
        default=None,
        alias="FifoThroughputLimit",
    )

    def __init__(
        self,
        name: str,
        fifo: bool = True,
        delay_seconds: int = 0,
        max_message_size: int = 262_144,
        visibility_timeout_sec: int = 0,
        receive_wait_time_sec: int = 0,
        retention_period_sec: int = 345_600,
        content_based_deduplication: bool = True,
        deduplication_scope: Optional[Literal["messageGroup", "queue"]] = None,
        throughput_limit: Optional[Literal["perMessageGroup", "perQueue"]] = None,
        redrive_policy: Optional[RedrivePolicy] = None,
        redrive_allow_policy: Optional[RedriveAllowPolicy] = None,
        kms_data_key_reuse_period_sec: int = 300,
        sse_enabled: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ):
        super().__init__(
            name=name,
            fifo=fifo,
            visibility_timeout_sec=visibility_timeout_sec,
            receive_wait_time_sec=receive_wait_time_sec,
            content_based_deduplication=content_based_deduplication,
            retention_period_sec=retention_period_sec,
            max_message_size=max_message_size,
            delay_seconds=delay_seconds,
            redrive_policy=redrive_policy or RedrivePolicy(),
            redrive_allow_policy=redrive_allow_policy or RedriveAllowPolicy(),
            kms_data_key_reuse_period_sec=kms_data_key_reuse_period_sec,
            sse_enabled=sse_enabled,
            deduplication_scope=deduplication_scope,
            throughput_limit=throughput_limit,
            tags=tags or {},
        )
