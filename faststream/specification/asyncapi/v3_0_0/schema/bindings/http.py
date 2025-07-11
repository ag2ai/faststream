from faststream.specification.asyncapi.v2_6_0.schema.bindings.http import (
    OperationBinding as OldOperationBinding,
)


class OperationBinding(OldOperationBinding):
    bindingVersion: str = "0.3.0"
