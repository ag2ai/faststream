from typing import Any, TypeVar

TypedDictCls = TypeVar("TypedDictCls")


def filter_by_dict(
    typed_dict: type[TypedDictCls],
    data: dict[str, Any],
) -> tuple[TypedDictCls, dict[str, Any]]:
    annotations = typed_dict.__annotations__

    out_data = {}
    extra_data = {}

    for k, v in data.items():
        if k in annotations:
            out_data[k] = v
        else:
            extra_data[k] = v

    return (
        typed_dict(out_data),  # type: ignore[call-arg]
        extra_data,
    )
