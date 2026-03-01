from typing import Any


def to_camelcase(*names: str) -> str:
    return " ".join(names).replace("_", " ").title().replace(" ", "")


def resolve_payloads(
    payloads: list[tuple["dict[str, Any]", str]],
    extra: str = "",
    served_words: int = 1,
) -> "dict[str, Any]":
    ln = len(payloads)
    payload: dict[str, Any]
    if ln > 1:
        one_of_payloads = {}

        for body, handler_name in payloads:
            title = body["title"]
            words = title.split(":")

            if len(words) > 1:  # not pydantic model case
                body["title"] = title = ":".join(
                    filter(
                        bool,
                        (
                            handler_name,
                            extra if extra not in words else "",
                            *words[served_words:],
                        ),
                    ),
                )

            one_of_payloads[title] = body

        payload = {"oneOf": one_of_payloads}

    elif ln == 1:
        payload = payloads[0][0]

    else:
        payload = {}

    return payload


def clear_key(key: str) -> str:
    return key.replace("/", ".")


def move_pydantic_refs(
    original: Any,
    key: str,
) -> Any:
    """Remove pydantic references and replacem them by real schemas."""
    if not isinstance(original, dict):
        return original

    data = original.copy()

    for k in data:
        item = data[k]

        if isinstance(item, str):
            if key in item:
                data[k] = data[k].replace(key, "components/schemas")

        elif isinstance(item, dict):
            data[k] = move_pydantic_refs(data[k], key)

        elif isinstance(item, list):
            for i in range(len(data[k])):
                data[k][i] = move_pydantic_refs(item[i], key)

    if (
        isinstance(desciminator := data.get("discriminator"), dict)
        and "propertyName" in desciminator
    ):
        data["discriminator"] = desciminator["propertyName"]

    return data
