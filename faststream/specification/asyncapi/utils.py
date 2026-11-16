from copy import deepcopy
from typing import Any
from urllib.parse import quote, unquote


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


def ref(*parts: str) -> dict[str, str]:
    return {"$ref": "#/" + "/".join(_encode_json_pointer_part(p) for p in parts)}


def _encode_json_pointer_part(part: str) -> str:
    pointer_part = part.replace("~", "~0").replace("/", "~1")
    return quote(pointer_part, safe="~:._-")


_SCHEMA_MAPS = {
    "properties",
    "patternProperties",
    "dependentSchemas",
    "dependencies",
    "$defs",
    "definitions",
}
_SCHEMA_LISTS = {"allOf", "anyOf", "oneOf", "prefixItems"}
_SCHEMA_VALUES = {
    "additionalProperties",
    "additionalItems",
    "unevaluatedProperties",
    "unevaluatedItems",
    "propertyNames",
    "items",
    "contains",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
}


def move_schema_refs(original: Any, *, root_name: str) -> Any:
    """Move local definitions to AsyncAPI components without changing data literals."""
    data = deepcopy(original)

    def rewrite(value: str, root_ref: str, names: dict[str, str]) -> str:
        for key in ("$defs", "definitions"):
            prefix = f"#/{key}/"
            if value.startswith(prefix):
                name, *tail = unquote(value[len(prefix) :]).split("/")
                name = name.replace("~1", "/").replace("~0", "~")
                target = ref("components", "schemas", clear_key(names.get(name, name)))[
                    "$ref"
                ]
                return target + (
                    "/" + quote("/".join(tail), safe="/~:._-") if tail else ""
                )
        if value == "#" or value.startswith("#/"):
            return root_ref + value[1:]
        return value

    def visit(
        schema: Any, root_ref: str, names: dict[str, str], namespace: str = ""
    ) -> Any:
        if not isinstance(schema, dict) or "$id" in schema:
            return schema
        schema = schema.copy()
        # Retained input definitions belong to one payload, unlike shared models.
        if namespace and "$ref" in schema:
            local_names = {
                name: f"{namespace}:{name}"
                for key in ("$defs", "definitions")
                for name in schema.get(key, {})
            }
            names = {**names, **local_names}
            for key in ("$defs", "definitions"):
                if key in schema:
                    schema[key] = {
                        local_names[name]: child for name, child in schema[key].items()
                    }
        for key in ("$ref", "$dynamicRef", "$recursiveRef"):
            if isinstance(value := schema.get(key), str):
                schema[key] = rewrite(value, root_ref, names)
        discriminator = schema.get("discriminator")
        if isinstance(discriminator, dict):
            discriminator = discriminator.copy()
            schema["discriminator"] = discriminator
            if isinstance(mapping := discriminator.get("mapping"), dict):
                discriminator["mapping"] = {
                    k: rewrite(v, root_ref, names) if isinstance(v, str) else v
                    for k, v in mapping.items()
                }
            if "propertyName" in discriminator:
                schema["discriminator"] = discriminator["propertyName"]
        for key, value in schema.items():
            if key in _SCHEMA_MAPS and isinstance(value, dict):
                schema[key] = {
                    name: visit(child, root_ref, names) for name, child in value.items()
                }
            elif key == "oneOf" and isinstance(value, dict):
                # FastStream's overloaded-handler payloads use a named map.
                children = {}
                for name, child in value.items():
                    child_name = (
                        child.get("title", name) if isinstance(child, dict) else name
                    )
                    if not isinstance(child_name, str):
                        child_name = name
                    children[name] = visit(
                        child,
                        ref("components", "schemas", clear_key(child_name))["$ref"],
                        names,
                        f"{namespace or root_name}:{child_name}",
                    )
                schema[key] = children
            elif (key in _SCHEMA_LISTS or key == "items") and isinstance(value, list):
                schema[key] = [visit(child, root_ref, names) for child in value]
            elif key in _SCHEMA_VALUES:
                schema[key] = visit(value, root_ref, names)
        return schema

    title = data.get("title", root_name) if isinstance(data, dict) else root_name
    return visit(
        data, ref("components", "schemas", clear_key(title))["$ref"], {}, root_name
    )


def pop_schema_definitions(payload: dict[str, Any]) -> dict[str, Any]:
    if "$id" in payload:
        return {}
    definitions: dict[str, Any] = {}
    for key in ("$defs", "definitions"):
        for name, schema in (payload.pop(key, None) or {}).items():
            normalized_name = clear_key(name)
            if normalized_name in definitions and definitions[normalized_name] != schema:
                msg = f"Conflicting schema definitions for {normalized_name!r}."
                raise ValueError(msg)
            definitions[normalized_name] = schema
    return definitions
