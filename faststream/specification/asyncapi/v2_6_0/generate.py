import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal._compat import DEF_KEY
from faststream._internal.constants import ContentTypes
from faststream.specification.asyncapi.utils import clear_key, move_pydantic_refs
from faststream.specification.asyncapi.v2_6_0.schema import (
    ApplicationInfo,
    ApplicationSchema,
    Channel,
    Components,
    Contact,
    ExternalDocs,
    License,
    Message,
    Operation,
    Reference,
    Server,
    Tag,
)
from faststream.specification.asyncapi.v2_6_0.schema.bindings import (
    OperationBinding,
    http as http_bindings,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyHttpUrl
    from faststream._internal.broker import BrokerUsecase
    from faststream._internal.types import ConnectionType, MsgType
    from faststream.asgi.handlers import HttpHandler
    from faststream.specification.schema.extra import (
        Contact as SpecContact,
        ContactDict,
        ExternalDocs as SpecDocs,
        ExternalDocsDict,
        License as SpecLicense,
        LicenseDict,
        Tag as SpecTag,
        TagDict,
    )


def get_app_schema(
    broker: "BrokerUsecase[Any, Any]",
    /,
    title: str,
    app_version: str,
    schema_version: str,
    description: str | None,
    terms_of_service: Optional["AnyHttpUrl"],
    contact: Union["SpecContact", "ContactDict", dict[str, Any]] | None,
    license: Union["SpecLicense", "LicenseDict", dict[str, Any]] | None,
    identifier: str | None,
    tags: Sequence[Union["SpecTag", "TagDict", dict[str, Any]]],
    external_docs: Union["SpecDocs", "ExternalDocsDict", dict[str, Any]] | None,
    http_handlers: list[tuple[str, "HttpHandler"]],
) -> ApplicationSchema:
    """Get the application schema."""
    servers = get_broker_server(broker)
    channels = get_broker_channels(broker)

    messages: dict[str, Message] = {}
    payloads: dict[str, dict[str, Any]] = {}

    for channel in channels.values():
        channel.servers = list(servers.keys())
    channels.update(get_asgi_routes(http_handlers))

    for channel_name, ch in channels.items():
        resolve_channel_messages(ch, channel_name, payloads, messages)

    return ApplicationSchema(
        info=ApplicationInfo(
            title=title,
            version=app_version,
            description=description,
            termsOfService=terms_of_service,
            contact=Contact.from_spec(contact),
            license=License.from_spec(license),
        ),
        tags=[Tag.from_spec(tag) for tag in tags] or None,
        externalDocs=ExternalDocs.from_spec(external_docs),
        asyncapi=schema_version,
        defaultContentType=ContentTypes.JSON.value,
        id=identifier,
        servers=servers,
        channels=channels,
        components=Components(
            messages=messages,
            schemas=payloads,
            securitySchemes=None
            if broker.specification.security is None
            else broker.specification.security.get_schema(),
        ),
    )


def resolve_channel_messages(
    channel: Channel,
    channel_name: str,
    payloads: dict[str, dict[str, Any]],
    messages: dict[str, Message],
) -> None:
    if channel.subscribe is not None and channel.subscribe.message:
        assert isinstance(channel.subscribe.message, Message)

        channel.subscribe.message = _resolve_msg_payloads(
            channel.subscribe.message,
            channel_name,
            payloads,
            messages,
        )

    if channel.publish is not None:
        assert isinstance(channel.publish.message, Message)

        channel.publish.message = _resolve_msg_payloads(
            channel.publish.message,
            channel_name,
            payloads,
            messages,
        )


def get_broker_server(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
) -> dict[str, Server]:
    """Get the broker server for an application."""
    specification = broker.specification

    servers = {}

    broker_meta: dict[str, Any] = {
        "protocol": specification.protocol,
        "protocolVersion": specification.protocol_version,
        "description": specification.description,
        "tags": [Tag.from_spec(tag) for tag in specification.tags] or None,
        "security": specification.security.get_requirement()
        if specification.security
        else None,
        # TODO
        # "variables": "",
        # "bindings": "",
    }

    single_server = len(specification.url) == 1
    for i, url in enumerate(specification.url, 1):
        server_name = "development" if single_server else f"Server{i}"
        servers[server_name] = Server(url=url, **broker_meta)

    return servers


def get_broker_channels(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
) -> dict[str, Channel]:
    """Get the broker channels for an application."""
    channels = {}

    for s in filter(lambda s: s.specification.include_in_schema, broker.subscribers):
        for key, sub in s.schema().items():
            if key in channels:
                warnings.warn(
                    f"Overwrite channel handler, channels have the same names: `{key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )

            channels[key] = Channel.from_sub(sub)

    for p in filter(lambda p: p.specification.include_in_schema, broker.publishers):
        for key, pub in p.schema().items():
            if key in channels:
                warnings.warn(
                    f"Overwrite channel handler, channels have the same names: `{key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )

            channels[key] = Channel.from_pub(pub)

    return channels


def get_asgi_routes(
    http_handlers: list[tuple[str, "HttpHandler"]],
) -> dict[str, Channel]:
    """Get the ASGI routes for an application."""
    channels: dict[str, Channel] = {}
    for path, asgi_app in http_handlers:
        if asgi_app.include_in_schema:
            channel = Channel(
                description=asgi_app.description,
                subscribe=Operation(
                    tags=asgi_app.tags,
                    operationId=asgi_app.unique_id,
                    bindings=OperationBinding(
                        http=http_bindings.OperationBinding(
                            method=", ".join(asgi_app.methods),
                        ),
                    ),
                    message=None,
                ),
            )

            channels[path] = channel

    return channels


def _resolve_msg_payloads(
    m: Message,
    channel_name: str,
    payloads: dict[str, Any],
    messages: dict[str, Any],
) -> Reference:
    """Replace message payload by reference and normalize payloads.

    Payloads and messages are editable dicts to store schemas for reference in AsyncAPI.
    """
    one_of_list: list[Reference] = []
    m.payload = move_pydantic_refs(m.payload, DEF_KEY)

    if DEF_KEY in m.payload:
        payloads.update(m.payload.pop(DEF_KEY))

    one_of = m.payload.get("oneOf")
    if isinstance(one_of, dict):
        for p_title, p in one_of.items():
            formatted_payload_title = clear_key(p_title)
            # Promote nested Pydantic $defs from each payload into components/schemas
            # so that referenced nested models are available globally.
            if isinstance(p, dict) and DEF_KEY in p:
                defs = p.pop(DEF_KEY) or {}
                for def_name, def_schema in defs.items():
                    payloads[clear_key(def_name)] = def_schema
            payloads.update(p.pop(DEF_KEY, {}))
            if formatted_payload_title not in payloads:
                payloads[formatted_payload_title] = p
            one_of_list.append(
                Reference(**{
                    "$ref": f"#/components/schemas/{formatted_payload_title}",
                }),
            )

    elif one_of is not None:
        # Discriminator case
        for p in one_of:
            p_value = next(iter(p.values()))
            p_title = p_value.split("/")[-1]
            p_title = clear_key(p_title)
            if p_title not in payloads:
                payloads[p_title] = p
            one_of_list.append(Reference(**{"$ref": f"#/components/schemas/{p_title}"}))

    if not one_of_list:
        payloads.update(m.payload.pop(DEF_KEY, {}))
        p_title = m.payload.get("title", f"{channel_name}Payload")
        p_title = clear_key(p_title)
        if p_title in payloads and payloads[p_title] != m.payload:
            warnings.warn(
                f"Overwriting the message schema, data types have the same name: `{p_title}`",
                RuntimeWarning,
                stacklevel=1,
            )

        payloads[p_title] = m.payload
        m.payload = {"$ref": f"#/components/schemas/{p_title}"}

    else:
        m.payload["oneOf"] = one_of_list

    assert m.title
    message_title = clear_key(m.title)
    messages[message_title] = m
    return Reference(**{"$ref": f"#/components/messages/{message_title}"})
