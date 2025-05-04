import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urlparse

from faststream._internal._compat import DEF_KEY
from faststream._internal.basic_types import AnyDict, AnyHttpUrl
from faststream._internal.constants import ContentTypes
from faststream.specification.asyncapi.utils import clear_key, move_pydantic_refs
from faststream.specification.asyncapi.v3_0_0.schema import (
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

if TYPE_CHECKING:
    from faststream._internal.broker.broker import BrokerUsecase
    from faststream._internal.types import ConnectionType, MsgType
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
    description: str,
    terms_of_service: Optional["AnyHttpUrl"],
    contact: Optional[Union["SpecContact", "ContactDict", "AnyDict"]],
    license: Optional[Union["SpecLicense", "LicenseDict", "AnyDict"]],
    identifier: Optional[str],
    tags: Optional[Sequence[Union["SpecTag", "TagDict", "AnyDict"]]],
    external_docs: Optional[Union["SpecDocs", "ExternalDocsDict", "AnyDict"]],
) -> ApplicationSchema:
    """Get the application schema."""
    broker._setup()

    servers = get_broker_server(broker)
    channels, operations = get_broker_channels(broker)

    messages: dict[str, Message] = {}
    payloads: dict[str, AnyDict] = {}

    for channel in channels.values():
        channel.servers = [
            {"$ref": f"#/servers/{server_name}"} for server_name in list(servers.keys())
        ]

    for channel_name, channel in channels.items():
        msgs: dict[str, Union[Message, Reference]] = {}
        for message_name, message in channel.messages.items():
            assert isinstance(message, Message)

            msgs[message_name] = _resolve_msg_payloads(
                message_name,
                message,
                channel_name,
                payloads,
                messages,
            )

        channel.messages = msgs

    return ApplicationSchema(
        info=ApplicationInfo(
            title=title,
            version=app_version,
            description=description,
            termsOfService=terms_of_service,
            contact=Contact.from_spec(contact),
            license=License.from_spec(license),
            tags=[Tag.from_spec(tag) for tag in tags] or None if tags else None,
            externalDocs=ExternalDocs.from_spec(external_docs),
        ),
        asyncapi=schema_version,
        defaultContentType=ContentTypes.JSON.value,
        id=identifier,
        servers=servers,
        channels=channels,
        operations=operations,
        components=Components(
            messages=messages,
            schemas=payloads,
            securitySchemes=None
            if broker.security is None
            else broker.security.get_schema(),
        ),
    )


def get_broker_server(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
) -> dict[str, Server]:
    """Get the broker server for an application."""
    servers = {}

    tags: Optional[list[Union[Tag, AnyDict]]] = None
    if broker.tags:
        tags = [Tag.from_spec(tag) for tag in broker.tags]

    broker_meta: AnyDict = {
        "protocol": broker.protocol,
        "protocolVersion": broker.protocol_version,
        "description": broker.description,
        "tags": tags,
        # TODO
        # "variables": "",
        # "bindings": "",
    }

    if broker.security is not None:
        broker_meta["security"] = broker.security.get_requirement()

    urls = broker.url if isinstance(broker.url, list) else [broker.url]

    for i, broker_url in enumerate(urls, 1):
        server_url = broker_url if "://" in broker_url else f"//{broker_url}"

        parsed_url = urlparse(server_url)
        server_name = "development" if len(urls) == 1 else f"Server{i}"
        servers[server_name] = Server(
            host=parsed_url.netloc,
            pathname=parsed_url.path,
            **broker_meta,
        )

    return servers


def get_broker_channels(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
) -> tuple[dict[str, Channel], dict[str, Operation]]:
    """Get the broker channels for an application."""
    channels = {}
    operations = {}

    for sub in broker._subscribers:
        for sub_key, sub_channel in sub.schema().items():
            channel_obj = Channel.from_sub(sub_key, sub_channel)

            channel_key = clear_key(sub_key)
            if channel_key in channels:
                warnings.warn(
                    f"Overwrite channel handler, channels have the same names: `{channel_key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )

            channels[channel_key] = channel_obj

            operations[f"{channel_key}Subscribe"] = Operation.from_sub(
                messages=[
                    Reference(**{
                        "$ref": f"#/channels/{channel_key}/messages/{msg_name}"
                    })
                    for msg_name in channel_obj.messages
                ],
                channel=Reference(**{"$ref": f"#/channels/{channel_key}"}),
                operation=sub_channel.operation,
            )

    for pub in broker._publishers:
        for pub_key, pub_channel in pub.schema().items():
            channel_obj = Channel.from_pub(pub_key, pub_channel)

            channel_key = clear_key(pub_key)
            if channel_key in channels:
                warnings.warn(
                    f"Overwrite channel handler, channels have the same names: `{channel_key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )
            channels[channel_key] = channel_obj

            operations[channel_key] = Operation.from_pub(
                messages=[
                    Reference(**{
                        "$ref": f"#/channels/{channel_key}/messages/{msg_name}"
                    })
                    for msg_name in channel_obj.messages
                ],
                channel=Reference(**{"$ref": f"#/channels/{channel_key}"}),
                operation=pub_channel.operation,
            )

    return channels, operations


def _resolve_msg_payloads(
    message_name: str,
    m: Message,
    channel_name: str,
    payloads: AnyDict,
    messages: AnyDict,
) -> Reference:
    assert isinstance(m.payload, dict)

    m.payload = move_pydantic_refs(m.payload, DEF_KEY)

    message_name = clear_key(message_name)
    channel_name = clear_key(channel_name)

    if DEF_KEY in m.payload:
        payloads.update(m.payload.pop(DEF_KEY))

    one_of = m.payload.get("oneOf", None)
    if isinstance(one_of, dict):
        one_of_list = []
        processed_payloads: dict[str, AnyDict] = {}
        for name, payload in one_of.items():
            processed_payloads[clear_key(name)] = payload
            one_of_list.append(Reference(**{"$ref": f"#/components/schemas/{name}"}))

        payloads.update(processed_payloads)
        m.payload["oneOf"] = one_of_list
        assert m.title
        messages[clear_key(m.title)] = m
        return Reference(
            **{"$ref": f"#/components/messages/{channel_name}:{message_name}"},
        )

    payloads.update(m.payload.pop(DEF_KEY, {}))
    payload_name = m.payload.get("title", f"{channel_name}:{message_name}:Payload")
    payload_name = clear_key(payload_name)

    if payload_name in payloads and payloads[payload_name] != m.payload:
        warnings.warn(
            f"Overwriting the message schema, data types have the same name: `{payload_name}`",
            RuntimeWarning,
            stacklevel=1,
        )

    payloads[payload_name] = m.payload
    m.payload = {"$ref": f"#/components/schemas/{payload_name}"}
    assert m.title
    messages[clear_key(m.title)] = m
    return Reference(
        **{"$ref": f"#/components/messages/{channel_name}:{message_name}"},
    )
