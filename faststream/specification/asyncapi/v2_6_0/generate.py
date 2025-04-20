import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal._compat import DEF_KEY
from faststream._internal.basic_types import AnyDict, AnyHttpUrl
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
    tags: Sequence[Union["SpecTag", "TagDict", "AnyDict"]],
    external_docs: Optional[Union["SpecDocs", "ExternalDocsDict", "AnyDict"]],
) -> ApplicationSchema:
    """Get the application schema."""
    broker._setup()

    servers = get_broker_server(broker)
    channels = get_broker_channels(broker)

    messages: dict[str, Message] = {}
    payloads: dict[str, AnyDict] = {}

    for channel in channels.values():
        channel.servers = list(servers.keys())

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
            if broker.security is None
            else broker.security.get_schema(),
        ),
    )


def resolve_channel_messages(
    channel: Channel,
    channel_name: str,
    payloads: dict[str, AnyDict],
    messages: dict[str, Message],
) -> None:
    if channel.subscribe is not None:
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
    servers = {}

    broker_meta: AnyDict = {
        "protocol": broker.protocol,
        "protocolVersion": broker.protocol_version,
        "description": broker.description,
        "tags": [Tag.from_spec(tag) for tag in broker.tags] or None,
        "security": broker.security.get_requirement() if broker.security else None,
        # TODO
        # "variables": "",
        # "bindings": "",
    }

    urls = broker.url if isinstance(broker.url, list) else [broker.url]

    for i, url in enumerate(urls, 1):
        server_name = "development" if len(urls) == 1 else f"Server{i}"
        servers[server_name] = Server(url=url, **broker_meta)

    return servers


def get_broker_channels(
    broker: "BrokerUsecase[MsgType, ConnectionType]",
) -> dict[str, Channel]:
    """Get the broker channels for an application."""
    channels = {}

    for h in broker._subscribers:
        for key, sub in h.schema().items():
            if key in channels:
                warnings.warn(
                    f"Overwrite channel handler, channels have the same names: `{key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )

            channels[key] = Channel.from_sub(sub)

    for p in broker._publishers:
        for key, pub in p.schema().items():
            if key in channels:
                warnings.warn(
                    f"Overwrite channel handler, channels have the same names: `{key}`",
                    RuntimeWarning,
                    stacklevel=1,
                )

            channels[key] = Channel.from_pub(pub)

    return channels


def _resolve_msg_payloads(
    m: Message,
    channel_name: str,
    payloads: AnyDict,
    messages: AnyDict,
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
            payloads.update(p.pop(DEF_KEY, {}))
            if formatted_payload_title not in payloads:
                payloads[formatted_payload_title] = p
            one_of_list.append(
                Reference(**{"$ref": f"#/components/schemas/{formatted_payload_title}"})
            )

    elif one_of is not None:
        # Descriminator case
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

    assert m.title  # nosec B101
    message_title = clear_key(m.title)
    messages[message_title] = m
    return Reference(**{"$ref": f"#/components/messages/{message_title}"})
