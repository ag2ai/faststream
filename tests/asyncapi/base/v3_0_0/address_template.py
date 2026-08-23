from tests.asyncapi.base.address_template import (
    AddressTemplateTestcase as BaseAddressTemplateTestcase,
)

from .basic import AsyncAPI300Factory


class AddressTemplateTestcase(BaseAddressTemplateTestcase, AsyncAPI300Factory):
    pass
