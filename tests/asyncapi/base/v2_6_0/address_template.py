from tests.asyncapi.base.address_template import (
    AddressTemplateTestcase as BaseAddressTemplateTestcase,
)

from .basic import AsyncAPI260Factory


class AddressTemplateTestcase(BaseAddressTemplateTestcase, AsyncAPI260Factory):
    pass
