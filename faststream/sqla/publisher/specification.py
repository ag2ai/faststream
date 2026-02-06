from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.specification.schema import Message, Operation, PublisherSpec
from faststream.sqla.configs.broker import SqlaBrokerConfig

from .config import SqlaPublisherSpecificationConfig


class SqlaPublisherSpecification(
    PublisherSpecification[SqlaBrokerConfig, SqlaPublisherSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.config.queue}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        self.get_payloads()

        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=None,
                    ),
                    bindings=None,
                ),
                bindings=None,
            ),
        }
