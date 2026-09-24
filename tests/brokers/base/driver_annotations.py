from typing import TYPE_CHECKING, Any

import pytest

from faststream import Context, Depends
from faststream.exceptions import SetupError

from .basic import BaseTestcaseConfig

if TYPE_CHECKING:
    from logging import Logger


class DriverAnnotationTestcase(BaseTestcaseConfig[Any]):
    driver_class: Any
    driver_path: str
    context_annotation: Any
    annotation_import: str

    def _expected_error(self, argument: str) -> str:
        return (
            f"{argument} is annotated with `{self.driver_path}`,"
            " which FastStream cannot inject.\n"
            "Use the context annotation instead:\n"
            f"\n    {self.annotation_import}\n"
        )

    @pytest.mark.asyncio()
    async def test_driver_class_names_the_import_to_use(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)
        driver_class = self.driver_class

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(driver: driver_class) -> None: ...  # type: ignore[valid-type]

        with pytest.raises(SetupError) as excinfo:
            async with self.patch_broker(broker):
                pass

        assert str(excinfo.value) == self._expected_error("`driver`")

    @pytest.mark.asyncio()
    async def test_driver_class_behind_depends_names_the_dependency(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        driver_class = self.driver_class

        def dependency(driver: driver_class) -> None: ...  # type: ignore[valid-type]

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(value: None = Depends(dependency)) -> None: ...

        with pytest.raises(SetupError) as excinfo:
            async with self.patch_broker(broker):
                pass

        assert str(excinfo.value) == self._expected_error(
            "`driver` of dependency `dependency`"
        )

    @pytest.mark.asyncio()
    async def test_driver_class_on_an_included_router_names_the_import_to_use(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        router = self.get_router()
        driver_class = self.driver_class

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def handler(driver: driver_class) -> None: ...  # type: ignore[valid-type]

        broker.include_router(router)

        with pytest.raises(SetupError) as excinfo:
            async with self.patch_broker(broker):
                pass

        assert str(excinfo.value) == self._expected_error("`driver`")

    @pytest.mark.asyncio()
    async def test_context_annotation_is_accepted(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)
        context_annotation = self.context_annotation

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(driver: context_annotation) -> None: ...  # type: ignore[valid-type]

        async with self.patch_broker(broker):
            pass

    @pytest.mark.asyncio()
    async def test_driver_class_is_not_checked_without_fastdepends(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=False)
        driver_class = self.driver_class

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(driver: driver_class) -> None: ...  # type: ignore[valid-type]

        async with self.patch_broker(broker):
            pass

    @pytest.mark.asyncio()
    async def test_hint_imported_for_type_checking_only_is_accepted(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(logger: "Logger" = Context()) -> None: ...

        async with self.patch_broker(broker):
            pass
