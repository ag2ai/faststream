from datetime import datetime

import pytest

from faststream._compat import model_to_jsonable
from faststream.kafka import TestKafkaBroker

from .app import ProductReview, broker, on_product_reviews


@broker.subscriber("customer_service")
async def on_customer_service(msg: ProductReview) -> None:
    pass


@pytest.mark.asyncio
async def test_app_where_review_grade_is_grater_then_5():
    async with TestKafkaBroker(broker):
        timestamp = datetime.now()
        await broker.publish(
            ProductReview(
                product_id=1, customer_id=1, review_grade=6, timestamp=timestamp
            ),
            "product_reviews",
        )

        on_product_review_json = model_to_jsonable(
            ProductReview(
                product_id=1, customer_id=1, review_grade=6, timestamp=timestamp
            )
        )
        on_product_reviews.mock.assert_called_with(on_product_review_json)
        on_customer_service.mock.assert_not_called()


@pytest.mark.asyncio
async def test_app_where_review_grade_is_less_then_5():
    async with TestKafkaBroker(broker):
        timestamp = datetime.now()
        await broker.publish(
            ProductReview(
                product_id=1, customer_id=2, review_grade=2, timestamp=timestamp
            ),
            "product_reviews",
        )

        product_review_json = model_to_jsonable(
            ProductReview(
                product_id=1, customer_id=2, review_grade=2, timestamp=timestamp
            )
        )

        on_product_reviews.mock.assert_called_with(product_review_json)
        on_customer_service.mock.assert_called_with(product_review_json)
