from opentelemetry.trace import Span
from typing_extensions import Annotated

from faststream import Context
from faststream.opentelemetry.baggage import Baggage

CurrentSpan = Annotated[Span, Context("span")]
CurrentBaggage = Annotated[Baggage, Context("baggage")]
