from typing import Annotated

from opentelemetry.trace import Span

from faststream import Context
from faststream.opentelemetry.baggage import Baggage

CurrentSpan = Annotated[Span, Context("span")]
CurrentBaggage = Annotated[Baggage, Context("baggage")]
