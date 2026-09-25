```python hl_lines="4 9"
from typing import Annotated
from faststream import Context

CorrelationId = Annotated[str, Context("message.correlation_id")]

@broker.subscriber("test")
async def base_handler(
    body: str,
    cor_id: CorrelationId,
):
    print(cor_id)
```
