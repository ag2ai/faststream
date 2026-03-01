=== "python 3.9+"
    ```python hl_lines="4 9"
    from types import Annotated
    from faststream import Context

    CorrelationId = Annotated[str, Context("message.correlation_id")]

    @broker.subscriber("test")
    async def base_handler(
        body: str,
        cor_id: CorrelationId,
    ):
        print(cor_id)
    ```

=== "python 3.6+"
    ```python hl_lines="4 9"
    from typing_extensions import Annotated
    from faststream import Context

    CorrelationId = Annotated[str, Context("message.correlation_id")]

    @broker.subscriber("test")
    async def base_handler(
        body: str,
        cor_id: CorrelationId,
    ):
        print(cor_id)
    ```
