import asyncio

from faststream.redis import RedisClusterBroker

broker = RedisClusterBroker("redis://127.0.0.1:7001")


async def increment_and_publish(
    broker: RedisClusterBroker,
    key: str = "orders",
) -> list[int | bytes]:
    client = await broker.connect()
    async with client.pipeline(transaction=True) as pipe:
        pipe.incr(f"{{{key}}}:count")
        await broker.publish(
            "created",
            stream=f"{{{key}}}:events",
            pipeline=pipe,
        )
        return await pipe.execute()


async def main() -> None:
    async with broker:
        await increment_and_publish(broker)


if __name__ == "__main__":
    asyncio.run(main())
