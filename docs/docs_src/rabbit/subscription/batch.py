import asyncio
from faststream import FastStream, Logger, AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue, RabbitExchange, Channel


broker = RabbitBroker()
app = FastStream(broker)
example_queue = RabbitQueue(name="example_queue")
example_exchange = RabbitExchange(name="example_exchange")

BATCH_SIZE = 10
FLUSH_INTERVAL = 5.0 #seconds

class BatchCollector:
    def __init__(self, batch_size:int, flush_interval:float):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.lock = asyncio.Lock()
        self.data: list[dict] = []
        self.msg: list[RabbitMessage] = []
        self._timer: asyncio.Task[None] | None = None
        self.processed: list[list[dict]] = [] #used in docs tests

    
    async def add(self, message:dict, raw_message:RabbitMessage, logger:Logger) -> None:
        should_flush = False
        async with self.lock:
            self.data.append(message)
            self.msg.append(raw_message)

            if len(self.data) == 1:
                self._timer = asyncio.create_task(self._on_timeout(logger))
            
            if len(self.data) >= self.batch_size:
                self._cancel_timer()
                should_flush = True
        if should_flush:
            await self._flush(logger)

    def _cancel_timer(self) -> None:
        if self._timer and not self._timer.done():
            self._timer.cancel()
        self._timer = None
    
    async def _on_timeout(self, logger:Logger) -> None:
        try:
            await asyncio.sleep(self.flush_interval)
            await self._flush(logger)
        except asyncio.CancelledError:
            return
    
    async def _flush(self, logger:Logger) -> None:
        async with self.lock:
            if not self.data:
                return

            local_data = list(self.data)
            self.processed.append(local_data)
            local_msg = list(self.msg)
            self.data.clear()
            self.msg.clear()

        try:
            logger.info(f"Processing batch of {len(local_data)} messages: {local_data}")

            await asyncio.sleep(0.5)
            for msg in local_msg:
                await msg.ack()
            logger.info(f"Successfully acknowledged {len(local_msg)} messages.")
        
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            for msg in local_msg:
                await msg.nack()


collector = BatchCollector(BATCH_SIZE, FLUSH_INTERVAL)

@broker.subscriber(queue=example_queue, exchange=example_exchange, 
                   channel=Channel(prefetch_count = BATCH_SIZE * 2), 
                   ack_policy=AckPolicy.MANUAL,)
async def handle_message(message:dict, logger: Logger, raw_message: RabbitMessage):
    await collector.add(message, raw_message, logger)

@app.after_startup
async def declare_topology():
    await broker.declare_exchange(exchange=example_exchange)
    await broker.declare_queue(queue=example_queue)
