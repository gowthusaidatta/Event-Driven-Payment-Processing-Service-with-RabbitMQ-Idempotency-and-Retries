import aio_pika
from src.config import settings
from src.logging import logger

class RabbitMQClient:
    def __init__(self, amqp_url: str | None = None):
        self.amqp_url = amqp_url or settings.get_rabbitmq_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.RobustChannel | None = None
        self.logger = logger.bind(service="RabbitMQClient")

    async def connect(self) -> None:
        self.logger.info("Connecting to RabbitMQ", url=self.amqp_url)
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()
        # Prefetch count = 1 to ensure competing consumer patterns and fair dispatch
        await self.channel.set_qos(prefetch_count=1)
        self.logger.info("RabbitMQ connected successfully")

    async def declare_topology(self) -> tuple[aio_pika.RobustQueue, aio_pika.RobustQueue]:
        if not self.channel:
            raise RuntimeError("RabbitMQ client not connected")

        self.logger.info("Declaring exchanges and queues")

        # 1. Declare DLX and DLQ
        dlx = await self.channel.declare_exchange("payment_dlx", type=aio_pika.ExchangeType.DIRECT)
        dlq = await self.channel.declare_queue("payment_dlq", durable=True)
        await dlq.bind(dlx, routing_key="payment.dlq")

        # 2. Declare Main Exchange and Main Queue with DLX configuration
        main_exchange = await self.channel.declare_exchange("payment_exchange", type=aio_pika.ExchangeType.DIRECT)
        
        main_queue = await self.channel.declare_queue(
            "payment_initiation",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "payment_dlx",
                "x-dead-letter-routing-key": "payment.dlq"
            }
        )
        await main_queue.bind(main_exchange, routing_key="payment.initiate")

        self.logger.info("Topology declared successfully")
        return main_queue, dlq

    async def close(self) -> None:
        self.logger.info("Closing RabbitMQ connection")
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.logger.info("RabbitMQ connections closed")
