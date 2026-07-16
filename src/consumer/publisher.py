import json
import aio_pika
from src.config import settings
from src.logging import logger

async def publish_payment_initiation(
    idempotency_key: str,
    amount: float,
    currency: str,
    user_id: str,
    simulate_transient: bool = False,
    simulate_permanent: bool = False,
    amqp_url: str = settings.get_rabbitmq_url
) -> None:
    payload = {
        "idempotency_key": idempotency_key,
        "amount": amount,
        "currency": currency,
        "user_id": user_id,
        "metadata": {
            "simulate_transient": simulate_transient,
            "simulate_permanent": simulate_permanent
        }
    }
    
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("payment_exchange", type=aio_pika.ExchangeType.DIRECT)
        
        message_body = json.dumps(payload).encode()
        message = aio_pika.Message(
            body=message_body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await exchange.publish(message, routing_key="payment.initiate")
        logger.info("Published payment initiation message", idempotency_key=idempotency_key, payload=payload)
