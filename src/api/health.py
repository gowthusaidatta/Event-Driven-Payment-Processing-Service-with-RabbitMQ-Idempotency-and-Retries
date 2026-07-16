from fastapi import APIRouter, Response, status
from sqlalchemy import text
import src.database
from src.consumer.rabbitmq import RabbitMQClient
from src.logging import logger

router = APIRouter()

@router.get("/health")
async def health_check(response: Response):
    postgres_ok = False
    rabbitmq_ok = False
    
    # 1. Check Postgres
    try:
        async with src.database.async_session_maker() as session:
            await session.execute(text("SELECT 1"))
            postgres_ok = True
    except Exception as e:
        logger.error("Health check failed for PostgreSQL", error=str(e))
        
    # 2. Check RabbitMQ
    try:
        client = RabbitMQClient()
        await client.connect()
        if client.connection and not client.connection.is_closed:
            rabbitmq_ok = True
        await client.close()
    except Exception as e:
        logger.error("Health check failed for RabbitMQ", error=str(e))
        
    if postgres_ok and rabbitmq_ok:
        return {
            "status": "healthy",
            "postgres": "up",
            "rabbitmq": "up"
        }
    else:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "unhealthy",
            "postgres": "up" if postgres_ok else "down",
            "rabbitmq": "up" if rabbitmq_ok else "down"
        }
