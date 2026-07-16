import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.config import settings
from src.database import engine, Base
from src.logging import configure_logging, logger
from src.api.health import router as health_router
from src.api.metrics import router as metrics_router
from src.consumer.rabbitmq import RabbitMQClient
from src.consumer.consumer import PaymentConsumer

# Set up logging early
configure_logging()

consumer_task = None
consumer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task, consumer
    logger.info("Application starting up")
    
    # 1. Initialize database schema automatically
    logger.info("Initializing database tables")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables initialized")

    # 2. Run RabbitMQ Consumer in background
    rmq_client = RabbitMQClient()
    consumer = PaymentConsumer(rmq_client)
    
    async def start_consumer():
        try:
            await consumer.start()
        except Exception as e:
            logger.exception("Failed to start RabbitMQ consumer", error=str(e))
            
    consumer_task = asyncio.create_task(start_consumer())
    logger.info("Background RabbitMQ consumer task started")

    yield
    
    logger.info("Application shutting down")
    
    # 3. Graceful shutdown of consumer
    if consumer:
        await consumer.stop()
        
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
            
    # Dispose SQLAlchemy connections
    await engine.dispose()
    logger.info("Application shutdown complete")

app = FastAPI(
    title="Event-Driven Payment Processor Service",
    description="Production-ready, event-driven payment processing service with RabbitMQ, Postgres, and Idempotency",
    version="1.0.0",
    lifespan=lifespan
)

# Mount Routers
app.include_router(health_router)
app.include_router(metrics_router)

@app.get("/")
async def root():
    return {
        "service": "payment-processor",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics"
        }
    }
