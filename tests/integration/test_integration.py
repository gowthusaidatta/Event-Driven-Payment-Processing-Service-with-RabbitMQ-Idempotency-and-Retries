import asyncio
import json
import os
import atexit
import pytest
import pytest_asyncio
from decimal import Decimal
import httpx
import aio_pika
import pika
import psycopg2

from testcontainers.postgres import PostgresContainer
from testcontainers.rabbitmq import RabbitMqContainer

# ==============================================================================
# 1. Start containers globally at module load time to set ENV before app imports
# ==============================================================================
postgres_container = PostgresContainer("postgres:16-alpine")
postgres_container.start()
atexit.register(postgres_container.stop)

rabbitmq_container = RabbitMqContainer("rabbitmq:3-management-alpine")
rabbitmq_container.start()
atexit.register(rabbitmq_container.stop)

db_url = postgres_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")

rmq_host = rabbitmq_container.get_container_host_ip()
rmq_port = rabbitmq_container.get_exposed_port(5672)
amqp_url = f"amqp://guest:guest@{rmq_host}:{rmq_port}/"

# Export variables to OS environment
os.environ["DATABASE_URL"] = db_url
os.environ["RABBITMQ_URL"] = amqp_url
os.environ["MAX_RETRIES"] = "3"

# ==============================================================================
# 2. Import application modules (they will boot with the correct ENV configuration)
# ==============================================================================
from src.config import settings
from src.database import Base, async_session_maker, engine
from src.models.payment import PaymentTransaction, PaymentStatus
from src.repositories.payment_repository import PaymentRepository
from src.consumer.rabbitmq import RabbitMQClient
from src.consumer.consumer import PaymentConsumer
from src.consumer.publisher import publish_payment_initiation
from src.main import app

@pytest_asyncio.fixture(scope="module", autouse=True)
async def setup_integration_db():
    # Wait for PostgreSQL container using psycopg2 connection
    pg_host = postgres_container.get_container_host_ip()
    pg_port = postgres_container.get_exposed_port(5432)
    pg_ready = False
    for _ in range(30):
        try:
            conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                user=postgres_container.username,
                password=postgres_container.password,
                database=postgres_container.dbname
            )
            conn.close()
            pg_ready = True
            break
        except Exception:
            await asyncio.sleep(0.5)
    if not pg_ready:
        raise RuntimeError("Postgres container failed to become ready in time")

    # Wait for RabbitMQ container using pika connection
    pika_params = pika.URLParameters(amqp_url)
    rmq_ready = False
    for _ in range(30):
        try:
            conn = pika.BlockingConnection(pika_params)
            conn.close()
            rmq_ready = True
            break
        except Exception:
            await asyncio.sleep(0.5)
    if not rmq_ready:
        raise RuntimeError("RabbitMQ container failed to become ready in time")

    # Initialize tables using the globally configured engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    yield
    
    # Dispose connection pools after tests
    await engine.dispose()

@pytest.mark.asyncio
async def test_integration_flow():
    # Start consumer connected to dynamic RabbitMQ container
    rmq_client = RabbitMQClient(amqp_url)
    consumer = PaymentConsumer(rmq_client, max_retries=3)
    await consumer.start()
    
    # Tiny sleep to let consumer attach to the broker
    await asyncio.sleep(0.5)

    try:
        # ==========================================
        # 1. Test GET /health
        # ==========================================
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
            health_response = await ac.get("/health")
            assert health_response.status_code == 200
            health_json = health_response.json()
            assert health_json["status"] == "healthy"
            assert health_json["postgres"] == "up"
            assert health_json["rabbitmq"] == "up"

        # ==========================================
        # 2. Test Success Payment Flow
        # ==========================================
        idempotency_key_success = "key-integration-success"
        await publish_payment_initiation(
            idempotency_key=idempotency_key_success,
            amount=99.99,
            currency="USD",
            user_id="user-integration-1",
            amqp_url=amqp_url
        )
        
        success = False
        for _ in range(30):
            await asyncio.sleep(0.5)
            async with async_session_maker() as session:
                repo = PaymentRepository(session)
                tx = await repo.get_by_idempotency_key(idempotency_key_success)
                if tx and tx.status == PaymentStatus.COMPLETED:
                    success = True
                    break
        assert success, "Payment did not transition to COMPLETED"

        # ==========================================
        # 3. Test Transient Failure & Retry Flow
        # ==========================================
        idempotency_key_retry = "key-integration-retry"
        await publish_payment_initiation(
            idempotency_key=idempotency_key_retry,
            amount=45.50,
            currency="EUR",
            user_id="user-integration-2",
            simulate_transient=True,
            amqp_url=amqp_url
        )
        
        success_retry = False
        for _ in range(40):
            await asyncio.sleep(0.5)
            async with async_session_maker() as session:
                repo = PaymentRepository(session)
                tx = await repo.get_by_idempotency_key(idempotency_key_retry)
                if tx and tx.status == PaymentStatus.COMPLETED:
                    assert tx.retry_count == 2
                    success_retry = True
                    break
        assert success_retry, "Retry flow failed to complete"

        # ==========================================
        # 4. Test Permanent Failure & DLQ Flow
        # ==========================================
        idempotency_key_dlq = "key-integration-dlq"
        await publish_payment_initiation(
            idempotency_key=idempotency_key_dlq,
            amount=150.00,
            currency="GBP",
            user_id="user-integration-3",
            simulate_permanent=True,
            amqp_url=amqp_url
        )
        
        # Wait for status in DB to transition to FAILED
        success_dlq = False
        for _ in range(30):
            await asyncio.sleep(0.5)
            async with async_session_maker() as session:
                repo = PaymentRepository(session)
                tx = await repo.get_by_idempotency_key(idempotency_key_dlq)
                if tx and tx.status == PaymentStatus.FAILED:
                    success_dlq = True
                    break
        assert success_dlq, "Permanent failure did not result in status FAILED"

        connection = await aio_pika.connect_robust(amqp_url)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("payment_dlq", durable=True)
            dlq_message = await queue.get(no_ack=True)
            assert dlq_message is not None, "Message was not routed to payment_dlq"
            
            dlq_body = json.loads(dlq_message.body.decode())
            assert dlq_body["idempotency_key"] == idempotency_key_dlq
            assert dlq_body["metadata"]["simulate_permanent"] is True

        # ==========================================
        # 5. Test GET /metrics
        # ==========================================
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
            metrics_response = await ac.get("/metrics")
            assert metrics_response.status_code == 200
            metrics_text = metrics_response.text
            
            assert "payment_processor_messages_consumed_total" in metrics_text
            assert "payment_processor_payments_successful_total" in metrics_text
            assert "payment_processor_payments_failed_total" in metrics_text
            assert "payment_processor_retries_total" in metrics_text

    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_app_lifespan():
    # Trigger application lifespan (startup and shutdown) explicitly using the context manager
    from src.main import lifespan
    async with lifespan(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/")
            assert response.status_code == 200
            assert response.json()["service"] == "payment-processor"


@pytest.mark.asyncio
async def test_app_lifespan_consumer_start_failure():
    from unittest.mock import patch, AsyncMock
    from src.main import lifespan
    
    with patch("src.main.PaymentConsumer") as mock_consumer_class:
        mock_cons = mock_consumer_class.return_value
        mock_cons.start.side_effect = Exception("Simulated RabbitMQ consumer startup failure")
        mock_cons.stop = AsyncMock()
        
        async with lifespan(app):
            pass
