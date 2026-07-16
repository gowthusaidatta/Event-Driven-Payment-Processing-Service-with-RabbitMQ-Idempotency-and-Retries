import json
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import aio_pika
from src.consumer.consumer import PaymentConsumer
from src.models.payment import PaymentTransaction, PaymentStatus
from src.exceptions import (
    PaymentTransientFailureException,
    PaymentPermanentFailureException,
    PaymentIdempotencyConflictException,
)

@pytest.fixture
def mock_rmq_client():
    client = MagicMock()
    client.connect = AsyncMock()
    client.declare_topology = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
    client.close = AsyncMock()
    return client

@pytest.mark.asyncio
async def test_consumer_on_message_validation_failure(mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = b"invalid json"
    message.nack = AsyncMock()
    
    await consumer.process_message_safe(message)
    
    message.nack.assert_awaited_once_with(requeue=False)

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_on_message_success(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-ok",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = False
    message.ack = AsyncMock()
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class, \
         patch("src.consumer.consumer.PaymentService") as mock_service_class:
        
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(return_value=None)
        
        mock_service = mock_service_class.return_value
        mock_service.process_payment = AsyncMock()
        
        await consumer.process_message_safe(message)
        
        message.ack.assert_awaited_once()
        mock_session.commit.assert_awaited_once()

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_on_message_already_completed(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-comp",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = False
    message.ack = AsyncMock()
    
    existing_tx = PaymentTransaction(
        idempotency_key="key-comp",
        amount=10.00,
        currency="USD",
        user_id="user-123",
        status=PaymentStatus.COMPLETED
    )
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class:
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(return_value=existing_tx)
        
        await consumer.process_message_safe(message)
        
        message.ack.assert_awaited_once()

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_transient_failure_retry(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-trans",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = False
    message.nack = AsyncMock()
    
    existing_tx = PaymentTransaction(
        idempotency_key="key-trans",
        amount=10.00,
        currency="USD",
        user_id="user-123",
        status=PaymentStatus.INITIATED,
        retry_count=0
    )
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class, \
         patch("src.consumer.consumer.PaymentService") as mock_service_class, \
         patch("asyncio.sleep") as mock_sleep:
        
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(side_effect=[existing_tx, existing_tx])
        mock_repo.update = AsyncMock()
        
        mock_service = mock_service_class.return_value
        mock_service.process_payment = AsyncMock(side_effect=PaymentTransientFailureException("Network error"))
        
        await consumer.process_message_safe(message)
        
        assert existing_tx.retry_count == 1
        mock_repo.update.assert_called_once_with(existing_tx)
        mock_session.commit.assert_called_once()
        
        mock_sleep.assert_called_once_with(1.0)
        message.nack.assert_awaited_once_with(requeue=True)

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_transient_failure_max_retries_reached(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-max-retries",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = True
    message.nack = AsyncMock()
    
    existing_tx = PaymentTransaction(
        idempotency_key="key-max-retries",
        amount=10.00,
        currency="USD",
        user_id="user-123",
        status=PaymentStatus.PROCESSING,
        retry_count=2
    )
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class, \
         patch("src.consumer.consumer.PaymentService") as mock_service_class:
        
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(side_effect=[existing_tx, existing_tx, existing_tx])
        mock_repo.update = AsyncMock()
        
        mock_service = mock_service_class.return_value
        mock_service.process_payment = AsyncMock(side_effect=PaymentTransientFailureException("Network error"))
        
        await consumer.process_message_safe(message)
        
        assert existing_tx.status == PaymentStatus.FAILED
        assert existing_tx.retry_count == 3
        assert mock_repo.update.call_count == 2
        
        message.nack.assert_awaited_once_with(requeue=False)

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_permanent_failure(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-perm",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = False
    message.nack = AsyncMock()
    
    existing_tx = PaymentTransaction(
        idempotency_key="key-perm",
        amount=10.00,
        currency="USD",
        user_id="user-123",
        status=PaymentStatus.INITIATED,
        retry_count=0
    )
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class, \
         patch("src.consumer.consumer.PaymentService") as mock_service_class:
        
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(side_effect=[existing_tx, existing_tx])
        mock_repo.update = AsyncMock()
        
        mock_service = mock_service_class.return_value
        mock_service.process_payment = AsyncMock(side_effect=PaymentPermanentFailureException("Invalid card number"))
        
        await consumer.process_message_safe(message)
        
        assert existing_tx.status == PaymentStatus.FAILED
        mock_repo.update.assert_called_once_with(existing_tx)
        message.nack.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_consumer_message_during_shutdown(mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    consumer.is_shutting_down = True
    
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.nack = AsyncMock()
    
    await consumer.on_message(message)
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_consumer_process_message_safe_unexpected_exception(mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.nack = AsyncMock()
    
    # Mock _process_message_payload to raise ValueError
    with patch.object(consumer, "_process_message_payload", side_effect=ValueError("Unexpected processing error")):
        await consumer.process_message_safe(message)
        message.nack.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_consumer_stop_waits_for_active_tasks(mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    consumer.queue = AsyncMock()
    consumer.consumer_tag = "tag-123"
    
    # Add a dummy pending task that completes after a tiny sleep
    async def dummy_task():
        await asyncio.sleep(0.1)
    
    task = asyncio.create_task(dummy_task())
    consumer.active_tasks.add(task)
    task.add_done_callback(consumer.active_tasks.discard)
    
    await consumer.stop()
    
    # Assert consumer tag cancelled and client closed
    consumer.queue.cancel.assert_awaited_once_with("tag-123")
    mock_rmq_client.close.assert_awaited_once()
    assert len(consumer.active_tasks) == 0


@pytest.mark.asyncio
async def test_consumer_on_message_spawns_task(mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    
    # Mock process_message_safe to do nothing
    with patch.object(consumer, "process_message_safe", new_callable=AsyncMock) as mock_process:
        await consumer.on_message(message)
        # Give control back to loop to let task run
        await asyncio.sleep(0.05)
        mock_process.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_consumer_nack_exception_handled(mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.nack = AsyncMock(side_effect=Exception("Nack failed connection error"))
    
    # Mock _process_message_payload to raise ValueError so it calls nack
    with patch.object(consumer, "_process_message_payload", side_effect=ValueError("Failed")):
        # Should not raise exception
        await consumer.process_message_safe(message)
        message.nack.assert_awaited_once()


@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_idempotency_conflict(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-conflict",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = False
    message.nack = AsyncMock()
    
    existing_tx = PaymentTransaction(
        idempotency_key="key-conflict",
        amount=10.00,
        currency="USD",
        user_id="user-123",
        status=PaymentStatus.PROCESSING,
        retry_count=0
    )
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class:
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(return_value=existing_tx)
        
        await consumer.process_message_safe(message)
        
        message.nack.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
@patch("src.database.async_session_maker")
async def test_consumer_retry_processing_state(mock_session_maker, mock_rmq_client):
    consumer = PaymentConsumer(mock_rmq_client, max_retries=3)
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    
    payload = {
        "idempotency_key": "key-retry-proc",
        "amount": 10.00,
        "currency": "USD",
        "user_id": "user-123"
    }
    message = AsyncMock(spec=aio_pika.IncomingMessage)
    message.body = json.dumps(payload).encode()
    message.redelivered = True
    message.ack = AsyncMock()
    
    existing_tx = PaymentTransaction(
        idempotency_key="key-retry-proc",
        amount=10.00,
        currency="USD",
        user_id="user-123",
        status=PaymentStatus.PROCESSING,
        retry_count=1
    )
    
    with patch("src.consumer.consumer.PaymentRepository") as mock_repo_class, \
         patch("src.consumer.consumer.PaymentService") as mock_service_class:
        
        mock_repo = mock_repo_class.return_value
        mock_repo.get_by_idempotency_key = AsyncMock(return_value=existing_tx)
        
        mock_service = mock_service_class.return_value
        mock_service.process_payment = AsyncMock()
        
        await consumer.process_message_safe(message)
        
        message.ack.assert_awaited_once()
        mock_service.process_payment.assert_called_once()
        args, kwargs = mock_service.process_payment.call_args
        assert args[0].idempotency_key == payload["idempotency_key"]
        assert kwargs["is_retry"] is True


