import asyncio
import json
from typing import Any, Optional
from pydantic import ValidationError
import aio_pika
from src.config import settings
import src.database
from src.exceptions import (
    PaymentTransientFailureException,
    PaymentPermanentFailureException,
    PaymentIdempotencyConflictException,
)
from src.models.payment import PaymentTransaction, PaymentStatus
from src.repositories.payment_repository import PaymentRepository
from src.services.payment_service import PaymentService
from src.services.retry_service import RetryService
from src.metrics import MESSAGES_CONSUMED, PAYMENTS_SUCCESSFUL, PAYMENTS_FAILED, RETRIES
from src.logging import logger

class PaymentConsumer:
    def __init__(self, rmq_client, max_retries: int = settings.max_retries):
        self.rmq_client = rmq_client
        self.max_retries = max_retries
        self.retry_service = RetryService(max_retries=max_retries)
        self.logger = logger.bind(service="PaymentConsumer")
        self.is_shutting_down = False
        self.active_tasks = set()
        self.consumer_tag = None
        self.queue = None

    async def start(self) -> None:
        await self.rmq_client.connect()
        self.queue, _ = await self.rmq_client.declare_topology()
        self.logger.info("Starting message consumption")
        
        # Start consuming messages
        self.consumer_tag = await self.queue.consume(
            self.on_message,
            no_ack=False
        )

    async def stop(self) -> None:
        self.logger.info("Graceful shutdown initiated. Stopping new message consumption.")
        self.is_shutting_down = True
        
        # 1. Stop consuming new messages
        if self.queue and self.consumer_tag:
            try:
                await self.queue.cancel(self.consumer_tag)
                self.logger.info("Consumer tag cancelled successfully")
            except Exception as e:
                self.logger.error("Error cancelling consumer tag", error=str(e))
        
        # 2. Wait for active tasks to drain
        if self.active_tasks:
            self.logger.info("Waiting for active tasks to drain", count=len(self.active_tasks))
            # Gather active tasks and shield them to finish execution
            await asyncio.gather(*[asyncio.shield(task) for task in self.active_tasks], return_exceptions=True)
            self.logger.info("All active tasks completed")
            
        # 3. Close RabbitMQ connection
        await self.rmq_client.close()
        self.logger.info("Consumer stopped successfully")

    async def on_message(self, message: aio_pika.IncomingMessage) -> None:
        if self.is_shutting_down:
            self.logger.warn("Received message during shutdown. Requeueing.")
            await message.nack(requeue=True)
            return

        # Track active task for graceful shutdown
        task = asyncio.create_task(self.process_message_safe(message))
        self.active_tasks.add(task)
        task.add_done_callback(self.active_tasks.discard)

    async def process_message_safe(self, message: aio_pika.IncomingMessage) -> None:
        MESSAGES_CONSUMED.inc()
        try:
            await self._process_message_payload(message)
        except Exception as e:
            self.logger.exception("Unhandled error during message processing", error=str(e))
            try:
                await message.nack(requeue=False)
            except Exception as nack_err:
                self.logger.error("Failed to nack message", error=str(nack_err))

    async def _process_message_payload(self, message: aio_pika.IncomingMessage) -> None:
        body_str = message.body.decode()
        self.logger.info("Received message for processing", message_id=message.message_id, body=body_str)
        
        # 1. Parse payload
        try:
            body_json = json.loads(body_str)
            from src.schemas.payment import PaymentInitiateRequest
            req = PaymentInitiateRequest(**body_json)
        except (json.JSONDecodeError, ValidationError) as e:
            self.logger.error("Payload validation failed", error=str(e), body=body_str)
            PAYMENTS_FAILED.inc()
            await message.nack(requeue=False)
            return

        # 2. Process using db session
        async with src.database.async_session_maker() as session:
            repo = PaymentRepository(session)
            payment_service = PaymentService(repo)
            
            # Check existing transaction state
            tx = await repo.get_by_idempotency_key(req.idempotency_key)
            is_retry = False
            if tx:
                if tx.status == PaymentStatus.COMPLETED:
                    self.logger.info("Payment already completed. Idempotent success response.", idempotency_key=req.idempotency_key)
                    PAYMENTS_SUCCESSFUL.inc()
                    await message.ack()
                    return
                elif tx.status == PaymentStatus.FAILED:
                    self.logger.info("Payment already failed permanently. Rejecting message.", idempotency_key=req.idempotency_key)
                    PAYMENTS_FAILED.inc()
                    await message.nack(requeue=False)
                    return
                elif tx.status == PaymentStatus.PROCESSING:
                    if message.redelivered or tx.retry_count > 0:
                        is_retry = True
                    else:
                        self.logger.warn("Idempotency conflict: message is already being processed", idempotency_key=req.idempotency_key)
                        PAYMENTS_FAILED.inc()
                        await message.nack(requeue=False)
                        return

            try:
                await payment_service.process_payment(req, is_retry=is_retry)
                await session.commit()
                PAYMENTS_SUCCESSFUL.inc()
                await message.ack()
                self.logger.info("Successfully processed payment", idempotency_key=req.idempotency_key)
            except PaymentTransientFailureException as e:
                await session.rollback()
                await self._handle_transient_error(message, req, e)
            except PaymentPermanentFailureException as e:
                await session.rollback()
                await self._handle_permanent_error(message, req, e)
            except PaymentIdempotencyConflictException as e:
                await session.rollback()
                self.logger.warn("PaymentIdempotencyConflictException in processing", error=str(e))
                PAYMENTS_FAILED.inc()
                await message.nack(requeue=False)
            except Exception as e:
                await session.rollback()
                self.logger.error("Unexpected error in process_payment", error=str(e))
                await self._handle_transient_error(message, req, e)

    async def _handle_transient_error(self, message: aio_pika.IncomingMessage, req: Any, error: Exception) -> None:
        idempotency_key = req.idempotency_key if req else "unknown"
        self.logger.warn("Handling transient error", idempotency_key=idempotency_key, error=str(error))
        
        retry_count = 0
        if req:
            async with src.database.async_session_maker() as session:
                repo = PaymentRepository(session)
                tx = await repo.get_by_idempotency_key(idempotency_key)
                if tx:
                    tx.retry_count += 1
                    tx.last_error_message = str(error)
                    retry_count = tx.retry_count
                    await repo.update(tx)
                    await session.commit()
                    self.logger.info("Updated retry count in DB", idempotency_key=idempotency_key, retry_count=retry_count)

        if self.retry_service.is_retryable(retry_count):
            RETRIES.inc()
            delay = self.retry_service.calculate_backoff(retry_count)
            self.logger.info("Backing off before NACK/requeue", idempotency_key=idempotency_key, delay_seconds=delay)
            await asyncio.sleep(delay)
            await message.nack(requeue=True)
            self.logger.info("NACKed message with requeue=True", idempotency_key=idempotency_key)
        else:
            self.logger.error("Max retries exceeded. Marking payment as FAILED.", idempotency_key=idempotency_key)
            if req:
                async with src.database.async_session_maker() as session:
                    repo = PaymentRepository(session)
                    tx = await repo.get_by_idempotency_key(idempotency_key)
                    if tx:
                        tx.status = PaymentStatus.FAILED
                        tx.last_error_message = f"Max retries exceeded. Last error: {str(error)}"
                        await repo.update(tx)
                        await session.commit()
            
            PAYMENTS_FAILED.inc()
            await message.nack(requeue=False)
            self.logger.info("NACKed message with requeue=False (routed to DLQ)", idempotency_key=idempotency_key)

    async def _handle_permanent_error(self, message: aio_pika.IncomingMessage, req: Any, error: Exception) -> None:
        idempotency_key = req.idempotency_key if req else "unknown"
        self.logger.error("Handling permanent error", idempotency_key=idempotency_key, error=str(error))
        
        if req:
            async with src.database.async_session_maker() as session:
                repo = PaymentRepository(session)
                tx = await repo.get_by_idempotency_key(idempotency_key)
                if tx:
                    tx.status = PaymentStatus.FAILED
                    tx.last_error_message = str(error)
                    await repo.update(tx)
                    await session.commit()
                    
        PAYMENTS_FAILED.inc()
        await message.nack(requeue=False)
        self.logger.info("NACKed message with requeue=False (routed to DLQ)", idempotency_key=idempotency_key)
