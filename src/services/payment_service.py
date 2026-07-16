import uuid
import structlog
from sqlalchemy.exc import IntegrityError
from src.exceptions import (
    PaymentTransientFailureException,
    PaymentPermanentFailureException,
    PaymentIdempotencyConflictException,
)
from src.models.payment import PaymentTransaction, PaymentStatus
from src.repositories.payment_repository import PaymentRepository
from src.schemas.payment import PaymentInitiateRequest
from src.logging import logger

class PaymentService:
    def __init__(self, repository: PaymentRepository):
        self.repository = repository
        self.logger = logger.bind(service="PaymentService")

    async def process_payment(self, request: PaymentInitiateRequest, is_retry: bool = False) -> PaymentTransaction:
        self.logger.info("Processing payment request", idempotency_key=request.idempotency_key, is_retry=is_retry)
        
        # 1. Idempotency Check
        existing_tx = await self.repository.get_by_idempotency_key(request.idempotency_key)
        if existing_tx:
            self.logger.info("Found existing transaction for idempotency key", 
                             idempotency_key=request.idempotency_key, 
                             status=existing_tx.status)
            if existing_tx.status == PaymentStatus.COMPLETED:
                return existing_tx
            elif existing_tx.status == PaymentStatus.PROCESSING:
                if not is_retry:
                    raise PaymentIdempotencyConflictException(
                        f"Payment with idempotency key '{request.idempotency_key}' is already being processed."
                    )
                # If is_retry=True, we proceed with processing.
                tx = existing_tx
            elif existing_tx.status == PaymentStatus.FAILED:
                raise PaymentPermanentFailureException(
                    f"Payment with idempotency key '{request.idempotency_key}' already permanently failed."
                )
            else:
                tx = existing_tx
        else:
            # 2. Create INITIATED Transaction
            tx = PaymentTransaction(
                idempotency_key=request.idempotency_key,
                amount=request.amount,
                currency=request.currency,
                user_id=request.user_id,
                status=PaymentStatus.INITIATED,
                retry_count=0,
            )
            try:
                tx = await self.repository.create(tx)
                await self.repository.session.flush()
            except IntegrityError as e:
                # Handle race conditions (Unique constraint handling)
                await self.repository.session.rollback()
                self.logger.warn("IntegrityError on creation, reloading transaction", 
                                 idempotency_key=request.idempotency_key)
                existing_tx = await self.repository.get_by_idempotency_key(request.idempotency_key)
                if not existing_tx:
                    raise e
                if existing_tx.status == PaymentStatus.COMPLETED:
                    return existing_tx
                elif existing_tx.status == PaymentStatus.PROCESSING:
                    if not is_retry:
                        raise PaymentIdempotencyConflictException(
                            f"Payment with idempotency key '{request.idempotency_key}' is already being processed (race)."
                        )
                    tx = existing_tx
                elif existing_tx.status == PaymentStatus.FAILED:
                    raise PaymentPermanentFailureException(
                        f"Payment with idempotency key '{request.idempotency_key}' already permanently failed (race)."
                    )
                else:
                    tx = existing_tx

        # 3. Transition to PROCESSING
        tx.status = PaymentStatus.PROCESSING
        await self.repository.update(tx)
        await self.repository.session.commit()

        # 4. Execute Payment (Simulated External Gateway)
        try:
            await self._execute_gateway_call(tx, request)
            
            # 5. Transition to COMPLETED
            tx.status = PaymentStatus.COMPLETED
            tx.last_error_message = None
            await self.repository.update(tx)
            await self.repository.session.commit()
            self.logger.info("Payment processed successfully", idempotency_key=request.idempotency_key, transaction_id=tx.id)
            return tx
            
        except PaymentTransientFailureException as e:
            # Roll back any other uncommitted session changes, then save error details
            await self.repository.session.rollback()
            tx.last_error_message = str(e)
            await self.repository.update(tx)
            await self.repository.session.commit()
            self.logger.warn("Transient failure in payment processing", idempotency_key=request.idempotency_key, error=str(e))
            raise e
            
        except PaymentPermanentFailureException as e:
            # Roll back any other uncommitted session changes, then save permanent failure status
            await self.repository.session.rollback()
            tx.status = PaymentStatus.FAILED
            tx.last_error_message = str(e)
            await self.repository.update(tx)
            await self.repository.session.commit()
            self.logger.error("Permanent failure in payment processing", idempotency_key=request.idempotency_key, error=str(e))
            raise e

    async def _execute_gateway_call(self, tx: PaymentTransaction, request: PaymentInitiateRequest) -> None:
        """Simulates external payment gateway call, incorporating simulated failure metadata."""
        if request.metadata.simulate_permanent:
            raise PaymentPermanentFailureException("Simulated permanent gateway failure")
            
        if request.metadata.simulate_transient:
            # If retry_count is less than 2, fail transiently.
            # On the 3rd attempt (retry_count == 2), we let it succeed!
            if tx.retry_count < 2:
                raise PaymentTransientFailureException(
                    f"Simulated transient failure (attempt {tx.retry_count + 1}/3)"
                )
            else:
                self.logger.info("Simulated transient failure bypassed as retry threshold reached", 
                                 retry_count=tx.retry_count)
        return
