import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
from src.exceptions import (
    PaymentTransientFailureException,
    PaymentPermanentFailureException,
    PaymentIdempotencyConflictException,
)
from src.models.payment import PaymentTransaction, PaymentStatus
from src.schemas.payment import PaymentInitiateRequest, PaymentMetadata
from src.services.payment_service import PaymentService

@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.get_by_idempotency_key = AsyncMock()
    
    async def mock_create(tx):
        tx.id = "some-uuid"
        return tx
    repo.create = AsyncMock(side_effect=mock_create)
    repo.update = AsyncMock(side_effect=lambda tx: tx)
    repo.session = MagicMock()
    repo.session.flush = AsyncMock()
    repo.session.commit = AsyncMock()
    repo.session.rollback = AsyncMock()
    return repo

@pytest.mark.asyncio
async def test_process_payment_new_success(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-new",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    res = await service.process_payment(req)
    assert res.status == PaymentStatus.COMPLETED
    assert res.idempotency_key == "key-new"
    assert mock_repo.create.call_count == 1
    assert mock_repo.update.call_count == 2  # Transitions to PROCESSING, then COMPLETED

@pytest.mark.asyncio
async def test_process_payment_idempotent_completed(mock_repo):
    existing_tx = PaymentTransaction(
        idempotency_key="key-comp",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.COMPLETED
    )
    mock_repo.get_by_idempotency_key.return_value = existing_tx
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-comp",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    res = await service.process_payment(req)
    assert res == existing_tx
    assert mock_repo.create.call_count == 0
    assert mock_repo.update.call_count == 0

@pytest.mark.asyncio
async def test_process_payment_idempotency_conflict_processing(mock_repo):
    existing_tx = PaymentTransaction(
        idempotency_key="key-proc",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.PROCESSING
    )
    mock_repo.get_by_idempotency_key.return_value = existing_tx
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-proc",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    with pytest.raises(PaymentIdempotencyConflictException):
        await service.process_payment(req, is_retry=False)

@pytest.mark.asyncio
async def test_process_payment_allow_retry_processing(mock_repo):
    existing_tx = PaymentTransaction(
        idempotency_key="key-retry",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.PROCESSING,
        retry_count=1
    )
    mock_repo.get_by_idempotency_key.return_value = existing_tx
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-retry",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    # Acts as a retry, so it proceeds and finishes successfully
    res = await service.process_payment(req, is_retry=True)
    assert res.status == PaymentStatus.COMPLETED
    assert mock_repo.update.call_count == 2

@pytest.mark.asyncio
async def test_process_payment_idempotent_failed(mock_repo):
    existing_tx = PaymentTransaction(
        idempotency_key="key-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.FAILED
    )
    mock_repo.get_by_idempotency_key.return_value = existing_tx
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    with pytest.raises(PaymentPermanentFailureException):
        await service.process_payment(req)

@pytest.mark.asyncio
async def test_process_payment_simulate_permanent_failure(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-perm-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        metadata=PaymentMetadata(simulate_permanent=True)
    )
    
    with pytest.raises(PaymentPermanentFailureException):
        await service.process_payment(req)
        
    assert mock_repo.update.call_count == 2 # 1 for PROCESSING, 1 for FAILED

@pytest.mark.asyncio
async def test_process_payment_simulate_transient_failure(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-trans-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        metadata=PaymentMetadata(simulate_transient=True)
    )
    
    # 1. First execution: retry_count = 0. Gateway should fail transiently.
    with pytest.raises(PaymentTransientFailureException):
        await service.process_payment(req)
    assert mock_repo.update.call_count == 2 # PROCESSING and then error state updated
    
    # Simulate database updates in between by mocking returning the updated transaction
    failed_tx = PaymentTransaction(
        idempotency_key="key-trans-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.PROCESSING,
        retry_count=1
    )
    mock_repo.get_by_idempotency_key.return_value = failed_tx
    
    # 2. Second execution (retry_count = 1): still < 2, gateway fails again
    with pytest.raises(PaymentTransientFailureException):
        await service.process_payment(req, is_retry=True)
        
    # Simulate DB update retry_count = 2
    success_tx = PaymentTransaction(
        idempotency_key="key-trans-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.PROCESSING,
        retry_count=2
    )
    mock_repo.get_by_idempotency_key.return_value = success_tx
    
    # 3. Third execution (retry_count = 2): retry threshold reached, succeeds!
    res = await service.process_payment(req, is_retry=True)
    assert res.status == PaymentStatus.COMPLETED

@pytest.mark.asyncio
async def test_process_payment_integrity_error_handling(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    
    # Mock create to raise IntegrityError (simulating concurrent insert race condition)
    mock_repo.create.side_effect = IntegrityError(None, None, None)
    
    service = PaymentService(mock_repo)
    
    req = PaymentInitiateRequest(
        idempotency_key="key-race",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    # Prepare the state for the reload of the record that got inserted by the other process
    inserted_tx = PaymentTransaction(
        idempotency_key="key-race",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.COMPLETED
    )
    
    # When reload happens, return the transaction inserted concurrently
    mock_repo.get_by_idempotency_key.side_effect = [None, inserted_tx]
    
    res = await service.process_payment(req)
    assert res == inserted_tx
    assert mock_repo.session.rollback.call_count == 1


@pytest.mark.asyncio
async def test_process_payment_integrity_error_processing_conflict(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    mock_repo.create.side_effect = IntegrityError(None, None, None)
    
    service = PaymentService(mock_repo)
    req = PaymentInitiateRequest(
        idempotency_key="key-race-proc",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    inserted_tx = PaymentTransaction(
        idempotency_key="key-race-proc",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.PROCESSING
    )
    mock_repo.get_by_idempotency_key.side_effect = [None, inserted_tx]
    
    with pytest.raises(PaymentIdempotencyConflictException):
        await service.process_payment(req, is_retry=False)


@pytest.mark.asyncio
async def test_process_payment_integrity_error_processing_retry(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    mock_repo.create.side_effect = IntegrityError(None, None, None)
    
    service = PaymentService(mock_repo)
    req = PaymentInitiateRequest(
        idempotency_key="key-race-retry",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    inserted_tx = PaymentTransaction(
        idempotency_key="key-race-retry",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.PROCESSING,
        retry_count=1
    )
    mock_repo.get_by_idempotency_key.side_effect = [None, inserted_tx]
    
    res = await service.process_payment(req, is_retry=True)
    assert res.status == PaymentStatus.COMPLETED


@pytest.mark.asyncio
async def test_process_payment_integrity_error_failed(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    mock_repo.create.side_effect = IntegrityError(None, None, None)
    
    service = PaymentService(mock_repo)
    req = PaymentInitiateRequest(
        idempotency_key="key-race-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    inserted_tx = PaymentTransaction(
        idempotency_key="key-race-fail",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.FAILED
    )
    mock_repo.get_by_idempotency_key.side_effect = [None, inserted_tx]
    
    with pytest.raises(PaymentPermanentFailureException):
        await service.process_payment(req)


@pytest.mark.asyncio
async def test_process_payment_integrity_error_initiated(mock_repo):
    mock_repo.get_by_idempotency_key.return_value = None
    mock_repo.create.side_effect = IntegrityError(None, None, None)
    
    service = PaymentService(mock_repo)
    req = PaymentInitiateRequest(
        idempotency_key="key-race-init",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    inserted_tx = PaymentTransaction(
        idempotency_key="key-race-init",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.INITIATED
    )
    mock_repo.get_by_idempotency_key.side_effect = [None, inserted_tx]
    
    res = await service.process_payment(req)
    assert res.status == PaymentStatus.COMPLETED


@pytest.mark.asyncio
async def test_process_payment_existing_initiated(mock_repo):
    # Tests line 42 fallback (status INITIATED)
    existing_tx = PaymentTransaction(
        idempotency_key="key-init-exists",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1",
        status=PaymentStatus.INITIATED
    )
    mock_repo.get_by_idempotency_key.return_value = existing_tx
    
    service = PaymentService(mock_repo)
    req = PaymentInitiateRequest(
        idempotency_key="key-init-exists",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    res = await service.process_payment(req)
    assert res.status == PaymentStatus.COMPLETED


@pytest.mark.asyncio
async def test_process_payment_integrity_error_none_reloaded(mock_repo):
    # Tests line 63 raise e fallback when reload returns None
    mock_repo.get_by_idempotency_key.side_effect = [None, None]
    mock_repo.create.side_effect = IntegrityError("msg", "params", "orig")
    
    service = PaymentService(mock_repo)
    req = PaymentInitiateRequest(
        idempotency_key="key-race-none",
        amount=Decimal("50.00"),
        currency="USD",
        user_id="user-1"
    )
    
    with pytest.raises(IntegrityError):
        await service.process_payment(req)


