import uuid
import pytest
from unittest.mock import AsyncMock
from src.repositories.payment_repository import PaymentRepository
from src.models.payment import PaymentTransaction

@pytest.mark.asyncio
async def test_payment_repository_get_by_id():
    mock_session = AsyncMock()
    repo = PaymentRepository(mock_session)
    tx_id = uuid.uuid4()
    
    expected_tx = PaymentTransaction(id=tx_id)
    mock_session.get.return_value = expected_tx
    
    res = await repo.get_by_id(tx_id)
    assert res == expected_tx
    mock_session.get.assert_awaited_once_with(PaymentTransaction, tx_id)
