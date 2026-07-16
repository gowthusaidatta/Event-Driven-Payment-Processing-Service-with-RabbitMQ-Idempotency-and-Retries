import pytest
from unittest.mock import AsyncMock
from src.dependencies import get_payment_repository
from src.repositories.payment_repository import PaymentRepository

@pytest.mark.asyncio
async def test_get_payment_repository():
    mock_session = AsyncMock()
    repo = await get_payment_repository(mock_session)
    assert isinstance(repo, PaymentRepository)
    assert repo.session == mock_session
