import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import Response
from src.api.health import health_check

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
@patch("src.api.health.RabbitMQClient")
async def test_health_check_postgres_down(mock_rabbitmq_client_class, mock_session_maker):
    # postgres fails, rabbitmq succeeds
    mock_session_maker.return_value.__aenter__.side_effect = Exception("DB Down")
    
    mock_client = mock_rabbitmq_client_class.return_value
    mock_client.connect = AsyncMock()
    mock_client.connection = MagicMock()
    mock_client.connection.is_closed = False
    mock_client.close = AsyncMock()
    
    response = Response()
    res = await health_check(response)
    
    assert response.status_code == 503
    assert res["status"] == "unhealthy"
    assert res["postgres"] == "down"
    assert res["rabbitmq"] == "up"

@pytest.mark.asyncio
@patch("src.database.async_session_maker")
@patch("src.api.health.RabbitMQClient")
async def test_health_check_rabbitmq_down(mock_rabbitmq_client_class, mock_session_maker):
    # postgres succeeds, rabbitmq fails
    mock_session = AsyncMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    mock_session.execute = AsyncMock()
    
    mock_client = mock_rabbitmq_client_class.return_value
    mock_client.connect = AsyncMock(side_effect=Exception("RMQ Down"))
    
    response = Response()
    res = await health_check(response)
    
    assert response.status_code == 503
    assert res["status"] == "unhealthy"
    assert res["postgres"] == "up"
    assert res["rabbitmq"] == "down"
