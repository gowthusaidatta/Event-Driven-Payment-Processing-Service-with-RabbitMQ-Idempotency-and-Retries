from src.config import Settings

def test_settings_url_fallbacks(monkeypatch):
    # Temporarily clear env vars to test fallbacks without host process pollution
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    
    # Instantiate Settings with None URLs and no env file to test the fallback properties
    s = Settings(_env_file=None, database_url=None, rabbitmq_url=None)
    db_url = s.get_database_url
    assert "postgresql+asyncpg://" in db_url
    assert s.postgres_user in db_url
    assert str(s.postgres_port) in db_url
    
    rmq_url = s.get_rabbitmq_url
    assert "amqp://" in rmq_url
    assert s.rabbitmq_user in rmq_url
    assert str(s.rabbitmq_port) in rmq_url


def test_payment_initiate_request_invalid_currency():
    from pydantic import ValidationError
    import pytest
    from src.schemas.payment import PaymentInitiateRequest
    
    with pytest.raises(ValidationError):
        PaymentInitiateRequest(
            idempotency_key="key-test",
            amount=10.0,
            currency="123",
            user_id="user-123"
        )
