from src.services.retry_service import RetryService

def test_calculate_backoff():
    service = RetryService(max_retries=4)
    assert service.calculate_backoff(0) == 1.0
    assert service.calculate_backoff(1) == 1.0
    assert service.calculate_backoff(2) == 2.0
    assert service.calculate_backoff(3) == 4.0
    assert service.calculate_backoff(4) == 8.0
    assert service.calculate_backoff(5) == 16.0

def test_is_retryable():
    service = RetryService(max_retries=4)
    assert service.is_retryable(0) is True
    assert service.is_retryable(3) is True
    assert service.is_retryable(4) is False
    assert service.is_retryable(5) is False
