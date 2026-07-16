import math
import structlog
from src.logging import logger

class RetryService:
    def __init__(self, max_retries: int = 4):
        self.max_retries = max_retries
        self.logger = logger.bind(service="RetryService")

    def calculate_backoff(self, retry_count: int) -> float:
        """Calculates backoff delay in seconds.
        For retry_count = 1: 1s
        For retry_count = 2: 2s
        For retry_count = 3: 4s
        For retry_count = 4: 8s
        Formula: 2 ** (retry_count - 1)
        """
        if retry_count <= 0:
            return 1.0
        # Cap exponential backoff calculation to prevent potential overflow
        capped_count = min(retry_count, 10)
        delay = float(2 ** (capped_count - 1))
        self.logger.info("Calculated retry backoff", retry_count=retry_count, delay_seconds=delay)
        return delay

    def is_retryable(self, retry_count: int) -> bool:
        """Checks if another retry can be attempted based on the max_retries limit."""
        return retry_count < self.max_retries
