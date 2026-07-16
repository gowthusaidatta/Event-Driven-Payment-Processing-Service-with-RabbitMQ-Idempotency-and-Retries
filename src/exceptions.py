class PaymentException(Exception):
    """Base exception for payment processor service."""
    pass

class PaymentTransientFailureException(PaymentException):
    """Exception raised when a transient error occurs during payment processing (e.g. timeout, temporary network error)."""
    pass

class PaymentPermanentFailureException(PaymentException):
    """Exception raised when a permanent error occurs during payment processing (e.g. card declined, insufficient funds, invalid account)."""
    pass

class PaymentIdempotencyConflictException(PaymentException):
    """Exception raised when a concurrent or conflicting request with the same idempotency key is detected."""
    pass
