from prometheus_client import Counter

# Metrics definitions
MESSAGES_CONSUMED = Counter(
    "payment_processor_messages_consumed_total",
    "Total number of payment initiation messages consumed"
)

PAYMENTS_SUCCESSFUL = Counter(
    "payment_processor_payments_successful_total",
    "Total number of successfully processed payments"
)

PAYMENTS_FAILED = Counter(
    "payment_processor_payments_failed_total",
    "Total number of permanently failed payments"
)

RETRIES = Counter(
    "payment_processor_retries_total",
    "Total number of retries attempted"
)
