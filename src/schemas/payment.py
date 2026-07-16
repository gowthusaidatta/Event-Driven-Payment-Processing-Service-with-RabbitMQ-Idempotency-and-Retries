from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional

class PaymentMetadata(BaseModel):
    simulate_transient: bool = False
    simulate_permanent: bool = False

class PaymentInitiateRequest(BaseModel):
    idempotency_key: str = Field(..., min_length=1, max_length=255, description="Unique key to identify this transaction idempotently")
    amount: Decimal = Field(..., gt=0, description="Amount to charge, must be positive")
    currency: str = Field(..., min_length=3, max_length=3, description="3-letter currency code (e.g. USD, EUR)")
    user_id: str = Field(..., min_length=1, max_length=255, description="ID of the user making the payment")
    metadata: PaymentMetadata = Field(default_factory=PaymentMetadata)

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        upper_v = v.upper()
        if not upper_v.isalpha():
            raise ValueError("Currency must only contain alphabetic characters")
        return upper_v

class PaymentTransactionResponse(BaseModel):
    id: str
    idempotency_key: str
    amount: Decimal
    currency: str
    user_id: str
    status: str
    retry_count: int
    last_error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
