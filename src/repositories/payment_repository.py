import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.models.payment import PaymentTransaction

class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_idempotency_key(self, idempotency_key: str) -> PaymentTransaction | None:
        stmt = select(PaymentTransaction).where(PaymentTransaction.idempotency_key == idempotency_key)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, transaction_id: uuid.UUID) -> PaymentTransaction | None:
        return await self.session.get(PaymentTransaction, transaction_id)

    async def create(self, payment: PaymentTransaction) -> PaymentTransaction:
        self.session.add(payment)
        await self.session.flush()  # Flushes to DB to obtain default fields/UUID
        return payment

    async def update(self, payment: PaymentTransaction) -> PaymentTransaction:
        await self.session.flush()
        return payment
