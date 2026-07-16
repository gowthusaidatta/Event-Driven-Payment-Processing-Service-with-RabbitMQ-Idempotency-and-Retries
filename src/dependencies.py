from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_db
from src.repositories.payment_repository import PaymentRepository

async def get_payment_repository(session: AsyncSession = Depends(get_db)) -> PaymentRepository:
    return PaymentRepository(session)
