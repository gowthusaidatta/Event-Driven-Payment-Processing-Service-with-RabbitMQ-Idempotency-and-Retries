import asyncio
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from src.config import settings

_engine = None
_session_maker = None
_engine_loop = None

def get_engine():
    global _engine, _engine_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
        
    if _engine is None or _engine_loop != current_loop:
        _engine = create_async_engine(
            settings.get_database_url,
            echo=False,
            future=True,
            pool_pre_ping=True,
        )
        _engine_loop = current_loop
    return _engine

def get_session_maker():
    global _session_maker, _engine_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
        
    if _session_maker is None or _engine_loop != current_loop:
        eng = get_engine()
        _session_maker = async_sessionmaker(
            bind=eng,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_maker

class EngineProxy:
    def __getattr__(self, name):
        return getattr(get_engine(), name)
        
    def begin(self):
        return get_engine().begin()
        
    async def dispose(self):
        eng = get_engine()
        await eng.dispose()

class AsyncSessionMakerProxy:
    def __call__(self, *args, **kwargs):
        return get_session_maker()(*args, **kwargs)

# Dynamic proxies to prevent connection pool "attached to a different loop" errors
engine = EngineProxy()
async_session_maker = AsyncSessionMakerProxy()

class Base(DeclarativeBase):
    pass

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
