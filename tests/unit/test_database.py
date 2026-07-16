import pytest
import asyncio
from src.database import get_engine, get_session_maker, get_db

def test_database_no_loop():
    # Calling outside of an active event loop triggers the RuntimeError -> current_loop = None block
    eng = get_engine()
    assert eng is not None
    sm = get_session_maker()
    assert sm is not None

@pytest.mark.asyncio
async def test_get_db_rollback():
    # Test that get_db rolls back when the client block raises an exception
    db_gen = get_db()
    session = await anext(db_gen)
    assert session is not None
    
    with pytest.raises(ValueError):
        try:
            raise ValueError("Simulated DB transaction block failure")
        except Exception as e:
            await db_gen.athrow(e)
