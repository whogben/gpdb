import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def db_factory(pg_server):
    """Factory function to create GPGraph instances with optional table_prefix."""
    from gpdb import GPGraph
    from sqlalchemy import text

    async def _create_db(table_prefix: str = None):
        url = pg_server.get_uri()
        db = GPGraph(url, table_prefix=table_prefix)

        if table_prefix:
            # Manually drop tables for prefixed databases
            async with db.sqla_engine.begin() as conn:
                await conn.execute(
                    text(f"DROP TABLE IF EXISTS {table_prefix}_schemas CASCADE")
                )
                await conn.execute(
                    text(f"DROP TABLE IF EXISTS {table_prefix}_edges CASCADE")
                )
                await conn.execute(
                    text(f"DROP TABLE IF EXISTS {table_prefix}_nodes CASCADE")
                )

            # Clear model cache and metadata
            from gpdb.models.base import _Base
            from gpdb.models.factories import _model_cache

            _model_cache.clear()
            _Base.metadata.clear()

        await db.create_tables()
        return db

    return _create_db


@pytest_asyncio.fixture
async def db(db_factory):
    """Default db fixture without table_prefix."""
    db = await db_factory(table_prefix=None)
    yield db
    await db.drop_tables()
    await db.sqla_engine.dispose()


@pytest_asyncio.fixture
async def db_with_prefix(db_factory):
    """Db fixture with table_prefix for schema tests."""
    db = await db_factory(table_prefix="test_schema")
    yield db
    await db.drop_tables()
    await db.sqla_engine.dispose()
