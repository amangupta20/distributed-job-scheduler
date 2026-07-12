import pytest
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from scheduler_api.config import settings
from scheduler_api.models import Base


@pytest.fixture
async def db_engine():
    db_url = settings.database_url
    if "postgres:5432" in db_url:
        db_url = db_url.replace("postgres:5432", "127.0.0.1:5432")

    engine = create_async_engine(db_url, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.fixture
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    # Configure the global async_session in db.py to bind to db_engine
    # so that scheduler code using the global session factory can connect to the test db!
    from scheduler_api import db
    db.AsyncSessionLocal = async_sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with db.AsyncSessionLocal() as session:
        yield session
        await session.rollback()
