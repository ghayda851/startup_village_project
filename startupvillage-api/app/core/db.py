# app/core/db.py
import ssl
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.core.config import settings

ssl_ctx = ssl.create_default_context()
# For Azure PostgreSQL, SSL is required; default context works.

engine = create_async_engine(
    settings.database_url,   # WITHOUT ?sslmode=require
    pool_pre_ping=True,
    connect_args={"ssl": ssl_ctx},
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_db():
    async with SessionLocal() as session:
        yield session