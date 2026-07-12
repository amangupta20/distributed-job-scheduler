import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict
from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import text

from scheduler_api.config import settings
from scheduler_service.tick import run_tick

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level":"%(levelname)s", "logger":"%(name)s", "message":"%(message)s"}',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("scheduler_service.main")

async def tick_loop(shutdown_event: asyncio.Event):
    logger.info("Starting scheduler background loop")
    from scheduler_api.db import AsyncSessionLocal

    while not shutdown_event.is_set():
        start_time = asyncio.get_event_loop().time()
        try:
            now = datetime.now(timezone.utc)
            res = await run_tick(AsyncSessionLocal, now, 100)
            if res.cron > 0 or res.promoted > 0 or res.recovered > 0:
                logger.info(
                    f"Scheduler tick: materialized_cron={res.cron}, promoted={res.promoted}, recovered_leases={res.recovered}"
                )
        except Exception as e:
            logger.error(f"Error in scheduler tick: {e}", exc_info=True)

        elapsed = asyncio.get_event_loop().time() - start_time
        sleep_time = max(0.1, 1.0 - elapsed)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
        except asyncio.TimeoutError:
            pass

    logger.info("Scheduler background loop stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create the shutdown event bound to the currently running event loop
    shutdown_event = asyncio.Event()
    # Start the tick loop in the background
    loop_task = asyncio.create_task(tick_loop(shutdown_event))
    yield
    # Trigger graceful stop
    logger.info("Shutting down scheduler service...")
    shutdown_event.set()
    await loop_task
    
    # Close database engine
    from scheduler_api.db import engine
    await engine.dispose()
    logger.info("Database engine closed successfully")


app = FastAPI(title="PulseQueue Scheduler Service", lifespan=lifespan)


@app.get("/health/live")
async def live() -> Dict[str, str]:
    return {"status": "alive"}


@app.get("/health/ready")
async def ready() -> Dict[str, str]:
    from scheduler_api.db import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        await session.execute(text("SELECT 1"))
    return {"status": "ready"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    # Start uvicorn server on port 8001
    uvicorn.run(app, host="0.0.0.0", port=8001)
