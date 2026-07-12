import uuid
from typing import Dict
from fastapi import Depends, FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from scheduler_api.db import get_session
from scheduler_api.errors import install_error_handlers


class TraceIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        trace_id = request.headers.get("x-trace-id") or str(uuid.uuid4())
        request.state.trace_id = trace_id
        response = await call_next(request)
        response.headers["x-trace-id"] = trace_id
        return response


def create_app() -> FastAPI:
    app = FastAPI(title="PulseQueue API", version="0.1.0")
    
    app.add_middleware(TraceIdMiddleware)
    install_error_handlers(app)

    # Register routers
    from scheduler_api.routes.auth import router as auth_router
    from scheduler_api.routes.projects import router as projects_router
    from scheduler_api.routes.retry_policies import router as retry_policies_router
    from scheduler_api.routes.queues import router as queues_router

    app.include_router(auth_router, prefix="/api/v1")
    app.include_router(projects_router, prefix="/api/v1")
    app.include_router(retry_policies_router, prefix="/api/v1")
    app.include_router(queues_router, prefix="/api/v1")


    @app.get("/health/live")
    async def live() -> Dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready(session: AsyncSession = Depends(get_session)) -> Dict[str, str]:
        await session.execute(text("SELECT 1"))
        return {"status": "ready"}

    return app
