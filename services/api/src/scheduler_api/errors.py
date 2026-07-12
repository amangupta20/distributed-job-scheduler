import uuid
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class DomainError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400, details: Optional[Dict[str, Any]] = None):
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)


def get_trace_id(request: Request) -> str:
    return getattr(request.state, "trace_id", str(uuid.uuid4()))


def make_error_response(code: str, message: str, status_code: int, trace_id: str, details: Optional[Dict[str, Any]] = None) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": details,
                "trace_id": trace_id,
            }
        },
    )


async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    return make_error_response(
        code=exc.code,
        message=exc.message,
        status_code=exc.status_code,
        trace_id=get_trace_id(request),
        details=exc.details,
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    details = {"errors": exc.errors()}
    return make_error_response(
        code="validation_failed",
        message="Request validation failed",
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        trace_id=get_trace_id(request),
        details=details,
    )


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    code = "not_found" if exc.status_code == 404 else "http_error"
    if exc.status_code == 401:
        code = "unauthorized"
    elif exc.status_code == 403:
        code = "forbidden"
    
    return make_error_response(
        code=code,
        message=str(exc.detail),
        status_code=exc.status_code,
        trace_id=get_trace_id(request),
    )


async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return make_error_response(
        code="internal_server_error",
        message="An unexpected error occurred",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        trace_id=get_trace_id(request),
    )


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, domain_error_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(Exception, global_exception_handler)
