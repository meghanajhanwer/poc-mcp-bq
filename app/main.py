import asyncio
import logging
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Security
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse

from .auth import get_principal_dependency
from .bq_service import BigQueryService
from .config import Settings
from .logging_utils import configure_logging
from .mcp_protocol import handle_mcp_request
from .models import ExecuteArgs, JsonRpcRequest
from .policy import load_policy

settings = Settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.app_name,
    version="1.0.1",
    docs_url="/docs",
    redoc_url="/redoc",
)

principal_dependency = get_principal_dependency(settings)
bq_service = BigQueryService(settings)
policy_engine = load_policy(settings)


def _health_payload() -> Dict[str, str]:
    return {"status": "ok", "service": settings.app_name, "version": app.version}


@app.api_route("/healthz", methods=["GET", "POST"])
@app.api_route("/healthz/", methods=["GET", "POST"], include_in_schema=False)
@app.api_route("/v1/healthz", methods=["GET", "POST"])
@app.api_route("/v1/healthz/", methods=["GET", "POST"], include_in_schema=False)
async def healthz() -> Dict[str, str]:
    return _health_payload()


@app.get("/readyz")
async def readyz() -> JSONResponse:
    try:
        def _probe():
            job = bq_service.client.query("SELECT 1 AS ok", location=bq_service.location)
            _ = list(job.result())

        await run_in_threadpool(_probe)
        return JSONResponse(status_code=200, content={"status": "ready"})
    except Exception as e:
        logger.exception("Readiness check failed")
        return JSONResponse(status_code=503, content={"status": "not_ready", "detail": str(e)})


@app.post("/v1/execute")
async def execute_rest(
    args: ExecuteArgs,
    principal: str = Security(principal_dependency),
) -> Dict[str, Any]:
    policy_engine.assert_allowed(
        principal=principal,
        operation=args.operation.value,
        dataset=args.dataset,
        table=args.table,
    )
    result = await run_in_threadpool(bq_service.execute, args)
    return {"principal": principal, "result": result}


@app.post("/mcp")
async def mcp_endpoint(
    req: JsonRpcRequest,
    principal: str = Security(principal_dependency),
) -> JSONResponse:
    async def execute_callable(args: ExecuteArgs):
        policy_engine.assert_allowed(
            principal=principal,
            operation=args.operation.value,
            dataset=args.dataset,
            table=args.table,
        )
        return await run_in_threadpool(bq_service.execute, args)

    response = await handle_mcp_request(req, execute_callable=execute_callable)
    return JSONResponse(response)


@app.get("/mcp")
async def mcp_sse_keepalive() -> StreamingResponse:
    async def gen():
        yield ": connected\n\n"
        while True:
            await asyncio.sleep(15)
            yield ": keepalive\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.exception_handler(PermissionError)
async def permission_error_handler(_, exc: PermissionError):
    return JSONResponse(status_code=403, content={"detail": str(exc)})


@app.exception_handler(ValueError)
async def value_error_handler(_, exc: ValueError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(HTTPException)
async def http_error_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
