from typing import Any, Awaitable, Callable, Dict

from pydantic import ValidationError

from .models import ExecuteArgs, JsonRpcRequest


def _rpc_ok(req_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _rpc_err(req_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


async def handle_mcp_request(
    req: JsonRpcRequest,
    execute_callable: Callable[[ExecuteArgs], Awaitable[Dict[str, Any]]],
) -> Dict[str, Any]:
    method = req.method
    params = req.params or {}

    if method == "tools/list":
        return _rpc_ok(
            req.id,
            {
                "tools": [
                    {
                        "name": "bigquery.execute",
                        "description": "Run controlled BigQuery operation",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "operation": {"type": "string", "enum": ["SELECT", "CREATE_TABLE", "INSERT", "UPDATE", "DELETE"]},
                                "dataset": {"type": "string"},
                                "table": {"type": "string"},
                                "columns": {"type": "array", "items": {"type": "string"}},
                                "filters": {"type": "object"},
                                "limit": {"type": "integer"},
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "type": {"type": "string"},
                                            "mode": {"type": "string"}
                                        },
                                        "required": ["name", "type"]
                                    }
                                },
                                "if_not_exists": {"type": "boolean"},
                                "rows": {"type": "array", "items": {"type": "object"}},
                                "set_values": {"type": "object"}
                            },
                            "required": ["operation", "dataset", "table"]
                        },
                    }
                ]
            },
        )

    if method == "tools/call":
        name = params.get("name")
        if name != "bigquery.execute":
            return _rpc_err(req.id, -32601, f"Unknown tool: {name}")

        raw_args = params.get("arguments", {})
        try:
            parsed = ExecuteArgs.model_validate(raw_args)
        except ValidationError as e:
            return _rpc_err(req.id, -32602, f"Invalid params: {e}")

        try:
            result = await execute_callable(parsed)
            return _rpc_ok(req.id, result)
        except Exception as e:
            return _rpc_err(req.id, -32000, str(e))

    return _rpc_err(req.id, -32601, f"Unknown method: {method}")
