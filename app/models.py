from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class Operation(str, Enum):
    SELECT = "SELECT"
    CREATE_TABLE = "CREATE_TABLE"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class TableField(BaseModel):
    name: str
    type: str = Field(description="BigQuery type, e.g. STRING, INT64")
    mode: str = Field(default="NULLABLE", description="NULLABLE | REQUIRED | REPEATED")

    @field_validator("type")
    @classmethod
    def upper_type(cls, v: str) -> str:
        return v.upper()

    @field_validator("mode")
    @classmethod
    def upper_mode(cls, v: str) -> str:
        v = v.upper()
        if v not in {"NULLABLE", "REQUIRED", "REPEATED"}:
            raise ValueError("mode must be one of NULLABLE, REQUIRED, REPEATED")
        return v


class ExecuteArgs(BaseModel):
    operation: Operation
    dataset: str
    table: str

    columns: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None
    limit: int = 100

    schema: Optional[List[TableField]] = None
    if_not_exists: bool = True
    rows: Optional[List[Dict[str, Any]]] = None
    set_values: Optional[Dict[str, Any]] = None


class JsonRpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Any = None
    method: str
    params: Optional[Dict[str, Any]] = None
