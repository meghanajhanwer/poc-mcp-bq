import datetime as dt
import decimal
import re
from typing import Any, Dict, List

from google.cloud import bigquery

from .config import Settings
from .models import ExecuteArgs, Operation, TableField


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
ALLOWED_TYPES = {
    "STRING", "BYTES", "INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC",
    "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "JSON"
}


def _ensure_ident(name: str, label: str) -> str:
    if not IDENT_RE.match(name):
        raise ValueError(f"Invalid {label}: {name}")
    return name


def _normalize_value(v: Any) -> Any:
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, list):
        return [_normalize_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _normalize_value(val) for k, val in v.items()}
    return v


def _param_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, decimal.Decimal):
        return "NUMERIC"
    if isinstance(value, dt.datetime):
        return "TIMESTAMP"
    if isinstance(value, dt.date):
        return "DATE"
    return "STRING"


class BigQueryService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = bigquery.Client(project=settings.project_id, location=settings.bigquery_location)
        self.location = settings.bigquery_location
        self.max_select_limit = settings.max_select_limit
        self.allow_full_table_delete = settings.allow_full_table_delete

    def _table_ref(self, dataset: str, table: str) -> str:
        ds = _ensure_ident(dataset, "dataset")
        tb = _ensure_ident(table, "table")
        return f"`{self.settings.project_id}.{ds}.{tb}`"

    def _col_ref(self, col: str) -> str:
        c = _ensure_ident(col, "column")
        return f"`{c}`"

    def execute(self, args: ExecuteArgs) -> Dict[str, Any]:
        op = args.operation
        if op == Operation.SELECT:
            return self._select(args)
        if op == Operation.CREATE_TABLE:
            return self._create_table(args)
        if op == Operation.INSERT:
            return self._insert(args)
        if op == Operation.UPDATE:
            return self._update(args)
        if op == Operation.DELETE:
            return self._delete(args)
        raise ValueError(f"Unsupported operation: {op}")

    def _select(self, args: ExecuteArgs) -> Dict[str, Any]:
        table_ref = self._table_ref(args.dataset, args.table)

        if args.columns:
            cols = ", ".join(self._col_ref(c) for c in args.columns)
        else:
            cols = "*"

        limit = min(max(int(args.limit or 100), 1), self.max_select_limit)

        sql = f"SELECT {cols} FROM {table_ref}"
        params: List[bigquery.ScalarQueryParameter] = []

        if args.filters:
            where_parts = []
            for i, (k, v) in enumerate(args.filters.items()):
                col = self._col_ref(k)
                pname = f"f{i}"
                where_parts.append(f"{col} = @{pname}")
                params.append(bigquery.ScalarQueryParameter(pname, _param_type(v), v))
            sql += " WHERE " + " AND ".join(where_parts)

        sql += " LIMIT @limit"
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

        job_config = bigquery.QueryJobConfig(query_parameters=params, use_legacy_sql=False)
        job = self.client.query(sql, location=self.location, job_config=job_config)
        rows = [dict(r.items()) for r in job.result()]
        rows = [_normalize_value(r) for r in rows]

        return {
            "operation": "SELECT",
            "row_count": len(rows),
            "rows": rows,
            "job_id": job.job_id,
        }

    def _create_table(self, args: ExecuteArgs) -> Dict[str, Any]:
        if not args.schema:
            raise ValueError("schema is required for CREATE_TABLE")

        _ensure_ident(args.dataset, "dataset")
        _ensure_ident(args.table, "table")

        schema_fields = []
        for f in args.schema:
            if f.type not in ALLOWED_TYPES:
                raise ValueError(f"Unsupported BigQuery type: {f.type}")
            _ensure_ident(f.name, "field name")
            schema_fields.append(bigquery.SchemaField(f.name, f.type, mode=f.mode))

        table_id = f"{self.settings.project_id}.{args.dataset}.{args.table}"
        table = bigquery.Table(table_id, schema=schema_fields)
        created = self.client.create_table(table, exists_ok=args.if_not_exists)

        return {
            "operation": "CREATE_TABLE",
            "table": created.full_table_id,
            "created": True,
        }

    def _insert(self, args: ExecuteArgs) -> Dict[str, Any]:
        if not args.rows:
            raise ValueError("rows is required for INSERT")

        _ensure_ident(args.dataset, "dataset")
        _ensure_ident(args.table, "table")

        table_id = f"{self.settings.project_id}.{args.dataset}.{args.table}"
        errors = self.client.insert_rows_json(table_id, args.rows)
        if errors:
            raise ValueError(f"BigQuery insert errors: {errors}")

        return {
            "operation": "INSERT",
            "inserted_rows": len(args.rows),
        }

    def _update(self, args: ExecuteArgs) -> Dict[str, Any]:
        if not args.set_values:
            raise ValueError("set_values is required for UPDATE")
        if not args.filters:
            raise ValueError("filters are required for UPDATE (safe default)")

        table_ref = self._table_ref(args.dataset, args.table)

        set_parts = []
        where_parts = []
        params: List[bigquery.ScalarQueryParameter] = []

        for i, (k, v) in enumerate(args.set_values.items()):
            col = self._col_ref(k)
            pname = f"s{i}"
            set_parts.append(f"{col} = @{pname}")
            params.append(bigquery.ScalarQueryParameter(pname, _param_type(v), v))

        for i, (k, v) in enumerate(args.filters.items()):
            col = self._col_ref(k)
            pname = f"w{i}"
            where_parts.append(f"{col} = @{pname}")
            params.append(bigquery.ScalarQueryParameter(pname, _param_type(v), v))

        sql = f"""
        UPDATE {table_ref}
        SET {", ".join(set_parts)}
        WHERE {" AND ".join(where_parts)}
        """

        job = self.client.query(
            sql,
            location=self.location,
            job_config=bigquery.QueryJobConfig(query_parameters=params, use_legacy_sql=False),
        )
        job.result()

        return {
            "operation": "UPDATE",
            "affected_rows": int(job.num_dml_affected_rows or 0),
            "job_id": job.job_id,
        }

    def _delete(self, args: ExecuteArgs) -> Dict[str, Any]:
        table_ref = self._table_ref(args.dataset, args.table)
        params: List[bigquery.ScalarQueryParameter] = []

        if args.filters:
            where_parts = []
            for i, (k, v) in enumerate(args.filters.items()):
                col = self._col_ref(k)
                pname = f"d{i}"
                where_parts.append(f"{col} = @{pname}")
                params.append(bigquery.ScalarQueryParameter(pname, _param_type(v), v))
            where_sql = " WHERE " + " AND ".join(where_parts)
        else:
            if not self.allow_full_table_delete:
                raise ValueError("DELETE without filters is blocked by policy")
            where_sql = ""

        sql = f"DELETE FROM {table_ref}{where_sql}"
        job = self.client.query(
            sql,
            location=self.location,
            job_config=bigquery.QueryJobConfig(query_parameters=params, use_legacy_sql=False),
        )
        job.result()

        return {
            "operation": "DELETE",
            "affected_rows": int(job.num_dml_affected_rows or 0),
            "job_id": job.job_id,
        }
