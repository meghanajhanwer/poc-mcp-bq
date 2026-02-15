from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "mcp-bigquery-server"
    log_level: str = "INFO"

    project_id: str
    bigquery_location: str = "EU"

    auth_mode: Literal["id_token", "header", "none"] = "id_token"
    mcp_audience: Optional[str] = None

    policy_json: str
    max_select_limit: int = 1000
    allow_full_table_delete: bool = False
