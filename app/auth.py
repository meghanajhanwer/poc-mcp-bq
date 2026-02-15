from typing import Optional

from fastapi import Header, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.auth.transport import requests as grequests
from google.oauth2 import id_token

from .config import Settings

bearer_scheme = HTTPBearer(auto_error=False)


def get_principal_dependency(settings: Settings):
    async def _get_principal(
        credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
        x_principal: Optional[str] = Header(default=None),
    ) -> str:
        if settings.auth_mode == "none":
            return "anonymous"

        if settings.auth_mode == "header":
            if not x_principal:
                raise HTTPException(status_code=401, detail="Missing X-Principal header")
            return x_principal.strip().lower()

        # id_token mode
        if not credentials or credentials.scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Missing Bearer token")

        token = (credentials.credentials or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="Missing Bearer token")

        try:
            info = id_token.verify_oauth2_token(
                token,
                grequests.Request(),
                settings.mcp_audience or None,
            )
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"Invalid ID token: {e}") from e

        principal = (info.get("email") or info.get("sub") or "").strip().lower()
        if not principal:
            raise HTTPException(status_code=401, detail="No principal in token")

        return principal

    return _get_principal
