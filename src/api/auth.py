from __future__ import annotations

import logging
import os

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

JWT_SECRET = os.getenv("JWT_SECRET")

_bearer = HTTPBearer()


def _decode(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])


async def get_current_user_id(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> str:
    """FastAPI dependency: validate a Bearer JWT and return the user_id claim."""
    try:
        data = _decode(creds.credentials)
        return str(data["user_id"])
    except Exception as exc:
        logging.error(f"Token decode error: {exc}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token is invalid!",
        )


async def verify_service_token(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> None:
    """FastAPI dependency for internal endpoints: only Prefect service tokens pass."""
    try:
        data = _decode(creds.credentials)
        if data.get("service") != "prefect":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Service token required",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logging.error(f"Service token error: {exc}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid service token",
        )


def decode_ws_token(token: str) -> str | None:
    """Parse and validate a raw JWT string (used for WebSocket query-param auth).

    Returns the user_id string, or *None* if the token is missing / invalid.
    """
    if not token:
        return None
    try:
        data = _decode(token)
        return str(data["user_id"])
    except Exception:
        return None
