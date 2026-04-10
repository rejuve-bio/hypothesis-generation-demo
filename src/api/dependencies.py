from __future__ import annotations

import os
from typing import Any

_deps: dict[str, Any] = {}
_JWT_SECRET = os.getenv("JWT_SECRET")


def init_deps(deps: dict[str, Any]) -> None:
    """Called once at application startup to register shared dependencies."""
    global _deps
    _deps.update(deps)
