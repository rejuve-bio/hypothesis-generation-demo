"""Socket.IO / WebSocket API documentation (embedded in OpenAPI description)."""


def build_websocket_api_docs(ws_origin: str | None = None) -> str:
    """Return a concise real-time API summary (not part of OpenAPI paths)."""
    if ws_origin:
        connect_target = f'"{ws_origin}"'
    else:
        connect_target = "window.location.origin"

    return f"""\
## WebSocket / Real-Time API

Transport is **Socket.IO**, mounted at the root path (`/`) on the same host and port as REST, and is **websocket only** (no polling fallback).

Authenticate on connect with a JWT via `auth: {{ token }}` in the Socket.IO client:

```javascript
import {{ io }} from "socket.io-client";

const socket = io({connect_target}, {{
  transports: ["websocket"],
  auth: {{ token: "<JWT>" }},
}});
```

- Fallback: `Authorization: Bearer <JWT>` header.
- Fallback: `?token=<JWT>` query string.

**Events:** Client emits `subscribe_hypothesis` and `subscribe_analysis`; server emits `task_update` and `analysis_update`.
"""
