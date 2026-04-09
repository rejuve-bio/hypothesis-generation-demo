import os
import socketio

redis_url = os.getenv("REDIS_URL")
_sio_kwargs = dict(
    async_mode="asgi",
    cors_allowed_origins="*",
    engineio_logger=False,
    logger=False,
    ping_interval=25,
    ping_timeout=20,
    max_http_buffer_size=1_000_000,
    always_connect=False,
    cookie=None,
    http_compression=False,
    transports=['websocket']
)

if redis_url:
    mgr = socketio.AsyncRedisManager(redis_url)
    sio = socketio.AsyncServer(client_manager=mgr, **_sio_kwargs)
else:
    sio = socketio.AsyncServer(**_sio_kwargs)
