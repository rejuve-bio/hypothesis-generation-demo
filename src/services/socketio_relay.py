"""Redis pub/sub bridge: workers publish; API subscribes and emits to Socket.IO rooms."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import redis as redis_sync
import redis.asyncio as aioredis
from loguru import logger

# Single channel for all Socket.IO relay messages (envelope distinguishes event/room).
SOCKETIO_RELAY_CHANNEL = "socketio:relay:v1"
DEDUPE_KEY_PREFIX = "socketio:relay:dedupe"
DEDUPE_TTL_SECONDS = 8
RECONNECT_DELAY_SECONDS = 3

# Process-wide pool for worker-side publishes (Dask/Prefect); avoids new TCP per update.
_sync_pool: redis_sync.Redis | None = None
_sync_pool_url: str | None = None


def _get_sync_redis(redis_url: str) -> redis_sync.Redis:
    global _sync_pool, _sync_pool_url
    if _sync_pool is not None and _sync_pool_url == redis_url:
        return _sync_pool
    if _sync_pool is not None:
        try:
            _sync_pool.close()
        except Exception:
            pass
        _sync_pool = None
        _sync_pool_url = None
    _sync_pool = redis_sync.Redis.from_url(
        redis_url,
        decode_responses=True,
        max_connections=5,
    )
    _sync_pool_url = redis_url
    return _sync_pool


def publish_socketio_relay(
    redis_url: str, event: str, target_room: str, data: dict[str, Any]
) -> None:
    """Sync publish for Dask/Prefect worker processes (called from ``utils.emit_*``).

    Each call gets a fresh ``_relay_id`` (UUID4). Identical task content still yields
    distinct IDs so duplicate suppression never merges separate real updates.
    """
    envelope = {
        "_relay_id": str(uuid.uuid4()),
        "event": event,
        "target_room": target_room,
        "data": data,
    }
    raw = json.dumps(envelope, separators=(",", ":"), default=str)
    r = _get_sync_redis(redis_url)
    r.publish(SOCKETIO_RELAY_CHANNEL, raw)


async def _close_async_relay_clients(
    pubsub: Any,
    r_sub: aioredis.Redis | None,
    r_cmd: aioredis.Redis | None,
) -> None:
    if pubsub is not None:
        try:
            await pubsub.unsubscribe(SOCKETIO_RELAY_CHANNEL)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass
    if r_sub is not None:
        try:
            await r_sub.close()
        except Exception:
            pass
    if r_cmd is not None:
        try:
            await r_cmd.close()
        except Exception:
            pass


async def relay_subscribe_forever(redis_url: str, sio: Any) -> None:
    """
    Subscribe to ``SOCKETIO_RELAY_CHANNEL`` and emit to Socket.IO rooms.

    Uses Redis SET NX per message so only one
    gunicorn worker emits when multiple API workers receive the same pub/sub message.

    Reconnects on connection loss (Redis restart, network) until the task is cancelled.
    """
    while True:
        r_sub: aioredis.Redis | None = None
        r_cmd: aioredis.Redis | None = None
        pubsub = None
        try:
            r_sub = aioredis.from_url(redis_url, decode_responses=True)
            r_cmd = aioredis.from_url(redis_url, decode_responses=True)
            pubsub = r_sub.pubsub()
            await pubsub.subscribe(SOCKETIO_RELAY_CHANNEL)
            logger.info("socketio_relay: subscribed, listening on {}", SOCKETIO_RELAY_CHANNEL)

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    envelope = json.loads(message["data"])
                    relay_id = envelope.get("_relay_id")
                    event = envelope.get("event")
                    room = envelope.get("target_room")
                    data = envelope.get("data")
                    if not relay_id or not event or not room:
                        logger.warning(
                            "socketio_relay: invalid envelope (missing fields): {}",
                            str(message.get("data"))[:300],
                        )
                        continue
                    ok = await r_cmd.set(
                        f"{DEDUPE_KEY_PREFIX}:{relay_id}",
                        "1",
                        ex=DEDUPE_TTL_SECONDS,
                        nx=True,
                    )
                    if not ok:
                        continue
                    await sio.emit(event, data, room=room)
                    logger.debug("socketio_relay: emitted {} → room {}", event, room)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception("socketio_relay: handler error: {}", exc)
        except asyncio.CancelledError:
            await _close_async_relay_clients(pubsub, r_sub, r_cmd)
            raise
        except Exception as exc:
            logger.error(
                "socketio_relay: connection lost: {}. Reconnecting in {}s...",
                exc,
                RECONNECT_DELAY_SECONDS,
            )
            await _close_async_relay_clients(pubsub, r_sub, r_cmd)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        else:
            # listen() ended without exception (should not happen); reconnect
            await _close_async_relay_clients(pubsub, r_sub, r_cmd)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
