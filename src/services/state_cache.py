import json
from typing import List, Dict, Any
from datetime import datetime, timezone

import redis


class RedisStatusCache:
    """
    Redis-backed cache for hypothesis status updates.
    """

    def __init__(self, redis_url: str) -> None:
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
    def _history_key(self, hyp_id: str) -> str:
        return f"hyp:{hyp_id}:history"

    def _latest_key(self, hyp_id: str) -> str:
        return f"hyp:{hyp_id}:latest"

    def add_update(self, hyp_id: str, update: Dict[str, Any]) -> None:
        """
        Append *update* to the hypothesis history sorted set, keyed by epoch-ms.
        """
        ts_iso = update.get("timestamp")
        if not ts_iso:
            ts_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z"
            update = {**update, "timestamp": ts_iso}

        try:
            score = int(
                datetime.fromisoformat(ts_iso.rstrip("Z")).timestamp() * 1000
            )
        except Exception:
            score = int(datetime.now(timezone.utc).timestamp() * 1000)

        payload = json.dumps(update, separators=(",", ":"), ensure_ascii=False)

        pipe = self.r.pipeline()
        pipe.srem("hyp:persisted:set", hyp_id)
        pipe.zadd(self._history_key(hyp_id), {payload: score})
        pipe.sadd("hyp:inprogress:set", hyp_id)
        pipe.set(self._latest_key(hyp_id), payload)
        pipe.execute()

    def get_history(self, hyp_id: str) -> List[Dict[str, Any]]:
        """Return all updates for *hyp_id* ordered by timestamp ascending."""
        raw = self.r.zrange(self._history_key(hyp_id), 0, -1)
        return [json.loads(x) for x in raw] if raw else []

    def get_latest(self, hyp_id: str) -> Dict[str, Any] | None:
        """Return the most recent update for *hyp_id*, or *None* if unknown."""
        raw = self.r.get(self._latest_key(hyp_id))
        return json.loads(raw) if raw else None
        
    def clear_history(self, hyp_id: str) -> None:
        """
        Remove all cached state for *hyp_id* and promote it to the persisted set.
        """
        pipe = self.r.pipeline()
        pipe.delete(self._history_key(hyp_id))
        pipe.delete(self._latest_key(hyp_id))
        pipe.srem("hyp:inprogress:set", hyp_id)
        pipe.sadd("hyp:persisted:set", hyp_id)
        pipe.execute()

    def is_persisted(self, hyp_id: str) -> bool:
        """Return *True* if *hyp_id* has already been flushed to the database."""
        return bool(self.r.sismember("hyp:persisted:set", hyp_id))

    def list_inprogress(self) -> List[str]:
        """Return all hypothesis IDs currently being processed."""
        members = self.r.smembers("hyp:inprogress:set") or set()
        return list(members)

    def ping(self) -> None:
        """Verify connectivity to Redis (raises on failure)."""
        self.r.ping()
