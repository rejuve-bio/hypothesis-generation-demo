from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from loguru import logger


class TaskState(Enum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

class StatusTracker:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.task_history = {}  # hypothesis_id -> list of task updates
            cls._instance.completed_hypotheses = set()
        return cls._instance
    
    @classmethod
    def initialize(cls, task_handler, redis_url: Optional[str] = None) -> None:
        cls._task_handler = task_handler

        if redis_url:
            try:
                from src.services.state_cache import RedisStatusCache
                cls._cache = RedisStatusCache(redis_url)
                cls._cache.ping()
                logger.info("StatusTracker: Redis cache enabled ({})", redis_url)
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "StatusTracker: could not connect to Redis – running without cache. Error: {}",
                    exc,
                )
                cls._cache = None
        else:
            cls._cache = None

    @staticmethod
    def _dedup_and_sort(updates):
        seen = {}
        for upd in updates:
            key = (upd.get("task"), upd.get("timestamp"))
            seen[key] = upd
        return sorted(seen.values(), key=lambda x: x.get("timestamp") or "")

    @classmethod
    def _cache_instance(cls):
        """Return the RedisStatusCache or None (never raises)."""
        return getattr(cls, "_cache", None)

    def add_update(
        self,
        hypothesis_id: str,
        progress: float,
        task_name: str,
        state: TaskState,
        details=None,
        error=None,
    ) -> None:
        if not hypothesis_id:
            raise ValueError("Hypothesis ID is required")
        if not isinstance(state, TaskState):
            raise ValueError("Invalid task state provided")

        if hypothesis_id not in self.task_history:
            self.task_history[hypothesis_id] = []
            
        update = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
            "task": task_name,
            "state": state.value,
            "progress": progress,
        }
        
        if details:
            update["details"] = details
        if error:
            update["error"] = error
            
        self.task_history[hypothesis_id].append(update)

        cache = getattr(self.__class__, "_cache", None)
        if cache is not None:
            try:
                cache.add_update(hypothesis_id, update)
            except Exception as exc:
                logger.warning(
                    "StatusTracker: Redis write failed for {} – {}", hypothesis_id, exc
                )

        # Persist to DB on completion or failure at specific milestones
        if state in [TaskState.COMPLETED, TaskState.FAILED]:
            if task_name in [
                "Creating enrich data",
                "Generating hypothesis",
            ] or (
                task_name.startswith("Verifying existence") and progress == 80
            ):
                self._persist_and_clear(hypothesis_id)

    def _persist_and_clear(self, hypothesis_id: str) -> None:
        """Persist task history to DB and clear from in-memory + Redis cache."""
        if hypothesis_id not in self.task_history:
            return

        if not hasattr(self.__class__, "_task_handler"):
            raise RuntimeError("StatusTracker not initialized")

        db_history = self.__class__._task_handler.get_task_history(hypothesis_id) or []
        new_history = self.task_history[hypothesis_id]

        redis_history: list = []
        cache = getattr(self.__class__, "_cache", None)
        if cache is not None:
            try:
                redis_history = cache.get_history(hypothesis_id) or []
            except Exception as exc:
                logger.warning(
                    "StatusTracker: Redis read failed during persist for {} – {}",
                    hypothesis_id,
                    exc,
                )

        combined = db_history + redis_history + new_history

        final_history = self._dedup_and_sort(combined)

        self.__class__._task_handler.save_task_history(hypothesis_id, final_history)

        # Clear in-memory state
        del self.task_history[hypothesis_id]
        self.completed_hypotheses.add(hypothesis_id)

        # Clear Redis cache entry now that it is safely in the DB
        cache = getattr(self.__class__, "_cache", None)
        if cache is not None:
            try:
                cache.clear_history(hypothesis_id)
            except Exception as exc:
                logger.warning(
                    "StatusTracker: Redis clear failed for {} – {}", hypothesis_id, exc
                )

    def get_history(self, hypothesis_id):
        memory_history = list(self.task_history.get(hypothesis_id, []))

        cache = self._cache_instance()
        redis_history = []
        is_persisted = False

        if cache is not None:
            try:
                is_persisted = cache.is_persisted(hypothesis_id)
                if not is_persisted:
                    redis_history = cache.get_history(hypothesis_id) or []
            except Exception as exc:
                logger.warning(
                    "StatusTracker: Redis read failed for {} – {}",
                    hypothesis_id,
                    exc,
                )
                is_persisted = True  # fall through to DB on Redis failure

        db_history = []
        if cache is None or is_persisted:
            db_history = (
                self.__class__._task_handler.get_task_history(hypothesis_id) or []
            )

        combined = memory_history + redis_history + db_history
        if not combined:
            return []
            
        return self._dedup_and_sort(combined)

    def get_latest_state(self, hypothesis_id: str):

        candidates = []
        history = self.task_history.get(hypothesis_id, [])
        if history:
            candidates.append(history[-1])

        cache = getattr(self.__class__, "_cache", None)
        if cache is not None:
            try:
                latest = cache.get_latest(hypothesis_id)
                if latest:
                    candidates.append(latest)
            except Exception as exc:
                logger.warning(
                    "StatusTracker: Redis latest-read failed for {} – {}", hypothesis_id, exc
                )

        db_history = []
        try:
            db_history = (
                self.__class__._task_handler.get_task_history(hypothesis_id) or []
            )
            if db_history:
                candidates.append(db_history[-1])
        except Exception as exc:
            logger.warning(
                "StatusTracker: DB latest-read failed for {} – {}",
                hypothesis_id,
                exc,
            )
        if not candidates:
            return None

        return max(candidates, key=lambda x: x.get("timestamp") or "")
    def calculate_progress(self, task_history: list) -> float:
        if not task_history:
            return 0.0

        enrichment_tasks = {
            "Verifying existence of enrichment data": 10,
            "Getting candidate genes": 10,
            "Predicting causal gene": 20,
            "Getting relevant gene proof": 20,
            "Creating enrich data": 20,
        }

        hypothesis_tasks = {
            "Verifying existence of hypothesis data": 2,
            "Getting enrichement data": 2,
            "Getting gene data": 2,
            "Querying gene data": 3,
            "Querying variant data": 3,
            "Querying phenotype data": 3,
            "Generating graph summary": 3,
            "Generating hypothesis": 2,
        }

        filtered = [
            t for t in task_history if t.get("state") == TaskState.COMPLETED.value
        ]

        enrichment_progress = 0
        hypothesis_progress = 0

        for task in filtered:
            name = task["task"]
            if name in enrichment_tasks:
                enrichment_progress += enrichment_tasks[name]
            elif name in hypothesis_tasks:
                hypothesis_progress += hypothesis_tasks[name]

        total_enrichment_weight = sum(enrichment_tasks.values())   # 80
        total_hypothesis_weight = sum(hypothesis_tasks.values())   # 20

        enrichment_pct = (enrichment_progress / total_enrichment_weight) * 80
        hypothesis_pct = (hypothesis_progress / total_hypothesis_weight) * 20

        return round(min(enrichment_pct + hypothesis_pct, 100), 2)


# Global singleton
status_tracker = StatusTracker()