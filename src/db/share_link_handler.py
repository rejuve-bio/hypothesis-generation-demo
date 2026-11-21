import secrets
from datetime import datetime, timezone
from uuid import uuid4

from loguru import logger
from pymongo import DESCENDING, ReturnDocument

from .base_handler import BaseHandler

SNAPSHOT_SCHEMA_VERSION = 1

_WITHOUT_SNAPSHOT = {"_id": 0, "snapshot": 0}


def _active(now):
    return {
        "revoked_at": None,
        "$or": [{"expires_at": None}, {"expires_at": {"$gt": now}}],
    }


class ShareLinkHandler(BaseHandler):
    """Public read-only links that serve a frozen hypothesis snapshot."""

    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.collection = self.db["share_links"]
        self._create_indexes()

    def _create_indexes(self):
        try:
            self.collection.create_index("token", unique=True)
            self.collection.create_index(
                [("owner_user_id", 1), ("hypothesis_id", 1), ("created_at", DESCENDING)]
            )
            self.collection.create_index([("owner_user_id", 1), ("project_id", 1)])
        except Exception as e:
            logger.warning(f"Could not create share link indexes (may already exist): {e}")

    def create_link(self, owner_user_id, hypothesis_id, project_id, label, expires_at, snapshot):
        link = {
            "id": str(uuid4()),
            "token": secrets.token_urlsafe(32),
            "owner_user_id": owner_user_id,
            "hypothesis_id": hypothesis_id,
            "project_id": project_id,
            "label": label,
            "created_at": datetime.now(timezone.utc),
            "expires_at": expires_at,
            "revoked_at": None,
            "view_count": 0,
            "last_viewed_at": None,
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "snapshot": snapshot,
        }
        self.collection.insert_one(link)
        logger.info(f"Created share link {link['id']} for hypothesis {hypothesis_id}")
        return link

    def count_active_links(self, owner_user_id, hypothesis_id):
        return self.collection.count_documents(
            {
                "owner_user_id": owner_user_id,
                "hypothesis_id": hypothesis_id,
                **_active(datetime.now(timezone.utc)),
            }
        )

    def list_links(self, owner_user_id, hypothesis_id):
        return list(
            self.collection.find(
                {"owner_user_id": owner_user_id, "hypothesis_id": hypothesis_id},
                _WITHOUT_SNAPSHOT,
            ).sort("created_at", DESCENDING)
        )

    def revoke_link(self, owner_user_id, link_id):
        """Revoke a link; revoking an already revoked link keeps its original time."""
        owned = {"id": link_id, "owner_user_id": owner_user_id}
        self.collection.update_one(
            {**owned, "revoked_at": None},
            {"$set": {"revoked_at": datetime.now(timezone.utc)}},
        )
        return self.collection.find_one(owned, _WITHOUT_SNAPSHOT)

    def open_link(self, token):
        """Return an active link's snapshot and count the view, or None."""
        now = datetime.now(timezone.utc)
        return self.collection.find_one_and_update(
            {"token": token, **_active(now)},
            {"$inc": {"view_count": 1}, "$set": {"last_viewed_at": now}},
            projection={
                "_id": 0,
                "snapshot": 1,
                "schema_version": 1,
                "created_at": 1,
                "expires_at": 1,
            },
            return_document=ReturnDocument.AFTER,
        )
