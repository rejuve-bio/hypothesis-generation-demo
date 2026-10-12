from __future__ import annotations

import asyncio
import os

from loguru import logger

from src.services.mail.client import send_email
from src.services.mail.templates import build_complete_email, build_failed_email


def send_pipeline_notification(
    user_email: str | None,
    project_name: str | None,
    project_id: str,
    *,
    success: bool,
    detail: str | None = None,
) -> None:
    """Send a completion or failure notification for an analysis pipeline run. Never raises."""
    if not user_email:
        logger.info("[PIPELINE] No user_email provided — skipping notification")
        return

    if success:
        payload = build_complete_email(
            to=user_email,
            project_name=project_name or project_id,
            project_id=project_id,
            app_base_url=os.getenv("APP_BASE_URL", "https://dev.rejuve.bio"),
        )
        ok_log = f"Completion email sent to {user_email}"
        err_log = "Could not send completion email"
    else:
        payload = build_failed_email(
            to=user_email,
            project_name=project_name or project_id,
            error=detail,
        )
        ok_log = f"Failure notification email sent to {user_email}"
        err_log = "Could not send failure notification email"

    try:
        asyncio.run(send_email(subject=payload.subject, recipients=[user_email], body=payload.body_html))
        logger.info(f"[PIPELINE] {ok_log}")
    except Exception as mail_e:
        logger.error(f"[PIPELINE] {err_log}: {mail_e}")
