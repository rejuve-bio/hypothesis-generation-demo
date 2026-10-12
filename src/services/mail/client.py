from __future__ import annotations

import os

from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from loguru import logger

_fast_mail: FastMail | None = None


def _build_fast_mail() -> FastMail | None:
    mail_server = os.getenv("MAIL_SERVER", "")
    if not mail_server:
        return None
    return FastMail(ConnectionConfig(
        MAIL_USERNAME=os.getenv("MAIL_USERNAME", ""),
        MAIL_PASSWORD=os.getenv("MAIL_PASSWORD", ""),
        MAIL_FROM=os.getenv("MAIL_FROM", ""),
        MAIL_PORT=int(os.getenv("MAIL_PORT", "587")),
        MAIL_SERVER=mail_server,
        MAIL_STARTTLS=os.getenv("MAIL_TLS", "true").lower() == "true",
        MAIL_SSL_TLS=os.getenv("MAIL_SSL", "false").lower() == "true",
        USE_CREDENTIALS=True,
    ))


def init_mail(
    mail_username: str,
    mail_password: str,
    mail_from: str,
    mail_server: str,
    mail_port: int = 587,
    mail_tls: bool = True,
    mail_ssl: bool = False,
) -> None:
    global _fast_mail
    if not mail_server:
        logger.warning("init_mail called with empty MAIL_SERVER — email disabled")
        return
    _fast_mail = FastMail(ConnectionConfig(
        MAIL_USERNAME=mail_username,
        MAIL_PASSWORD=mail_password,
        MAIL_FROM=mail_from,
        MAIL_PORT=mail_port,
        MAIL_SERVER=mail_server,
        MAIL_STARTTLS=mail_tls,
        MAIL_SSL_TLS=mail_ssl,
        USE_CREDENTIALS=True,
    ))
    logger.info("Mail service initialized")


async def send_email(
    subject: str,
    recipients: list[str],
    body: str,
    subtype: str = "html",
) -> None:
    """Send an email from the API process or a Prefect/Dask worker.
    Falls back to building FastMail from env vars if init_mail was never called."""
    if not recipients:
        logger.warning("send_email called with empty recipients list — skipping")
        return

    fm = _fast_mail or _build_fast_mail()
    if fm is None:
        logger.warning("MAIL_SERVER not configured — skipping email: %s", subject)
        return

    try:
        await fm.send_message(MessageSchema(
            subject=subject,
            recipients=recipients,
            body=body,
            subtype=subtype,
        ))
        logger.info("Email sent to {} | subject: {}", recipients, subject)
    except Exception as exc:
        logger.error("Failed to send email to {}: {}", recipients, exc)
