from src.services.mail.client import init_mail, send_email
from src.services.mail.notifications import send_pipeline_notification

__all__ = ["init_mail", "send_email", "send_pipeline_notification"]
