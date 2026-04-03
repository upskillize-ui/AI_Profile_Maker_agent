"""
Celery Task Queue
═════════════════
Background tasks for email notifications etc.
"""

from celery import Celery
from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "upskillize_profile",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=120,
    task_soft_time_limit=90,
    worker_prefetch_multiplier=1,
)


@celery_app.task(name="send_visibility_notification")
def send_visibility_notification(student_email: str, visibility: str, profile_url: str):
    """
    INTEGRATION POINT — Wire up your email service here.
    (SendGrid / SES / SMTP)
    """
    pass
