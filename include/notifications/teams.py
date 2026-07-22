"""MS Teams failure notifications via on_failure_callback."""

import logging
import traceback
from datetime import datetime, timezone

import requests

from include.notifications.config import TEAMS_WEBHOOK_URL


logger = logging.getLogger(__name__)


def post_to_teams(webhook_url: str, payload: dict) -> None:
    """
    POST a payload to the Teams webhook, degrading gracefully
    if it's unset or the request fails.
    """
    if not webhook_url:
        logger.warning(
            "Teams webhook URL not configured, skipping notification."
        )
        return
    try:
        response = requests.post(webhook_url, json=payload, timeout=5)
        if response.ok:
            logger.info("Notification sent to MS Teams.")
        else:
            logger.warning(
                "MS Teams notification failed with status %d: %s",
                response.status_code, response.text[:500],
            )
    except requests.exceptions.RequestException as notify_error:
        logger.warning(
            "MS Teams notification failed: %s",
            notify_error
        )
    except Exception:
        logger.exception(
            "Unexpected error while sending MS Teams notification."
        )


def build_failure_payload(context: dict) -> dict:
    """
    Build a Teams Adaptive Card payload from an Airflow failure context.
    """
    ti = context["task_instance"]
    exception = context.get("exception")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    tb = traceback.extract_tb(exception.__traceback__) if exception else []
    last_frame = tb[-1] if tb else None
    file_name = last_frame.filename if last_frame else "unknown"
    line_no = str(last_frame.lineno) if last_frame else "unknown"

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": (
                        "http://adaptivecards.io/schemas/adaptive-card.json"
                    ),
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"🚨 Airflow task failed: {ti.task_id}",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": "Attention",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "DAG", "value": str(ti.dag_id)},
                                {"title": "Task", "value": str(ti.task_id)},
                                {
                                    "title": "Run ID",
                                    "value": str(
                                        context.get("run_id", "unknown")
                                    ),
                                },
                                {
                                    "title": "Error Type",
                                    "value": type(
                                        exception
                                    ).__name__ if exception else "unknown",
                                },
                                {"title": "Error", "value": str(exception)},
                                {"title": "File", "value": str(file_name)},
                                {"title": "Line", "value": line_no},
                                {"title": "Time", "value": timestamp},
                            ],
                        },
                    ],
                },
            }
        ],
    }


def notify_task_failure(context: dict) -> None:
    """
    on_failure_callback that builds and sends the Teams failure notification.
    """
    payload = build_failure_payload(context)
    post_to_teams(TEAMS_WEBHOOK_URL, payload)
