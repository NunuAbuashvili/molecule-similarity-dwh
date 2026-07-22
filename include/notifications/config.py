"""Configuration for MS Teams failure notifications."""

import os

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")
