from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any, Dict, List

import discord
from discord import app_commands
from zoneinfo import ZoneInfo

DATA_DIR = "data"
DB_FILE = os.path.join(DATA_DIR, "bapnboard.sqlite3")
TZ = ZoneInfo("America/New_York")
DEFAULT_START_ELO = 800.0
GLOBAL_BIO_KEY = "__global"
PENDING_CHALLENGE_TIMEOUT = timedelta(hours=6)

logger = logging.getLogger("bapnboard")

MODES: Dict[str, Dict[str, Any]] = {
    "speedrun": {"label": "Speedrun", "type": "time"},
    "score": {"label": "First to X", "type": "score", "default_target": 1},
}

MODE_TYPE_CHOICES = [
    app_commands.Choice(name="Speedrun", value="speedrun"),
    app_commands.Choice(name="First to X", value="score"),
]


def normalize_category(category: str) -> str:
    return category.replace(" ", "_").lower()


def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def chunk_list(items: List[str], size: int) -> List[List[str]]:
    if not items:
        return [["No entries."]]
    return [items[i : i + size] for i in range(0, len(items), size)]
