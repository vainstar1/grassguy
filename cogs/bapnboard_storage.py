from __future__ import annotations

import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from .bapnboard_shared import DB_FILE, GLOBAL_BIO_KEY, normalize_category


class BoardStorage:
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id INTEGER PRIMARY KEY,
                    participant_role_id INTEGER,
                    challenge_channel_id INTEGER,
                    outgoing_channel_id INTEGER,
                    announce_channel_id INTEGER,
                    leaderboard_channel_id INTEGER,
                    leaderboard_message_id INTEGER,
                    thread_cleanup_seconds INTEGER DEFAULT 21600
                );
                CREATE TABLE IF NOT EXISTS leaderboards (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    participant_role_id INTEGER,
                    challenge_channel_id INTEGER,
                    outgoing_channel_id INTEGER,
                    announce_channel_id INTEGER,
                    leaderboard_channel_id INTEGER,
                    leaderboard_message_id INTEGER,
                    thread_cleanup_seconds INTEGER DEFAULT 21600,
                    PRIMARY KEY (guild_id, category)
                );
                CREATE TABLE IF NOT EXISTS category_modes (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    mode_key TEXT NOT NULL,
                    mode_target INTEGER,
                    PRIMARY KEY (guild_id, category)
                );
                CREATE TABLE IF NOT EXISTS legacy_categories (
                    guild_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    PRIMARY KEY (guild_id, name)
                );
                CREATE TABLE IF NOT EXISTS players (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    elo REAL NOT NULL,
                    wins INTEGER NOT NULL,
                    losses INTEGER NOT NULL,
                    PRIMARY KEY (guild_id, category, user_id)
                );
                CREATE TABLE IF NOT EXISTS player_meta (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    display_name TEXT,
                    avatar_url TEXT,
                    PRIMARY KEY (guild_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS removed_players (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    elo REAL NOT NULL,
                    wins INTEGER NOT NULL,
                    losses INTEGER NOT NULL,
                    PRIMARY KEY (guild_id, category, user_id)
                );
                CREATE TABLE IF NOT EXISTS bios (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    bio TEXT NOT NULL,
                    PRIMARY KEY (guild_id, category, user_id)
                );
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL,
                    opponent_id INTEGER NOT NULL,
                    challenger INTEGER NOT NULL,
                    user_value TEXT,
                    opponent_value TEXT,
                    result TEXT NOT NULL,
                    elo_change REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_matches_lookup ON matches (guild_id, category, user_id);
                CREATE TABLE IF NOT EXISTS active_matches (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    leaderboard TEXT NOT NULL,
                    challenger_id INTEGER NOT NULL,
                    opponent_id INTEGER,
                    status TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER,
                    thread_id INTEGER,
                    thread_message_id INTEGER,
                    created_at TEXT NOT NULL,
                    rank_range INTEGER,
                    mode_key TEXT NOT NULL,
                    mode_target INTEGER,
                    response_deadline TEXT,
                    accepted_at TEXT,
                    PRIMARY KEY (guild_id, category, match_id)
                );
                CREATE TABLE IF NOT EXISTS active_match_results (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    winner_id INTEGER,
                    loser_id INTEGER,
                    winner_value TEXT,
                    loser_value TEXT,
                    completed_at TEXT,
                    override_notes TEXT,
                    winner_elo_change REAL,
                    loser_elo_change REAL,
                    winner_new_elo REAL,
                    loser_new_elo REAL,
                    winner_old_elo REAL,
                    loser_old_elo REAL,
                    PRIMARY KEY (guild_id, category, match_id)
                );
                CREATE TABLE IF NOT EXISTS active_match_submissions (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    value TEXT,
                    metric REAL,
                    PRIMARY KEY (guild_id, category, match_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS active_match_cancel_votes (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    PRIMARY KEY (guild_id, category, match_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS active_match_deletions (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    thread_id INTEGER NOT NULL,
                    delete_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, category, thread_id)
                );
                """
            )
            try:
                conn.execute("ALTER TABLE active_matches ADD COLUMN thread_message_id INTEGER")
            except sqlite3.OperationalError:
                pass
    def _ensure_guild_entry(self, target: Dict[str, Dict[str, Any]], gid_s: str) -> Dict[str, Any]:
        data = target.get(gid_s)
        if data is None:
            data = {"leaderboards": {}, "category_modes": {}, "categories": []}
            target[gid_s] = data
        else:
            data.setdefault("leaderboards", {})
            data.setdefault("category_modes", {})
            data.setdefault("categories", [])
        return data

    def _display_from_safe(self, safe: str) -> str:
        text = safe.replace("_", " ").strip()
        return text.title() if text else safe

    def load_all(self) -> Dict[str, Any]:
        with self._lock:
            with self._connect() as conn:
                guild_configs: Dict[str, Dict[str, Any]] = {}
                players: Dict[str, Dict[str, Dict[int, Dict[str, Any]]]] = {}
                players_meta: Dict[str, Dict[str, Dict[str, Any]]] = {}
                removed: Dict[str, Dict[str, Dict[int, Dict[str, Any]]]] = {}
                bios: Dict[str, Dict[str, Dict[str, str]]] = {}
                active_fights: Dict[str, Dict[str, Dict[str, Any]]] = {}
                for row in conn.execute("SELECT * FROM guild_settings"):
                    gid_s = str(row["guild_id"])
                    data = self._ensure_guild_entry(guild_configs, gid_s)
                    if row["participant_role_id"] is not None:
                        data["participant_role_id"] = row["participant_role_id"]
                    if row["challenge_channel_id"] is not None:
                        data["challenge_channel_id"] = row["challenge_channel_id"]
                    if row["outgoing_channel_id"] is not None:
                        data["outgoing_channel_id"] = row["outgoing_channel_id"]
                    if row["announce_channel_id"] is not None:
                        data["announce_channel_id"] = row["announce_channel_id"]
                    if row["leaderboard_channel_id"] is not None:
                        data["leaderboard_channel_id"] = row["leaderboard_channel_id"]
                    if row["leaderboard_message_id"] is not None:
                        data["leaderboard_message_id"] = row["leaderboard_message_id"]
                    data["thread_cleanup_seconds"] = row["thread_cleanup_seconds"] or 21600
                for row in conn.execute("SELECT * FROM leaderboards"):
                    gid_s = str(row["guild_id"])
                    safe = row["category"]
                    data = self._ensure_guild_entry(guild_configs, gid_s)
                    entry = {
                        "name": row["display_name"],
                        "participant_role_id": row["participant_role_id"],
                        "challenge_channel_id": row["challenge_channel_id"],
                        "outgoing_channel_id": row["outgoing_channel_id"],
                        "announce_channel_id": row["announce_channel_id"],
                        "leaderboard_channel_id": row["leaderboard_channel_id"],
                        "leaderboard_message_id": row["leaderboard_message_id"],
                        "thread_cleanup_seconds": row["thread_cleanup_seconds"] or data.get("thread_cleanup_seconds", 21600),
                    }
                    data["leaderboards"][safe] = entry
                for row in conn.execute("SELECT * FROM category_modes"):
                    gid_s = str(row["guild_id"])
                    safe = row["category"]
                    data = self._ensure_guild_entry(guild_configs, gid_s)
                    data["category_modes"][safe] = {"key": row["mode_key"], "target": row["mode_target"]}
                for row in conn.execute("SELECT * FROM legacy_categories"):
                    gid_s = str(row["guild_id"])
                    data = self._ensure_guild_entry(guild_configs, gid_s)
                    if row["name"] not in data["categories"]:
                        data["categories"].append(row["name"])
                for row in conn.execute("SELECT guild_id, category, user_id, elo, wins, losses FROM players"):
                    gid_s = str(row["guild_id"])
                    safe = row["category"]
                    players.setdefault(gid_s, {}).setdefault(safe, {})[row["user_id"]] = {
                        "elo": row["elo"],
                        "wins": row["wins"],
                        "losses": row["losses"],
                    }
                for row in conn.execute("SELECT guild_id, user_id, display_name, avatar_url FROM player_meta"):
                    gid_s = str(row["guild_id"])
                    players_meta.setdefault(gid_s, {})[str(row["user_id"])] = {
                        "name": row["display_name"],
                        "avatar": row["avatar_url"],
                    }
                for row in conn.execute("SELECT guild_id, category, user_id, elo, wins, losses FROM removed_players"):
                    gid_s = str(row["guild_id"])
                    safe = row["category"]
                    removed.setdefault(gid_s, {}).setdefault(safe, {})[row["user_id"]] = {
                        "elo": row["elo"],
                        "wins": row["wins"],
                        "losses": row["losses"],
                    }
                for row in conn.execute("SELECT guild_id, category, user_id, bio FROM bios"):
                    gid_s = str(row["guild_id"])
                    bios.setdefault(gid_s, {}).setdefault(row["category"], {})[str(row["user_id"])] = row["bio"]
                match_index: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
                for row in conn.execute("SELECT * FROM active_matches"):
                    gid = row["guild_id"]
                    gid_s = str(gid)
                    category = row["category"]
                    bucket = active_fights.setdefault(gid_s, {}).setdefault(category, {"matches": {}, "deletions": []})
                    match_data: Dict[str, Any] = {
                        "id": row["match_id"],
                        "leaderboard": row["leaderboard"],
                        "challenger_id": row["challenger_id"],
                        "opponent_id": row["opponent_id"],
                        "status": row["status"],
                        "channel_id": row["channel_id"],
                        "message_id": row["message_id"],
                        "thread_id": row["thread_id"],
                        "thread_message_id": row["thread_message_id"] if "thread_message_id" in row.keys() else None,
                        "created_at": row["created_at"],
                        "rank_range": row["rank_range"],
                        "mode": {"key": row["mode_key"], "target": row["mode_target"]},
                        "submissions": {},
                        "cancel_votes": [],
                    }
                    if row["response_deadline"]:
                        match_data["response_deadline"] = row["response_deadline"]
                    if row["accepted_at"]:
                        match_data["accepted_at"] = row["accepted_at"]
                    bucket["matches"][row["match_id"]] = match_data
                    match_index[(gid, category, row["match_id"])] = match_data
                for row in conn.execute("SELECT * FROM active_match_results"):
                    key = (row["guild_id"], row["category"], row["match_id"])
                    match = match_index.get(key)
                    if not match:
                        continue
                    match["result"] = {
                        "winner_id": row["winner_id"],
                        "loser_id": row["loser_id"],
                        "winner_value": row["winner_value"],
                        "loser_value": row["loser_value"],
                        "override_notes": row["override_notes"],
                        "completed_at": row["completed_at"],
                        "winner_elo_change": row["winner_elo_change"],
                        "loser_elo_change": row["loser_elo_change"],
                        "winner_new_elo": row["winner_new_elo"],
                        "loser_new_elo": row["loser_new_elo"],
                        "winner_old_elo": row["winner_old_elo"],
                        "loser_old_elo": row["loser_old_elo"],
                    }
                for row in conn.execute("SELECT guild_id, category, match_id, user_id, kind, value, metric FROM active_match_submissions"):
                    key = (row["guild_id"], row["category"], row["match_id"])
                    match = match_index.get(key)
                    if not match:
                        continue
                    match.setdefault("submissions", {})[str(row["user_id"])] = {
                        "kind": row["kind"],
                        "value": row["value"],
                        "metric": row["metric"],
                    }
                for row in conn.execute("SELECT guild_id, category, match_id, user_id FROM active_match_cancel_votes"):
                    key = (row["guild_id"], row["category"], row["match_id"])
                    match = match_index.get(key)
                    if not match:
                        continue
                    votes = match.setdefault("cancel_votes", [])
                    if row["user_id"] not in votes:
                        votes.append(row["user_id"])
                for row in conn.execute("SELECT guild_id, category, thread_id, delete_at FROM active_match_deletions"):
                    gid_s = str(row["guild_id"])
                    bucket = active_fights.setdefault(gid_s, {}).setdefault(row["category"], {"matches": {}, "deletions": []})
                    bucket["deletions"].append({"thread_id": row["thread_id"], "delete_at": row["delete_at"]})
                return {
                    "guild_configs": guild_configs,
                    "players": players,
                    "players_meta": players_meta,
                    "removed": removed,
                    "bios": bios,
                    "active_fights": active_fights,
                }
    def save_guild_configs(self, configs: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                seen: List[int] = []
                for gid_s, payload in configs.items():
                    try:
                        gid = int(gid_s)
                    except ValueError:
                        continue
                    seen.append(gid)
                    cur.execute(
                        """
                        INSERT INTO guild_settings (
                            guild_id,
                            participant_role_id,
                            challenge_channel_id,
                            outgoing_channel_id,
                            announce_channel_id,
                            leaderboard_channel_id,
                            leaderboard_message_id,
                            thread_cleanup_seconds
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(guild_id) DO UPDATE SET
                            participant_role_id=excluded.participant_role_id,
                            challenge_channel_id=excluded.challenge_channel_id,
                            outgoing_channel_id=excluded.outgoing_channel_id,
                            announce_channel_id=excluded.announce_channel_id,
                            leaderboard_channel_id=excluded.leaderboard_channel_id,
                            leaderboard_message_id=excluded.leaderboard_message_id,
                            thread_cleanup_seconds=excluded.thread_cleanup_seconds
                        """,
                        (
                            gid,
                            payload.get("participant_role_id"),
                            payload.get("challenge_channel_id"),
                            payload.get("outgoing_channel_id"),
                            payload.get("announce_channel_id"),
                            payload.get("leaderboard_channel_id"),
                            payload.get("leaderboard_message_id"),
                            payload.get("thread_cleanup_seconds", 21600),
                        ),
                    )
                    cur.execute("DELETE FROM leaderboards WHERE guild_id=?", (gid,))
                    cur.execute("DELETE FROM category_modes WHERE guild_id=?", (gid,))
                    cur.execute("DELETE FROM legacy_categories WHERE guild_id=?", (gid,))
                    boards = payload.get("leaderboards", {})
                    modes_map: Dict[str, Tuple[str, Optional[int]]] = {}
                    for safe_key, data in payload.get("category_modes", {}).items():
                        if isinstance(data, dict):
                            key = data.get("key") or "speedrun"
                            modes_map[normalize_category(safe_key)] = (key, data.get("target"))
                    for safe_key, board in boards.items():
                        safe = normalize_category(safe_key)
                        cur.execute(
                            """
                            INSERT INTO leaderboards (
                                guild_id,
                                category,
                                display_name,
                                participant_role_id,
                                challenge_channel_id,
                                outgoing_channel_id,
                                announce_channel_id,
                                leaderboard_channel_id,
                                leaderboard_message_id,
                                thread_cleanup_seconds
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                gid,
                                safe,
                                board.get("name", self._display_from_safe(safe)),
                                board.get("participant_role_id"),
                                board.get("challenge_channel_id"),
                                board.get("outgoing_channel_id"),
                                board.get("announce_channel_id"),
                                board.get("leaderboard_channel_id"),
                                board.get("leaderboard_message_id"),
                                board.get("thread_cleanup_seconds", payload.get("thread_cleanup_seconds", 21600)),
                            ),
                        )
                        mode = board.get("mode")
                        if isinstance(mode, dict):
                            key = mode.get("key") or "speedrun"
                            modes_map[safe] = (key, mode.get("target"))
                        elif safe not in modes_map:
                            modes_map[safe] = ("speedrun", None)
                    for safe, (mode_key, mode_target) in modes_map.items():
                        cur.execute(
                            """
                            INSERT INTO category_modes (guild_id, category, mode_key, mode_target)
                            VALUES (?, ?, ?, ?)
                            """,
                            (gid, safe, mode_key, mode_target),
                        )
                    for name in payload.get("categories", []):
                        cur.execute(
                            "INSERT INTO legacy_categories (guild_id, name) VALUES (?, ?)",
                            (gid, name),
                        )
                if seen:
                    placeholders = ",".join("?" for _ in seen)
                    cur.execute(f"DELETE FROM guild_settings WHERE guild_id NOT IN ({placeholders})", seen)
                    cur.execute(f"DELETE FROM leaderboards WHERE guild_id NOT IN ({placeholders})", seen)
                    cur.execute(f"DELETE FROM category_modes WHERE guild_id NOT IN ({placeholders})", seen)
                    cur.execute(f"DELETE FROM legacy_categories WHERE guild_id NOT IN ({placeholders})", seen)
    def load_players(self, guild_id: int, category: str) -> Dict[int, Dict[str, Any]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                result: Dict[int, Dict[str, Any]] = {}
                for row in conn.execute(
                    "SELECT user_id, elo, wins, losses FROM players WHERE guild_id=? AND category=?",
                    (guild_id, safe),
                ):
                    result[row["user_id"]] = {
                        "elo": row["elo"],
                        "wins": row["wins"],
                        "losses": row["losses"],
                    }
                return result

    def save_players(self, guild_id: int, category: str, players: Dict[int, Dict[str, Any]]) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM players WHERE guild_id=? AND category=?", (guild_id, safe))
                for user_id, payload in players.items():
                    cur.execute(
                        """
                        INSERT INTO players (guild_id, category, user_id, elo, wins, losses)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            guild_id,
                            safe,
                            user_id,
                            float(payload.get("elo", 0.0)),
                            int(payload.get("wins", 0)),
                            int(payload.get("losses", 0)),
                        ),
                    )

    def save_player_meta(self, guild_id: int, meta: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM player_meta WHERE guild_id=?", (guild_id,))
                for uid_str, payload in meta.items():
                    try:
                        user_id = int(uid_str)
                    except ValueError:
                        continue
                    cur.execute(
                        """
                        INSERT INTO player_meta (guild_id, user_id, display_name, avatar_url)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            guild_id,
                            user_id,
                            payload.get("name"),
                            payload.get("avatar"),
                        ),
                    )

    def save_removed(self, guild_id: int, category: str, removed_map: Dict[int, Dict[str, Any]]) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM removed_players WHERE guild_id=? AND category=?", (guild_id, safe))
                for user_id, payload in removed_map.items():
                    cur.execute(
                        """
                        INSERT INTO removed_players (guild_id, category, user_id, elo, wins, losses)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            guild_id,
                            safe,
                            user_id,
                            float(payload.get("elo", 0.0)),
                            int(payload.get("wins", 0)),
                            int(payload.get("losses", 0)),
                        ),
                    )

    def save_bios(self, guild_id: int, category: str, bios_map: Dict[str, str]) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM bios WHERE guild_id=? AND category=?", (guild_id, safe))
                for uid_str, value in bios_map.items():
                    try:
                        user_id = int(uid_str)
                    except ValueError:
                        continue
                    cur.execute(
                        """
                        INSERT INTO bios (guild_id, category, user_id, bio)
                        VALUES (?, ?, ?, ?)
                        """,
                        (guild_id, safe, user_id, value),
                    )

    def append_match(
        self,
        guild_id: int,
        category: str,
        user_id: int,
        recorded_at: str,
        opponent_id: int,
        challenger: bool,
        user_value: Optional[str],
        opponent_value: Optional[str],
        result: str,
        elo_change: float,
    ) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO matches (
                        guild_id,
                        category,
                        user_id,
                        recorded_at,
                        opponent_id,
                        challenger,
                        user_value,
                        opponent_value,
                        result,
                        elo_change
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id,
                        safe,
                        user_id,
                        recorded_at,
                        opponent_id,
                        1 if challenger else 0,
                        user_value,
                        opponent_value,
                        result,
                        float(elo_change),
                    ),
                )

    def load_match_history(self, guild_id: int, category: str) -> List[Dict[str, Any]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches
                    WHERE guild_id=? AND category=?
                    ORDER BY recorded_at ASC, id ASC
                    """,
                    (guild_id, safe),
                )
                output: List[Dict[str, Any]] = []
                for row in rows:
                    output.append(
                        {
                            "user_id": str(row["user_id"]),
                            "date": row["recorded_at"],
                            "opponent_id": str(row["opponent_id"]),
                            "challenger": bool(row["challenger"]),
                            "time": row["user_value"],
                            "opponent_time": row["opponent_value"],
                            "result": row["result"],
                            "elo_change": str(row["elo_change"]),
                        }
                    )
                return output

    def count_member_matches(self, guild_id: int, category: str, member_id: int) -> int:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM matches WHERE guild_id=? AND category=? AND user_id=?",
                    (guild_id, safe, member_id),
                ).fetchone()
                return int(row["c"] if row else 0)
    def save_active_fights(self, guild_id: int, payload: Dict[str, Any]) -> None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM active_matches WHERE guild_id=?", (guild_id,))
                cur.execute("DELETE FROM active_match_results WHERE guild_id=?", (guild_id,))
                cur.execute("DELETE FROM active_match_submissions WHERE guild_id=?", (guild_id,))
                cur.execute("DELETE FROM active_match_cancel_votes WHERE guild_id=?", (guild_id,))
                cur.execute("DELETE FROM active_match_deletions WHERE guild_id=?", (guild_id,))
                for category, data in payload.items():
                    if not isinstance(data, dict):
                        continue
                    matches = data.get("matches", {})
                    deletions = data.get("deletions", [])
                    if not isinstance(matches, dict):
                        matches = {}
                    if not isinstance(deletions, list):
                        deletions = []
                    for match_id, match in matches.items():
                        if not isinstance(match, dict):
                            continue
                        mode = match.get("mode") or {}
                        cur.execute(
                            """
                            INSERT INTO active_matches (
                                guild_id,
                                category,
                                match_id,
                                leaderboard,
                                challenger_id,
                                opponent_id,
                                status,
                                channel_id,
                                message_id,
                                thread_id,
                                thread_message_id,
                                created_at,
                                rank_range,
                                mode_key,
                                mode_target,
                                response_deadline,
                                accepted_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                guild_id,
                                category,
                                match_id,
                                match.get("leaderboard") or normalize_category(category),
                                match.get("challenger_id"),
                                match.get("opponent_id"),
                                match.get("status", "open"),
                                match.get("channel_id"),
                                match.get("message_id"),
                                match.get("thread_id"),
                                match.get("thread_message_id"),
                                match.get("created_at"),
                                match.get("rank_range"),
                                mode.get("key") or "speedrun",
                                mode.get("target"),
                                match.get("response_deadline"),
                                match.get("accepted_at"),
                            ),
                        )
                        result = match.get("result") if isinstance(match.get("result"), dict) else None
                        if result:
                            cur.execute(
                                """
                                INSERT INTO active_match_results (
                                    guild_id,
                                    category,
                                    match_id,
                                    winner_id,
                                    loser_id,
                                    winner_value,
                                    loser_value,
                                    completed_at,
                                    override_notes,
                                    winner_elo_change,
                                    loser_elo_change,
                                    winner_new_elo,
                                    loser_new_elo,
                                    winner_old_elo,
                                    loser_old_elo
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    guild_id,
                                    category,
                                    match_id,
                                    result.get("winner_id"),
                                    result.get("loser_id"),
                                    result.get("winner_value"),
                                    result.get("loser_value"),
                                    result.get("completed_at"),
                                    result.get("override_notes"),
                                    result.get("winner_elo_change"),
                                    result.get("loser_elo_change"),
                                    result.get("winner_new_elo"),
                                    result.get("loser_new_elo"),
                                    result.get("winner_old_elo"),
                                    result.get("loser_old_elo"),
                                ),
                            )
                        submissions = match.get("submissions", {})
                        if isinstance(submissions, dict):
                            for uid_str, record in submissions.items():
                                if not isinstance(record, dict):
                                    continue
                                try:
                                    user_id = int(uid_str)
                                except ValueError:
                                    continue
                                cur.execute(
                                    """
                                    INSERT INTO active_match_submissions (
                                        guild_id,
                                        category,
                                        match_id,
                                        user_id,
                                        kind,
                                        value,
                                        metric
                                    )
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        guild_id,
                                        category,
                                        match_id,
                                        user_id,
                                        record.get("kind"),
                                        record.get("value"),
                                        record.get("metric"),
                                    ),
                                )
                        cancel_votes = match.get("cancel_votes", [])
                        if isinstance(cancel_votes, list):
                            for user_id in cancel_votes:
                                cur.execute(
                                    """
                                    INSERT INTO active_match_cancel_votes (
                                        guild_id,
                                        category,
                                        match_id,
                                        user_id
                                    )
                                    VALUES (?, ?, ?, ?)
                                    """,
                                    (guild_id, category, match_id, user_id),
                                )
                    for entry in deletions:
                        if not isinstance(entry, dict):
                            continue
                        thread_id = entry.get("thread_id")
                        delete_at = entry.get("delete_at")
                        if thread_id and delete_at:
                            cur.execute(
                                """
                                INSERT INTO active_match_deletions (guild_id, category, thread_id, delete_at)
                                VALUES (?, ?, ?, ?)
                                """,
                                (guild_id, category, thread_id, delete_at),
                            )

    def rename_category(self, guild_id: int, old_category: str, new_category: str) -> None:
        old_safe = normalize_category(old_category)
        new_safe = normalize_category(new_category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE players SET category=? WHERE guild_id=? AND category=?",
                    (new_safe, guild_id, old_safe),
                )
                cur.execute(
                    "UPDATE removed_players SET category=? WHERE guild_id=? AND category=?",
                    (new_safe, guild_id, old_safe),
                )
                cur.execute(
                    "UPDATE bios SET category=? WHERE guild_id=? AND category=?",
                    (new_safe, guild_id, old_safe),
                )
                cur.execute(
                    "UPDATE matches SET category=? WHERE guild_id=? AND category=?",
                    (new_safe, guild_id, old_safe),
                )
                cur.execute(
                    "UPDATE active_matches SET category=?, leaderboard=? WHERE guild_id=? AND category=?",
                    (new_category, new_safe, guild_id, old_category),
                )
                cur.execute(
                    "UPDATE active_match_results SET category=? WHERE guild_id=? AND category=?",
                    (new_category, guild_id, old_category),
                )
                cur.execute(
                    "UPDATE active_match_submissions SET category=? WHERE guild_id=? AND category=?",
                    (new_category, guild_id, old_category),
                )
                cur.execute(
                    "UPDATE active_match_cancel_votes SET category=? WHERE guild_id=? AND category=?",
                    (new_category, guild_id, old_category),
                )
                cur.execute(
                    "UPDATE active_match_deletions SET category=? WHERE guild_id=? AND category=?",
                    (new_category, guild_id, old_category),
                )

    def delete_category(self, guild_id: int, category: str) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    "DELETE FROM players WHERE guild_id=? AND category=?",
                    (guild_id, safe),
                )
                cur.execute(
                    "DELETE FROM removed_players WHERE guild_id=? AND category=?",
                    (guild_id, safe),
                )
                cur.execute(
                    "DELETE FROM bios WHERE guild_id=? AND category=?",
                    (guild_id, safe),
                )
                cur.execute(
                    "DELETE FROM matches WHERE guild_id=? AND category=?",
                    (guild_id, safe),
                )
                cur.execute(
                    "DELETE FROM active_matches WHERE guild_id=? AND category=?",
                    (guild_id, category),
                )
                cur.execute(
                    "DELETE FROM active_match_results WHERE guild_id=? AND category=?",
                    (guild_id, category),
                )
                cur.execute(
                    "DELETE FROM active_match_submissions WHERE guild_id=? AND category=?",
                    (guild_id, category),
                )
                cur.execute(
                    "DELETE FROM active_match_cancel_votes WHERE guild_id=? AND category=?",
                    (guild_id, category),
                )
                cur.execute(
                    "DELETE FROM active_match_deletions WHERE guild_id=? AND category=?",
                    (guild_id, category),
                )

    def list_categories(self, guild_id: int) -> List[str]:
        with self._lock:
            with self._connect() as conn:
                names: Dict[str, str] = {}
                for row in conn.execute("SELECT display_name FROM leaderboards WHERE guild_id=?", (guild_id,)):
                    value = row["display_name"]
                    if value:
                        names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT name FROM legacy_categories WHERE guild_id=?", (guild_id,)):
                    value = row["name"]
                    if value:
                        names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM active_matches WHERE guild_id=?", (guild_id,)):
                    value = row["category"]
                    if value:
                        names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM players WHERE guild_id=?", (guild_id,)):
                    value = self._display_from_safe(row["category"])
                    names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM matches WHERE guild_id=?", (guild_id,)):
                    value = self._display_from_safe(row["category"])
                    names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM removed_players WHERE guild_id=?", (guild_id,)):
                    safe = row["category"]
                    if safe == GLOBAL_BIO_KEY:
                        continue
                    value = self._display_from_safe(safe)
                    names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM bios WHERE guild_id=?", (guild_id,)):
                    safe = row["category"]
                    if safe == GLOBAL_BIO_KEY:
                        continue
                    value = safe if " " in safe else self._display_from_safe(safe)
                    names.setdefault(value.lower(), value)
                return [names[key] for key in sorted(names)]
