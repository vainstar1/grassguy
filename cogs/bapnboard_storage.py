from __future__ import annotations

import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from .bapnboard_shared import DB_FILE, GLOBAL_BAN_SCOPE, GLOBAL_BIO_KEY, normalize_category


class BoardStorage:
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._ensure_schema()

    @staticmethod
    def _is_player_bans_schema_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return "malformed database schema" in text and "player_bans" in text

    def _repair_player_bans_schema(self, conn: sqlite3.Connection) -> None:
        # Best-effort repair for a broken sqlite_schema entry introduced around player_bans.
        # We only touch the player_bans table and its indexes.
        conn.executescript(
            """
            PRAGMA writable_schema=ON;
            DELETE FROM sqlite_master WHERE type='index' AND name='idx_player_bans_user';
            DELETE FROM sqlite_master WHERE type='index' AND name='sqlite_autoindex_player_bans_1';
            DELETE FROM sqlite_master WHERE type='table' AND name='player_bans';
            PRAGMA writable_schema=OFF;
            """
        )
        row = conn.execute("PRAGMA schema_version").fetchone()
        current_version = int(row[0]) if row else 0
        conn.execute(f"PRAGMA schema_version={current_version + 1}")
        conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError as exc:
            if not self._is_player_bans_schema_error(exc):
                conn.close()
                raise
            self._repair_player_bans_schema(conn)
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
                    pending_timeout_enabled INTEGER DEFAULT 1,
                    anti_farm_enabled INTEGER DEFAULT 1,
                    inactivity_decay_enabled INTEGER DEFAULT 1,
                    inactivity_decay_days INTEGER DEFAULT 7,
                    inactivity_decay_amount REAL DEFAULT 10.0,
                    inactivity_decay_floor REAL DEFAULT 800.0,
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
                CREATE TABLE IF NOT EXISTS player_bans (
                    guild_id INTEGER NOT NULL,
                    scope_category TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    reason TEXT,
                    banned_by INTEGER,
                    banned_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, scope_category, user_id)
                );
                CREATE INDEX IF NOT EXISTS idx_player_bans_user ON player_bans (guild_id, user_id);
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
                CREATE TABLE IF NOT EXISTS match_announcements (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    winner_match_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    PRIMARY KEY (guild_id, category, winner_match_id)
                );
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
                CREATE TABLE IF NOT EXISTS player_decay (
                    guild_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    last_decay_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, category, user_id)
                );
                """
            )
            # Best-effort index rebuild for history lookups.
            try:
                conn.execute("REINDEX idx_matches_lookup")
            except sqlite3.DatabaseError:
                pass
            try:
                conn.execute("ALTER TABLE active_matches ADD COLUMN thread_message_id INTEGER")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leaderboards ADD COLUMN pending_timeout_enabled INTEGER DEFAULT 1")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leaderboards ADD COLUMN anti_farm_enabled INTEGER DEFAULT 1")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leaderboards ADD COLUMN inactivity_decay_enabled INTEGER DEFAULT 1")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leaderboards ADD COLUMN inactivity_decay_days INTEGER DEFAULT 7")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leaderboards ADD COLUMN inactivity_decay_amount REAL DEFAULT 10.0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE leaderboards ADD COLUMN inactivity_decay_floor REAL DEFAULT 800.0")
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
                bans: Dict[str, Dict[str, Dict[int, Dict[str, Any]]]] = {}
                decay_state: Dict[str, Dict[str, Dict[int, str]]] = {}
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
                    row_keys = row.keys()
                    entry = {
                        "name": row["display_name"],
                        "participant_role_id": row["participant_role_id"],
                        "challenge_channel_id": row["challenge_channel_id"],
                        "outgoing_channel_id": row["outgoing_channel_id"],
                        "announce_channel_id": row["announce_channel_id"],
                        "leaderboard_channel_id": row["leaderboard_channel_id"],
                        "leaderboard_message_id": row["leaderboard_message_id"],
                        "pending_timeout_enabled": bool(row["pending_timeout_enabled"]) if "pending_timeout_enabled" in row_keys else True,
                        "anti_farm_enabled": bool(row["anti_farm_enabled"]) if "anti_farm_enabled" in row_keys else True,
                        "inactivity_decay_enabled": bool(row["inactivity_decay_enabled"]) if "inactivity_decay_enabled" in row_keys else True,
                        "inactivity_decay_days": int(row["inactivity_decay_days"]) if "inactivity_decay_days" in row_keys and row["inactivity_decay_days"] is not None else 7,
                        "inactivity_decay_amount": float(row["inactivity_decay_amount"]) if "inactivity_decay_amount" in row_keys and row["inactivity_decay_amount"] is not None else 10.0,
                        "inactivity_decay_floor": float(row["inactivity_decay_floor"]) if "inactivity_decay_floor" in row_keys and row["inactivity_decay_floor"] is not None else 800.0,
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
                for row in conn.execute("SELECT guild_id, scope_category, user_id, reason, banned_by, banned_at FROM player_bans"):
                    gid_s = str(row["guild_id"])
                    scope = row["scope_category"] or GLOBAL_BAN_SCOPE
                    bans.setdefault(gid_s, {}).setdefault(scope, {})[row["user_id"]] = {
                        "reason": row["reason"],
                        "banned_by": row["banned_by"],
                        "banned_at": row["banned_at"],
                    }
                for row in conn.execute("SELECT guild_id, category, user_id, last_decay_at FROM player_decay"):
                    gid_s = str(row["guild_id"])
                    safe = row["category"]
                    decay_state.setdefault(gid_s, {}).setdefault(safe, {})[row["user_id"]] = row["last_decay_at"]
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
                    "bans": bans,
                    "decay_state": decay_state,
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
                                pending_timeout_enabled,
                                anti_farm_enabled,
                                inactivity_decay_enabled,
                                inactivity_decay_days,
                                inactivity_decay_amount,
                                inactivity_decay_floor,
                                thread_cleanup_seconds
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                                1 if board.get("pending_timeout_enabled", True) else 0,
                                1 if board.get("anti_farm_enabled", True) else 0,
                                1 if board.get("inactivity_decay_enabled", True) else 0,
                                max(1, int(board.get("inactivity_decay_days", 7))),
                                max(0.0, float(board.get("inactivity_decay_amount", 10.0))),
                                max(0.0, float(board.get("inactivity_decay_floor", 800.0))),
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

    def save_bans(self, guild_id: int, bans_map: Dict[str, Dict[int, Dict[str, Any]]]) -> None:
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM player_bans WHERE guild_id=?", (guild_id,))
                for raw_scope, users in bans_map.items():
                    if not isinstance(users, dict):
                        continue
                    scope = (
                        GLOBAL_BAN_SCOPE
                        if str(raw_scope) == GLOBAL_BAN_SCOPE
                        else normalize_category(str(raw_scope))
                    )
                    for raw_user_id, payload in users.items():
                        if not isinstance(payload, dict):
                            payload = {}
                        try:
                            user_id = int(raw_user_id)
                        except Exception:
                            continue
                        banned_at = payload.get("banned_at")
                        if not banned_at:
                            continue
                        banned_by = payload.get("banned_by")
                        try:
                            banned_by_value = int(banned_by) if banned_by is not None else None
                        except Exception:
                            banned_by_value = None
                        cur.execute(
                            """
                            INSERT INTO player_bans (guild_id, scope_category, user_id, reason, banned_by, banned_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                guild_id,
                                scope,
                                user_id,
                                payload.get("reason"),
                                banned_by_value,
                                str(banned_at),
                            ),
                        )

    def save_decay_state(self, guild_id: int, category: str, decay_map: Dict[int, str]) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM player_decay WHERE guild_id=? AND category=?", (guild_id, safe))
                for raw_user_id, raw_marker in decay_map.items():
                    try:
                        user_id = int(raw_user_id)
                    except Exception:
                        continue
                    marker = str(raw_marker or "").strip()
                    if not marker:
                        continue
                    cur.execute(
                        """
                        INSERT INTO player_decay (guild_id, category, user_id, last_decay_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (guild_id, safe, user_id, marker),
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
    ) -> int:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
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
                return int(cur.lastrowid)

    def audit_completed_history_integrity(
        self,
        guild_id: int,
        category: str,
        sample_limit: int = 3,
    ) -> Dict[str, Any]:
        safe = normalize_category(category)
        limit = max(1, int(sample_limit))
        with self._lock:
            with self._connect() as conn:
                win_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM matches NOT INDEXED
                        WHERE guild_id=?
                          AND category=?
                          AND result IN ('Win', 'DeclineWin')
                        """,
                        (guild_id, safe),
                    ).fetchone()["c"]
                )
                loss_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM matches NOT INDEXED
                        WHERE guild_id=?
                          AND category=?
                          AND result='Loss'
                        """,
                        (guild_id, safe),
                    ).fetchone()["c"]
                )

                missing_loss_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM matches w NOT INDEXED
                        WHERE w.guild_id=?
                          AND w.category=?
                          AND w.result IN ('Win', 'DeclineWin')
                          AND NOT EXISTS (
                                SELECT 1
                                FROM matches l NOT INDEXED
                                WHERE l.guild_id=w.guild_id
                                  AND l.category=w.category
                                  AND l.recorded_at=w.recorded_at
                                  AND l.user_id=w.opponent_id
                                  AND l.opponent_id=w.user_id
                                  AND l.result='Loss'
                          )
                        """,
                        (guild_id, safe),
                    ).fetchone()["c"]
                )
                missing_win_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM matches l NOT INDEXED
                        WHERE l.guild_id=?
                          AND l.category=?
                          AND l.result='Loss'
                          AND NOT EXISTS (
                                SELECT 1
                                FROM matches w NOT INDEXED
                                WHERE w.guild_id=l.guild_id
                                  AND w.category=l.category
                                  AND w.recorded_at=l.recorded_at
                                  AND w.user_id=l.opponent_id
                                  AND w.opponent_id=l.user_id
                                  AND w.result IN ('Win', 'DeclineWin')
                          )
                        """,
                        (guild_id, safe),
                    ).fetchone()["c"]
                )

                missing_loss_samples: List[Dict[str, Any]] = []
                for row in conn.execute(
                    """
                    SELECT id, recorded_at, user_id, opponent_id, user_value, opponent_value, result
                    FROM matches w NOT INDEXED
                    WHERE w.guild_id=?
                      AND w.category=?
                      AND w.result IN ('Win', 'DeclineWin')
                      AND NOT EXISTS (
                            SELECT 1
                            FROM matches l NOT INDEXED
                            WHERE l.guild_id=w.guild_id
                              AND l.category=w.category
                              AND l.recorded_at=w.recorded_at
                              AND l.user_id=w.opponent_id
                              AND l.opponent_id=w.user_id
                              AND l.result='Loss'
                      )
                    ORDER BY w.recorded_at DESC, w.id DESC
                    LIMIT ?
                    """,
                    (guild_id, safe, limit),
                ):
                    missing_loss_samples.append(
                        {
                            "id": int(row["id"]),
                            "recorded_at": row["recorded_at"],
                            "user_id": int(row["user_id"]),
                            "opponent_id": int(row["opponent_id"]),
                            "user_value": row["user_value"],
                            "opponent_value": row["opponent_value"],
                            "result": row["result"],
                        }
                    )

                missing_win_samples: List[Dict[str, Any]] = []
                for row in conn.execute(
                    """
                    SELECT id, recorded_at, user_id, opponent_id, user_value, opponent_value, result
                    FROM matches l NOT INDEXED
                    WHERE l.guild_id=?
                      AND l.category=?
                      AND l.result='Loss'
                      AND NOT EXISTS (
                            SELECT 1
                            FROM matches w NOT INDEXED
                            WHERE w.guild_id=l.guild_id
                              AND w.category=l.category
                              AND w.recorded_at=l.recorded_at
                              AND w.user_id=l.opponent_id
                              AND w.opponent_id=l.user_id
                              AND w.result IN ('Win', 'DeclineWin')
                      )
                    ORDER BY l.recorded_at DESC, l.id DESC
                    LIMIT ?
                    """,
                    (guild_id, safe, limit),
                ):
                    missing_win_samples.append(
                        {
                            "id": int(row["id"]),
                            "recorded_at": row["recorded_at"],
                            "user_id": int(row["user_id"]),
                            "opponent_id": int(row["opponent_id"]),
                            "user_value": row["user_value"],
                            "opponent_value": row["opponent_value"],
                            "result": row["result"],
                        }
                    )

                orphan_announcement_count = 0
                orphan_announcement_samples: List[int] = []
                try:
                    orphan_announcement_count = int(
                        conn.execute(
                            """
                            SELECT COUNT(*) AS c
                            FROM match_announcements a
                            LEFT JOIN matches w
                              ON w.guild_id=a.guild_id
                             AND w.category=a.category
                             AND w.id=a.winner_match_id
                             AND w.result IN ('Win', 'DeclineWin')
                            WHERE a.guild_id=?
                              AND a.category=?
                              AND w.id IS NULL
                            """,
                            (guild_id, safe),
                        ).fetchone()["c"]
                    )
                    for row in conn.execute(
                        """
                        SELECT a.winner_match_id
                        FROM match_announcements a
                        LEFT JOIN matches w
                          ON w.guild_id=a.guild_id
                         AND w.category=a.category
                         AND w.id=a.winner_match_id
                         AND w.result IN ('Win', 'DeclineWin')
                        WHERE a.guild_id=?
                          AND a.category=?
                          AND w.id IS NULL
                        ORDER BY a.winner_match_id DESC
                        LIMIT ?
                        """,
                        (guild_id, safe, limit),
                    ):
                        orphan_announcement_samples.append(int(row["winner_match_id"]))
                except sqlite3.OperationalError:
                    orphan_announcement_count = 0
                    orphan_announcement_samples = []

                has_issues = (
                    missing_loss_count > 0
                    or missing_win_count > 0
                    or orphan_announcement_count > 0
                )
                return {
                    "has_issues": has_issues,
                    "wins": win_count,
                    "losses": loss_count,
                    "missing_loss_count": missing_loss_count,
                    "missing_win_count": missing_win_count,
                    "orphan_announcement_count": orphan_announcement_count,
                    "missing_loss_samples": missing_loss_samples,
                    "missing_win_samples": missing_win_samples,
                    "orphan_announcement_samples": orphan_announcement_samples,
                }

    def load_raw_match_rows(self, guild_id: int, category: str) -> List[Dict[str, Any]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches NOT INDEXED
                    WHERE guild_id=? AND category=?
                    ORDER BY recorded_at ASC, id ASC
                    """,
                    (guild_id, safe),
                )
                output: List[Dict[str, Any]] = []
                for row in rows:
                    output.append(
                        {
                            "id": int(row["id"]),
                            "user_id": int(row["user_id"]),
                            "recorded_at": row["recorded_at"],
                            "opponent_id": int(row["opponent_id"]),
                            "challenger": bool(row["challenger"]),
                            "user_value": row["user_value"],
                            "opponent_value": row["opponent_value"],
                            "result": row["result"],
                            "elo_change": float(row["elo_change"]),
                        }
                    )
                return output

    def load_recent_completed_matches(
        self,
        guild_id: int,
        category: str,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        w.id AS winner_match_id,
                        l.id AS loser_match_id,
                        w.recorded_at AS recorded_at,
                        w.user_id AS winner_id,
                        w.opponent_id AS loser_id,
                        w.user_value AS winner_value,
                        w.opponent_value AS loser_value
                    FROM matches w NOT INDEXED
                    JOIN matches l NOT INDEXED
                      ON l.guild_id = w.guild_id
                     AND l.category = w.category
                     AND l.recorded_at = w.recorded_at
                     AND l.user_id = w.opponent_id
                     AND l.opponent_id = w.user_id
                     AND l.result = 'Loss'
                    WHERE w.guild_id=?
                      AND w.category=?
                      AND w.result IN ('Win', 'DeclineWin')
                    ORDER BY w.recorded_at DESC, w.id DESC
                    LIMIT ?
                    """,
                    (guild_id, safe, max(1, int(limit))),
                )
                output: List[Dict[str, Any]] = []
                for row in rows:
                    output.append(
                        {
                            "winner_match_id": int(row["winner_match_id"]),
                            "loser_match_id": int(row["loser_match_id"]),
                            "recorded_at": row["recorded_at"],
                            "winner_id": int(row["winner_id"]),
                            "loser_id": int(row["loser_id"]),
                            "winner_value": row["winner_value"],
                            "loser_value": row["loser_value"],
                        }
                    )
                return output

    def load_recent_challenger_opponents(
        self,
        guild_id: int,
        category: str,
        challenger_id: int,
        limit: int = 4,
    ) -> List[int]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT opponent_id
                    FROM matches NOT INDEXED
                    WHERE guild_id=?
                      AND category=?
                      AND user_id=?
                      AND challenger=1
                      AND result IN ('Win', 'DeclineWin', 'Loss')
                    ORDER BY recorded_at DESC, id DESC
                    LIMIT ?
                    """,
                    (guild_id, safe, int(challenger_id), max(1, int(limit))),
                )
                output: List[int] = []
                for row in rows:
                    output.append(int(row["opponent_id"]))
                return output

    def load_recent_pair_matches(
        self,
        guild_id: int,
        category: str,
        player_a: int,
        player_b: int,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        w.id AS winner_match_id,
                        (
                            SELECT l.id
                            FROM matches l NOT INDEXED
                            WHERE l.guild_id = w.guild_id
                              AND l.category = w.category
                              AND l.recorded_at = w.recorded_at
                              AND l.user_id = w.opponent_id
                              AND l.opponent_id = w.user_id
                              AND l.result = 'Loss'
                            ORDER BY l.id ASC
                            LIMIT 1
                        ) AS loser_match_id,
                        w.recorded_at AS recorded_at,
                        w.user_id AS winner_id,
                        w.opponent_id AS loser_id,
                        w.user_value AS winner_value,
                        w.opponent_value AS loser_value,
                        w.elo_change AS winner_elo_change,
                        COALESCE(
                            (
                                SELECT l.elo_change
                                FROM matches l NOT INDEXED
                                WHERE l.guild_id = w.guild_id
                                  AND l.category = w.category
                                  AND l.recorded_at = w.recorded_at
                                  AND l.user_id = w.opponent_id
                                  AND l.opponent_id = w.user_id
                                  AND l.result = 'Loss'
                                ORDER BY l.id ASC
                                LIMIT 1
                            ),
                            -w.elo_change
                        ) AS loser_elo_change
                    FROM matches w NOT INDEXED
                    WHERE w.guild_id=?
                      AND w.category=?
                      AND w.result IN ('Win', 'DeclineWin')
                      AND (
                            (w.user_id=? AND w.opponent_id=?)
                            OR
                            (w.user_id=? AND w.opponent_id=?)
                          )
                    ORDER BY w.recorded_at DESC, w.id DESC
                    LIMIT ?
                    """,
                    (guild_id, safe, player_a, player_b, player_b, player_a, max(1, int(limit))),
                )
                output: List[Dict[str, Any]] = []
                for row in rows:
                    loser_match_id = row["loser_match_id"]
                    output.append(
                        {
                            "winner_match_id": int(row["winner_match_id"]),
                            "loser_match_id": int(loser_match_id) if loser_match_id is not None else None,
                            "recorded_at": row["recorded_at"],
                            "winner_id": int(row["winner_id"]),
                            "loser_id": int(row["loser_id"]),
                            "winner_value": row["winner_value"],
                            "loser_value": row["loser_value"],
                            "winner_elo_change": float(row["winner_elo_change"]),
                            "loser_elo_change": float(row["loser_elo_change"]),
                        }
                    )
                return output

    def load_match_pair_by_winner_row_id(
        self,
        guild_id: int,
        category: str,
        winner_row_id: int,
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                winner_row = conn.execute(
                    """
                    SELECT id, user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches NOT INDEXED
                    WHERE guild_id=? AND category=? AND id=? AND result='Win'
                    """,
                    (guild_id, safe, int(winner_row_id)),
                ).fetchone()
                if winner_row is None:
                    return None
                loser_row = conn.execute(
                    """
                    SELECT id, user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches NOT INDEXED
                    WHERE guild_id=?
                      AND category=?
                      AND recorded_at=?
                      AND user_id=?
                      AND opponent_id=?
                      AND result='Loss'
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (
                        guild_id,
                        safe,
                        winner_row["recorded_at"],
                        int(winner_row["opponent_id"]),
                        int(winner_row["user_id"]),
                    ),
                ).fetchone()
                if loser_row is None:
                    return None
                return {
                    "winner": {
                        "id": int(winner_row["id"]),
                        "user_id": int(winner_row["user_id"]),
                        "recorded_at": winner_row["recorded_at"],
                        "opponent_id": int(winner_row["opponent_id"]),
                        "challenger": bool(winner_row["challenger"]),
                        "user_value": winner_row["user_value"],
                        "opponent_value": winner_row["opponent_value"],
                        "result": winner_row["result"],
                        "elo_change": float(winner_row["elo_change"]),
                    },
                    "loser": {
                        "id": int(loser_row["id"]),
                        "user_id": int(loser_row["user_id"]),
                        "recorded_at": loser_row["recorded_at"],
                        "opponent_id": int(loser_row["opponent_id"]),
                        "challenger": bool(loser_row["challenger"]),
                        "user_value": loser_row["user_value"],
                        "opponent_value": loser_row["opponent_value"],
                        "result": loser_row["result"],
                        "elo_change": float(loser_row["elo_change"]),
                    },
                }

    def delete_match_pair_by_winner_row_id(
        self,
        guild_id: int,
        category: str,
        winner_row_id: int,
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                winner_row = conn.execute(
                    """
                    SELECT id, user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches NOT INDEXED
                    WHERE guild_id=? AND category=? AND id=? AND result='Win'
                    """,
                    (guild_id, safe, int(winner_row_id)),
                ).fetchone()
                if winner_row is None:
                    return None
                loser_row = conn.execute(
                    """
                    SELECT id, user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches NOT INDEXED
                    WHERE guild_id=?
                      AND category=?
                      AND recorded_at=?
                      AND user_id=?
                      AND opponent_id=?
                      AND result='Loss'
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (
                        guild_id,
                        safe,
                        winner_row["recorded_at"],
                        int(winner_row["opponent_id"]),
                        int(winner_row["user_id"]),
                    ),
                ).fetchone()
                if loser_row is None:
                    return None

                output = {
                    "winner": {
                        "id": int(winner_row["id"]),
                        "user_id": int(winner_row["user_id"]),
                        "recorded_at": winner_row["recorded_at"],
                        "opponent_id": int(winner_row["opponent_id"]),
                        "challenger": bool(winner_row["challenger"]),
                        "user_value": winner_row["user_value"],
                        "opponent_value": winner_row["opponent_value"],
                        "result": winner_row["result"],
                        "elo_change": float(winner_row["elo_change"]),
                    },
                    "loser": {
                        "id": int(loser_row["id"]),
                        "user_id": int(loser_row["user_id"]),
                        "recorded_at": loser_row["recorded_at"],
                        "opponent_id": int(loser_row["opponent_id"]),
                        "challenger": bool(loser_row["challenger"]),
                        "user_value": loser_row["user_value"],
                        "opponent_value": loser_row["opponent_value"],
                        "result": loser_row["result"],
                        "elo_change": float(loser_row["elo_change"]),
                    },
                }
                conn.execute(
                    "DELETE FROM matches WHERE guild_id=? AND category=? AND id IN (?, ?)",
                    (guild_id, safe, int(winner_row["id"]), int(loser_row["id"])),
                )
                return output

    def update_match_rows(self, guild_id: int, category: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                cur = conn.cursor()
                for payload in rows:
                    try:
                        row_id = int(payload["id"])
                        user_id = int(payload["user_id"])
                        opponent_id = int(payload["opponent_id"])
                    except Exception:
                        continue
                    cur.execute(
                        """
                        UPDATE matches
                        SET
                            user_id=?,
                            opponent_id=?,
                            challenger=?,
                            user_value=?,
                            opponent_value=?,
                            result=?,
                            elo_change=?
                        WHERE guild_id=? AND category=? AND id=?
                        """,
                        (
                            user_id,
                            opponent_id,
                            1 if payload.get("challenger") else 0,
                            payload.get("user_value"),
                            payload.get("opponent_value"),
                            payload.get("result"),
                            float(payload.get("elo_change", 0.0)),
                            guild_id,
                            safe,
                            row_id,
                        ),
                    )

    def save_match_announcement(self, guild_id: int, category: str, winner_match_id: int, channel_id: int, message_id: int) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO match_announcements (guild_id, category, winner_match_id, channel_id, message_id)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id, category, winner_match_id) DO UPDATE SET
                        channel_id=excluded.channel_id,
                        message_id=excluded.message_id
                    """,
                    (guild_id, safe, int(winner_match_id), int(channel_id), int(message_id)),
                )

    def get_match_announcement(self, guild_id: int, category: str, winner_match_id: int) -> Optional[Dict[str, int]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT channel_id, message_id
                    FROM match_announcements
                    WHERE guild_id=? AND category=? AND winner_match_id=?
                    """,
                    (guild_id, safe, int(winner_match_id)),
                ).fetchone()
                if row is None:
                    return None
                return {"channel_id": int(row["channel_id"]), "message_id": int(row["message_id"])}

    def delete_match_announcement(self, guild_id: int, category: str, winner_match_id: int) -> None:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM match_announcements WHERE guild_id=? AND category=? AND winner_match_id=?",
                    (guild_id, safe, int(winner_match_id)),
                )

    def load_match_history(self, guild_id: int, category: str) -> List[Dict[str, Any]]:
        safe = normalize_category(category)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT user_id, recorded_at, opponent_id, challenger, user_value, opponent_value, result, elo_change
                    FROM matches NOT INDEXED
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
                    "SELECT COUNT(*) AS c FROM matches NOT INDEXED WHERE guild_id=? AND category=? AND user_id=?",
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
                    "UPDATE player_bans SET scope_category=? WHERE guild_id=? AND scope_category=?",
                    (new_safe, guild_id, old_safe),
                )
                cur.execute(
                    "UPDATE player_decay SET category=? WHERE guild_id=? AND category=?",
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
                    "UPDATE match_announcements SET category=? WHERE guild_id=? AND category=?",
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
                    "DELETE FROM player_bans WHERE guild_id=? AND scope_category=?",
                    (guild_id, safe),
                )
                cur.execute(
                    "DELETE FROM player_decay WHERE guild_id=? AND category=?",
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
                    "DELETE FROM match_announcements WHERE guild_id=? AND category=?",
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
                for row in conn.execute("SELECT DISTINCT category FROM matches NOT INDEXED WHERE guild_id=?", (guild_id,)):
                    value = self._display_from_safe(row["category"])
                    names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM removed_players WHERE guild_id=?", (guild_id,)):
                    safe = row["category"]
                    if safe == GLOBAL_BIO_KEY:
                        continue
                    value = self._display_from_safe(safe)
                    names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT scope_category FROM player_bans WHERE guild_id=?", (guild_id,)):
                    safe = row["scope_category"]
                    if safe in {GLOBAL_BIO_KEY, GLOBAL_BAN_SCOPE}:
                        continue
                    value = self._display_from_safe(safe)
                    names.setdefault(value.lower(), value)
                for row in conn.execute("SELECT DISTINCT category FROM player_decay WHERE guild_id=?", (guild_id,)):
                    safe = row["category"]
                    if not safe:
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
