import asyncio
import copy
import functools
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from .bapnboard_shared import (
    DB_FILE,
    DEFAULT_START_ELO,
    GLOBAL_BIO_KEY,
    MODE_TYPE_CHOICES,
    MODES,
    PENDING_CHALLENGE_TIMEOUT,
    TZ,
    chunk_list,
    ensure_dirs,
    logger,
    normalize_category,
)
from .bapnboard_storage import BoardStorage
from .bapnboard_views import ChallengeControlView, PagedListView, ProfileView


async def category_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []
    gid = interaction.guild.id
    cog = interaction.client.get_cog("LeaderboardCog")
    mapping = {}
    if cog:
        cfg = cog.guild_configs.get(str(gid), {})
        leaderboards = cfg.get("leaderboards", {})
        if not leaderboards and cfg.get("categories"):
            for name in cfg.get("categories", []):
                mapping[name.lower()] = name
        else:
            for entry in leaderboards.values():
                name = entry.get("name")
                if name:
                    mapping[name.lower()] = name
        try:
            category_names = await asyncio.to_thread(cog.storage.list_categories, gid)
        except Exception:
            category_names = []
        for name in category_names:
            lowered = name.lower()
            if lowered not in mapping and normalize_category(name) != GLOBAL_BIO_KEY:
                mapping[lowered] = name
    lowered_current = current.lower()
    choices = [
        value
        for _, value in sorted(mapping.items(), key=lambda x: x[1].lower())
        if lowered_current in value.lower() and normalize_category(value) != GLOBAL_BIO_KEY
    ]
    return [app_commands.Choice(name=c, value=c) for c in choices[:25]]

async def member_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []
    choices = []
    lowered = current.lower()
    for m in interaction.guild.members:
        if lowered in m.display_name.lower():
            choices.append(app_commands.Choice(name=m.display_name, value=str(m.id)))
            if len(choices) >= 25:
                break
    return choices[:25]

async def removed_player_autocomplete(interaction: discord.Interaction, current: str):
    cog = interaction.client.get_cog("LeaderboardCog")
    if cog is None:
        return []
    return await cog.removed_autocomplete(interaction, current)

async def scope_autocomplete(interaction: discord.Interaction, current: str):
    options = ["All", "Personal"]
    lowered = current.lower()
    matches = [opt for opt in options if opt.lower().startswith(lowered)]
    if not matches:
        matches = options
    return [app_commands.Choice(name=opt, value=opt.lower()) for opt in matches[:25]]

class LeaderboardCog(commands.Cog):
    leaderboard = app_commands.Group(name="leaderboard", description="Leaderboard commands", guild_only=True)

    challenge_group = app_commands.Group(name="challenge", description="Challenge commands", parent=leaderboard)

    def __init__(self, client: commands.Bot):
        self.client = client
        ensure_dirs()
        self.storage = BoardStorage(DB_FILE)
        self.guild_configs = {}
        self._cleanup_task = None
        self._active_fight_save_tasks = {}
        self._config_save_task = None
        self._config_save_pending = False
        self.players_data = {}
        self.players_meta = {}
        self.active_fights = {}
        self.bios = {}
        self.removed = {}
        self.load_all()

    def cog_unload(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            self._cleanup_task = None
        for task in self._active_fight_save_tasks.values():
            task.cancel()
        self._active_fight_save_tasks.clear()
        if self._config_save_task:
            self._config_save_task.cancel()
            self._config_save_task = None
        self._config_save_pending = False

    def load_all(self):
        if not hasattr(self, "_config_save_task"):
            self._config_save_task = None
            self._config_save_pending = False
        if not hasattr(self, "_active_fight_save_tasks"):
            self._active_fight_save_tasks = {}
        snapshot = self.storage.load_all()
        self.guild_configs = snapshot["guild_configs"]
        self.players_data = snapshot["players"]
        self.players_meta = snapshot["players_meta"]
        self.removed = snapshot["removed"]
        self.bios = snapshot["bios"]
        self.active_fights = snapshot["active_fights"]
        changed_any = False
        for gid_s, data in list(self.guild_configs.items()):
            try:
                gid = int(gid_s)
            except Exception:
                continue
            if not isinstance(data, dict):
                data = {}
                self.guild_configs[gid_s] = data
            data.setdefault("leaderboards", {})
            data.setdefault("category_modes", {})
            converted_modes = {}
            for safe_cat, stored in data["category_modes"].items():
                info = self.normalize_mode_value(stored)
                converted_modes[safe_cat] = {"key": info["key"], "target": info["target"]}
            data["category_modes"] = converted_modes
            if not data["leaderboards"] and data.get("categories"):
                for name in data.get("categories", []):
                    safe_name = normalize_category(name)
                    mode_info = converted_modes.get(safe_name, {"key": "speedrun", "target": None})
                    data["leaderboards"][safe_name] = {
                        "name": name,
                        "participant_role_id": data.get("participant_role_id"),
                        "challenge_channel_id": data.get("challenge_channel_id"),
                        "outgoing_channel_id": data.get("outgoing_channel_id"),
                        "announce_channel_id": data.get("announce_channel_id"),
                        "leaderboard_channel_id": data.get("leaderboard_channel_id"),
                        "leaderboard_message_id": data.get("leaderboard_message_id"),
                        "thread_cleanup_seconds": int(data.get("thread_cleanup_seconds", 21600)),
                        "mode": {"key": mode_info["key"], "target": mode_info["target"]},
                    }
                if data.get("categories"):
                    changed_any = True
            for safe_name, entry in list(data["leaderboards"].items()):
                if not isinstance(entry, dict):
                    entry = {}
                    data["leaderboards"][safe_name] = entry
                display_name = entry.get("name") or safe_name.replace("_", " ").title()
                if entry.get("name") != display_name:
                    entry["name"] = display_name
                    changed_any = True
                entry["participant_role_id"] = entry.get("participant_role_id") or data.get("participant_role_id")
                entry["challenge_channel_id"] = entry.get("challenge_channel_id") or data.get("challenge_channel_id")
                entry["outgoing_channel_id"] = entry.get("outgoing_channel_id")
                entry["announce_channel_id"] = entry.get("announce_channel_id") or data.get("announce_channel_id")
                entry["leaderboard_channel_id"] = entry.get("leaderboard_channel_id") or data.get("leaderboard_channel_id")
                entry["leaderboard_message_id"] = entry.get("leaderboard_message_id") or data.get("leaderboard_message_id")
                entry["thread_cleanup_seconds"] = int(entry.get("thread_cleanup_seconds", data.get("thread_cleanup_seconds", 21600)))
                mode_source = entry.get("mode") or converted_modes.get(safe_name) or {"key": "speedrun", "target": None}
                normalized_mode = self.normalize_mode_value(mode_source)
                mode_payload = {"key": normalized_mode["key"], "target": normalized_mode["target"]}
                if entry.get("mode") != mode_payload:
                    entry["mode"] = mode_payload
                    changed_any = True
        for gid_s in list(self.guild_configs.keys()):
            self.players_data.setdefault(gid_s, {})
            self.players_meta.setdefault(gid_s, {})
            self.removed.setdefault(gid_s, {})
            self.bios.setdefault(gid_s, {})
            self.active_fights.setdefault(gid_s, {})
        for gid_s, active_map in list(self.active_fights.items()):
            if not isinstance(active_map, dict):
                self.active_fights[gid_s] = {}
                continue
            for category, entry in list(active_map.items()):
                if not isinstance(entry, dict):
                    active_map[category] = {"matches": {}, "deletions": []}
                    changed_any = True
                    continue
                matches = entry.get("matches")
                if not isinstance(matches, dict):
                    matches = {}
                    changed_any = True
                else:
                    for match_id, payload in list(matches.items()):
                        if not isinstance(payload, dict):
                            matches.pop(match_id, None)
                            changed_any = True
                            continue
                        payload.setdefault("status", "open")
                        payload.setdefault("cancel_votes", [])
                        payload.setdefault("submissions", {})
                        payload.setdefault("created_at", datetime.now(timezone.utc).isoformat())
                        payload.setdefault("channel_id", None)
                        payload.setdefault("message_id", None)
                        payload.setdefault("thread_id", None)
                        payload.setdefault("leaderboard", normalize_category(category))
                deletions = entry.get("deletions")
                if not isinstance(deletions, list):
                    deletions = []
                    changed_any = True
                active_map[category] = {"matches": matches, "deletions": deletions}
        if changed_any:
            self._schedule_config_save()

    async def cog_check(self, interaction: discord.Interaction):
        if interaction.guild is None:
            raise app_commands.CheckFailure("Server-only commands")
        return True

    def parse_time(self, s: str) -> Optional[float]:
        try:
            s = s.strip()
            if ":" in s:
                parts = s.split(":")
                if len(parts) == 2:
                    m = int(parts[0])
                    sec = float(parts[1])
                    return m * 60.0 + sec
                return None
            return float(s)
        except Exception:
            return None

    def format_time_value(self, seconds: float) -> str:
        minutes = int(seconds // 60)
        secs = seconds % 60
        if minutes:
            return f"{minutes}:{secs:06.3f}"
        return f"{secs:.3f}"

    def parse_score(self, s: str) -> Optional[int]:
        try:
            value = int(s.strip())
            if value < 0:
                return None
            return value
        except Exception:
            return None

    def load_players_for(self, gid: int, category: str):
        gid_s = str(gid)
        safe_cat = normalize_category(category)
        if gid_s not in self.players_data:
            self.players_data[gid_s] = {}
        if safe_cat not in self.players_data[gid_s]:
            self.players_data[gid_s][safe_cat] = self.storage.load_players(gid, category)
        return self.players_data[gid_s][safe_cat]

    def get_player_rank(self, gid: int, category: str, user_id: int) -> Optional[Tuple[int, Dict[str, Any]]]:
        players = self.load_players_for(gid, category)
        safe = normalize_category(category)
        removed_map = self.removed.get(str(gid), {}).get(safe, {})
        eligible = [(uid, data) for uid, data in players.items() if uid not in removed_map]
        ranked = sorted(eligible, key=lambda item: item[1]["elo"], reverse=True)
        for index, (uid, data) in enumerate(ranked, start=1):
            if uid == user_id:
                return index, data
        return None

    def is_participant(self, member: discord.Member, board_cfg: Dict[str, Any]) -> bool:
        role_id = board_cfg.get("participant_role_id")
        if not role_id or member is None:
            return True
        return any(role.id == role_id for role in getattr(member, "roles", []))

    async def save_players_for(self, gid: int, category: str):
        gid_s = str(gid)
        safe_cat = normalize_category(category)
        players_snapshot = copy.deepcopy(self.players_data.get(gid_s, {}).get(safe_cat, {}))
        meta_snapshot = copy.deepcopy(self.players_meta.get(gid_s, {}))
        removed_snapshot = copy.deepcopy(self.removed.get(gid_s, {}).get(safe_cat, {}))
        bios_snapshot = copy.deepcopy(self.bios.get(gid_s, {}).get(safe_cat, {}))
        await asyncio.gather(
            asyncio.to_thread(self.storage.save_players, gid, category, players_snapshot),
            asyncio.to_thread(self.storage.save_player_meta, gid, meta_snapshot),
            asyncio.to_thread(self.storage.save_removed, gid, category, removed_snapshot),
            asyncio.to_thread(self.storage.save_bios, gid, category, bios_snapshot),
        )
        await self.save_active_fights_for(gid)

    async def persist_player_meta(self, guild_id: int):
        gid_s = str(guild_id)
        meta_snapshot = copy.deepcopy(self.players_meta.get(gid_s, {}))
        await asyncio.to_thread(self.storage.save_player_meta, guild_id, meta_snapshot)

    async def save_match_for(self, gid: int, category: str, user_id: int, date: datetime, opponent_id: int, challenger: bool, time_user, time_opp, result: str, elo_change: float = 0.0):
        if isinstance(time_user, str):
            t_user_field = time_user
        else:
            try:
                t_user_field = f"{float(time_user):.3f}"
            except Exception:
                t_user_field = "0.000"
        if isinstance(time_opp, str):
            t_opp_field = time_opp
        else:
            try:
                t_opp_field = f"{float(time_opp):.3f}"
            except Exception:
                t_opp_field = "0.000"
        await asyncio.to_thread(
            self.storage.append_match,
            gid,
            category,
            user_id,
            date.isoformat(),
            opponent_id,
            challenger,
            t_user_field,
            t_opp_field,
            result,
            elo_change,
        )

    def format_match_entry(self, gid: int, category: str, row: Dict[str, Any], perspective_id: Optional[int] = None, include_category: bool = False) -> Optional[Tuple[datetime, str]]:
        try:
            user_id = int(row["user_id"])
            opponent_id = int(row["opponent_id"])
        except Exception:
            return None
        if perspective_id is not None and user_id != perspective_id:
            return None
        mode_info = self.get_category_mode(gid, category)
        value_self = row.get("time", "?")
        value_opp = row.get("opponent_time", "?")
        if mode_info["type"] == "score":
            detail = f"{value_self}-{value_opp}"
        else:
            detail = f"{value_self} vs {value_opp}"
        try:
            recorded_at = datetime.fromisoformat(row.get("date", ""))
            if recorded_at.tzinfo is None:
                recorded_at = recorded_at.replace(tzinfo=timezone.utc)
        except Exception:
            recorded_at = datetime.now(timezone.utc)
        stamp = recorded_at.astimezone(TZ).strftime("%m/%d/%Y %H:%M")
        token = row.get("result", "")
        change_raw = row.get("elo_change")
        try:
            elo_change = float(change_raw) if change_raw is not None else 0.0
        except (TypeError, ValueError):
            elo_change = 0.0
        change_text = f" ({self.format_elo_delta(elo_change)})" if abs(elo_change) > 0.0001 else ""
        if perspective_id is not None:
            opponent = self.client.get_user(opponent_id)
            opp_name = opponent.display_name if opponent else self.user_snapshot_name_for(gid, opponent_id)
            me_name = self.user_snapshot_name_for(gid, perspective_id)
            if token in ["Win", "DeclineWin"]:
                outcome = "Win"
            elif token == "Draw":
                outcome = "Draw"
            else:
                outcome = "Loss"
            line = f"{stamp} - {me_name} vs {opp_name}: {outcome} {detail}{change_text}"
            return recorded_at, line
        winner = self.client.get_user(user_id)
        winner_name = winner.display_name if winner else self.user_snapshot_name_for(gid, user_id)
        loser = self.client.get_user(opponent_id)
        loser_name = loser.display_name if loser else self.user_snapshot_name_for(gid, opponent_id)
        if token == "Draw":
            verb = "drew with"
        else:
            verb = "defeated"
        prefix = f"[{category}] " if include_category else ""
        line = f"{stamp} - {prefix}{winner_name} {verb} {loser_name} ({detail}){change_text}"
        return recorded_at, line

    def get_gconfig(self, gid: int):
        return self.guild_configs.get(str(gid), {})

    def ensure_gconfig(self, gid: int):
        gid_s = str(gid)
        if gid_s not in self.guild_configs:
            self.guild_configs[gid_s] = {"leaderboards": {}, "category_modes": {}}
            self._schedule_config_save()
        else:
            self.guild_configs[gid_s].setdefault("leaderboards", {})
            self.guild_configs[gid_s].setdefault("category_modes", {})

    def get_leaderboard_config(self, gid: int, category: str) -> Dict[str, Any]:
        gid_s = str(gid)
        data = self.guild_configs.get(gid_s, {})
        safe = normalize_category(category)
        boards = data.get("leaderboards", {})
        board = boards.get(safe)
        if board:
            return board
        legacy_categories = data.get("categories", [])
        if any(entry.lower() == category.lower() for entry in legacy_categories):
            template = {
                "name": category,
                "participant_role_id": data.get("participant_role_id"),
                "challenge_channel_id": data.get("challenge_channel_id"),
                "outgoing_channel_id": data.get("outgoing_channel_id"),
                "announce_channel_id": data.get("announce_channel_id"),
                "leaderboard_channel_id": data.get("leaderboard_channel_id"),
                "leaderboard_message_id": data.get("leaderboard_message_id"),
                "thread_cleanup_seconds": int(data.get("thread_cleanup_seconds", 21600)),
            }
            mode_map = data.get("category_modes", {})
            mode_info = self.normalize_mode_value(mode_map.get(safe))
            template["mode"] = {"key": mode_info["key"], "target": mode_info["target"]}
            boards[safe] = template
            data["leaderboards"] = boards
            self._schedule_config_save()
            return template
        return {}

    def upsert_leaderboard_config(self, gid: int, category: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_gconfig(gid)
        gid_s = str(gid)
        safe = normalize_category(category)
        boards = self.guild_configs[gid_s]["leaderboards"]
        existing = boards.get(safe, {})
        merged = dict(existing)
        merged.update(payload)
        merged["name"] = payload.get("name", existing.get("name", category))
        boards[safe] = merged
        self._schedule_config_save()
        return merged

    def list_leaderboards(self, gid: int) -> List[str]:
        data = self.guild_configs.get(str(gid), {})
        boards = data.get("leaderboards", {})
        names = []
        for entry in boards.values():
            name = entry.get("name")
            if name:
                names.append(name)
        if not names and data.get("categories"):
            names.extend(data.get("categories", []))
        return sorted(set(names), key=lambda value: value.lower())

    def get_active_bucket(self, gid: int, category: str) -> Dict[str, Any]:
        gid_s = str(gid)
        cat_map = self.active_fights.setdefault(gid_s, {})
        bucket = cat_map.get(category)
        if not isinstance(bucket, dict):
            bucket = {"matches": {}, "deletions": []}
            cat_map[category] = bucket
        bucket.setdefault("matches", {})
        bucket.setdefault("deletions", [])
        return bucket

    def get_match(self, gid: int, category: str, match_id: str) -> Optional[Dict[str, Any]]:
        bucket = self.get_active_bucket(gid, category)
        return bucket["matches"].get(match_id)

    def upsert_match(self, gid: int, category: str, match_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        bucket = self.get_active_bucket(gid, category)
        existing = bucket["matches"].get(match_id, {})
        merged = dict(existing)
        for key, value in payload.items():
            merged[key] = value
        merged["id"] = match_id
        merged.setdefault("leaderboard", normalize_category(category))
        merged.setdefault("submissions", {})
        merged.setdefault("cancel_votes", [])
        merged.setdefault("status", "open")
        merged.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        bucket["matches"][match_id] = merged
        self._schedule_active_fights_save(gid)
        return merged

    async def delete_match(self, gid: int, category: str, match_id: str):
        bucket = self.get_active_bucket(gid, category)
        if match_id in bucket["matches"]:
            bucket["matches"].pop(match_id)
            await self.save_active_fights_for(gid)

    def find_active_match_for(self, gid: int, user_id: int, exclude: Optional[str] = None) -> Optional[Tuple[str, str, Dict[str, Any]]]:
        gid_s = str(gid)
        cat_map = self.active_fights.get(gid_s, {})
        for category, bucket in cat_map.items():
            matches = bucket.get("matches", {})
            for match_id, data in matches.items():
                if exclude and match_id == exclude:
                    continue
                status = data.get("status", "open")
                if status in {"completed", "cancelled"}:
                    continue
                challenger = data.get("challenger_id")
                opponent = data.get("opponent_id")
                participants = {challenger, opponent}
                if user_id in participants:
                    return category, match_id, data
                if opponent is None and challenger == user_id:
                    return category, match_id, data
        return None

    def has_mod_permissions(self, member: discord.Member) -> bool:
        perms = member.guild_permissions
        return perms.administrator or perms.manage_guild or perms.manage_roles or perms.manage_channels or perms.manage_messages

    def find_match_between(self, gid: int, category: str, player_a: int, player_b: int) -> Optional[Tuple[str, Dict[str, Any]]]:
        bucket = self.get_active_bucket(gid, category)
        matches = bucket.get("matches", {})
        for match_id, data in matches.items():
            participants = {data.get("challenger_id"), data.get("opponent_id")}
            if participants == {player_a, player_b}:
                return match_id, data
        return None

    def normalize_mode_value(self, value: Any) -> Dict[str, Any]:
        key = "speedrun"
        target = None
        if isinstance(value, dict):
            key = value.get("key", "speedrun")
            target = value.get("target")
        elif isinstance(value, str):
            lowered = value.lower()
            if lowered.startswith("ft"):
                key = "score"
                digits = "".join(ch for ch in lowered if ch.isdigit())
                if digits:
                    try:
                        target = int(digits)
                    except Exception:
                        target = None
            else:
                key = lowered
        if key not in MODES:
            key = "speedrun"
        template = MODES[key]
        if template["type"] == "score":
            default_target = template.get("default_target", 1)
            try:
                target = int(target)
            except Exception:
                target = default_target
            target = max(1, target)
        else:
            target = None
        return {
            "key": key,
            "type": template["type"],
            "label": template["label"],
            "target": target,
        }

    def get_category_mode(self, gid: int, category: str) -> Dict[str, Any]:
        gid_s = str(gid)
        safe_cat = normalize_category(category)
        data = self.guild_configs.get(gid_s, {})
        board = data.get("leaderboards", {}).get(safe_cat)
        if board:
            info = self.normalize_mode_value(board.get("mode"))
            board["mode"] = {"key": info["key"], "target": info["target"]}
        else:
            modes = data.get("category_modes", {})
            info = self.normalize_mode_value(modes.get(safe_cat))
        self.guild_configs.setdefault(gid_s, {}).setdefault("category_modes", {})[safe_cat] = {"key": info["key"], "target": info["target"]}
        return info

    def set_category_mode(self, gid: int, category: str, key: str, target: Optional[int] = None) -> Dict[str, Any]:
        gid_s = str(gid)
        safe_cat = normalize_category(category)
        self.ensure_gconfig(gid)
        info = self.normalize_mode_value({"key": key, "target": target})
        boards = self.guild_configs[gid_s].setdefault("leaderboards", {})
        board = boards.get(safe_cat)
        if not board:
            board = {"name": category}
        board["mode"] = {"key": info["key"], "target": info["target"]}
        boards[safe_cat] = board
        self.guild_configs[gid_s].setdefault("category_modes", {})[safe_cat] = {"key": info["key"], "target": info["target"]}
        self._schedule_config_save()
        return info

    def mode_label(self, mode_info: Any) -> str:
        if isinstance(mode_info, dict):
            if mode_info.get("type") == "score":
                return f"First to {mode_info.get('target', 1)}"
            return "Speedrun"
        entry = MODES.get(str(mode_info))
        if entry and entry["type"] == "score":
            default_target = entry.get("default_target", 1)
            return f"First to {default_target}"
        return entry["label"] if entry else "Speedrun"

    def user_snapshot_name_for(self, gid: int, uid: int):
        meta = self.players_meta.get(str(gid), {})
        if str(uid) in meta and "name" in meta[str(uid)]:
            return meta[str(uid)]["name"]
        u = self.client.get_user(uid)
        if u:
            return u.display_name
        return f"User {uid}"

    def user_snapshot_avatar_for(self, gid: int, uid: int):
        meta = self.players_meta.get(str(gid), {})
        if str(uid) in meta and "avatar" in meta[str(uid)]:
            return meta[str(uid)]["avatar"]
        u = self.client.get_user(uid)
        if u and u.avatar:
            return u.avatar.url
        return None

    def compute_elo_change(self, mode_info: Any, elo_w: float, elo_l: float, winner_metric: float, loser_metric: float) -> float:
        info = self.normalize_mode_value(mode_info)
        expected = 1.0 / (1.0 + 10 ** ((elo_l - elo_w) / 400.0))
        if info["type"] == "time":
            margin = max(0.0, loser_metric - winner_metric)
            winner_time = max(winner_metric, 1e-3)
            relative_margin = margin / winner_time
            relative_scale = min(relative_margin / 0.10, 1.0)
            absolute_scale = min(margin / 45.0, 1.0)
            margin_scale = max(relative_scale, absolute_scale)
            length_scale = min(180.0 / max(winner_time, 180.0), 1.0)
            margin_multiplier = (0.2 + 0.8 * margin_scale) * (0.6 + 0.4 * length_scale)
            base_k = 30.0
        else:
            target = max(1.0, float(info.get("target") or 1))
            diff = max(0.0, winner_metric - loser_metric)
            relative_scale = min(diff / max(target, 1.0), 1.0)
            absolute_scale = min(diff / max(target * 0.5, 1.0), 1.0)
            margin_scale = max(relative_scale, absolute_scale)
            margin_multiplier = 0.25 + 0.75 * margin_scale
            base_k = 26.0
        if expected < 0.5:
            swing_factor = 1.0 + (0.5 - expected) * 3.0
        else:
            swing_factor = 1.0 - (expected - 0.5) * 0.8
        swing_factor = max(0.35, min(swing_factor, 2.75))
        adjusted_k = base_k * margin_multiplier * swing_factor
        return adjusted_k * (1.0 - expected)

    @staticmethod
    def format_elo_delta(delta: float) -> str:
        rounded = round(delta)
        if abs(delta - rounded) < 0.05:
            return f"{int(rounded):+d}"
        return f"{delta:+.1f}"

    async def update_leaderboard_message_for(self, gid: int, category: str):
        board_cfg = self.get_leaderboard_config(gid, category)
        if not board_cfg:
            return
        ch_id = board_cfg.get("leaderboard_channel_id")
        msg_id = board_cfg.get("leaderboard_message_id")
        if not ch_id or not msg_id:
            return
        ch = self.client.get_channel(ch_id)
        if not ch:
            return
        try:
            msg = await ch.fetch_message(msg_id)
            view = self.build_leaderboard_view(gid, category)
            await msg.edit(embed=view.create_embed(), view=view)
            self.client.add_view(view, message_id=msg_id)
        except Exception:
            logger.exception("Failed updating leaderboard message for guild %s category %s", gid, category)

    def build_leaderboard_view(self, gid: int, category: str) -> PagedListView:
        players = self.load_players_for(gid, category)
        removed_map = self.removed.get(str(gid), {}).get(normalize_category(category), {})
        removed_ids = set(removed_map.keys())
        sorted_players = sorted(((uid, data) for uid, data in players.items() if uid not in removed_ids), key=lambda x: x[1]["elo"], reverse=True)
        lines = []
        medal_map = {1: "🥇", 2: "🥈", 3: "🥉"}
        for i, (uid, data) in enumerate(sorted_players, start=1):
            user = self.client.get_user(uid)
            name = user.display_name if user else self.user_snapshot_name_for(gid, uid)
            total = data["wins"] + data["losses"]
            win_pct = data["wins"] / total * 100 if total > 0 else 0.0
            prefix = medal_map.get(i, f"{i}.")
            lines.append(f"{prefix} **{name}** - Elo {data['elo']:.1f} | W:{data['wins']} L:{data['losses']} ({win_pct:.1f}%)")
        pages = chunk_list(lines, 10)
        footer = f"{len(sorted_players)} players"
        return PagedListView(title=f"{category} Leaderboard", pages=pages, color=discord.Color.gold(), footer_note=footer)

    def build_match_embed(self, guild: discord.Guild, category: str, match: Dict[str, Any]) -> discord.Embed:
        board_cfg = self.get_leaderboard_config(guild.id, category)
        board_name = board_cfg.get("name", category)
        challenger_id = match.get("challenger_id")
        opponent_id = match.get("opponent_id")
        challenger_member = guild.get_member(challenger_id) or self.client.get_user(challenger_id)
        opponent_member = guild.get_member(opponent_id) if opponent_id else None
        if not opponent_member and opponent_id:
            opponent_member = self.client.get_user(opponent_id)
        challenger_rank = self.get_player_rank(guild.id, category, challenger_id)
        opponent_rank = self.get_player_rank(guild.id, category, opponent_id) if opponent_id else None
        challenger_line = challenger_member.mention if challenger_member else f"<@{challenger_id}>"
        if challenger_rank:
            rank_idx, data = challenger_rank
            challenger_line += f"\nElo {data['elo']:.1f} | Rank #{rank_idx}"
        opponent_line = "**Awaiting opponent**"
        if opponent_id:
            opponent_line = opponent_member.mention if opponent_member else f"<@{opponent_id}>"
            if opponent_rank:
                rank_idx, data = opponent_rank
                opponent_line += f"\nElo {data['elo']:.1f} | Rank #{rank_idx}"
        status_labels = {
            "open": "Awaiting opponent",
            "pending": "Awaiting acceptance",
            "active": "Match in progress",
            "awaiting_result": "Waiting for result submissions",
            "completed": "Completed",
            "cancelled": "Cancelled",
            "disputed": "Requires moderator review",
            "pending_cancel": "Cancellation pending approval",
        }
        status = match.get("status", "open")
        status_text = status_labels.get(status, status.title())
        mode_info_raw = match.get("mode") or self.get_category_mode(guild.id, category)
        mode_info = self.normalize_mode_value(mode_info_raw)
        mode_label = self.mode_label(mode_info)
        embed_color = discord.Color.blurple()
        if status == "completed":
            embed_color = discord.Color.green()
        elif status == "cancelled":
            embed_color = discord.Color.red()
        elif status == "disputed":
            embed_color = discord.Color.orange()
        embed = discord.Embed(title=f"{board_name} Challenge", color=embed_color)
        if status == "pending" and opponent_id:
            challenger_mention = challenger_member.mention if challenger_member else f"<@{challenger_id}>"
            opponent_mention = opponent_member.mention if opponent_member else f"<@{opponent_id}>"
            embed.description = f"{challenger_mention} challenged {opponent_mention}."
        embed.add_field(name="Challenger", value=challenger_line, inline=True)
        embed.add_field(name="Opponent", value=opponent_line, inline=True)
        rank_range = match.get("rank_range")
        extra = mode_label
        if rank_range:
            extra += f"\nRank window +/-{rank_range}"
        embed.add_field(name="Format", value=extra, inline=False)
        embed.add_field(name="Status", value=status_text, inline=False)
        submissions = match.get("submissions", {})
        if submissions:
            submission_lines = []
            for uid_str, record in submissions.items():
                try:
                    uid = int(uid_str)
                except Exception:
                    uid = uid_str
                member = guild.get_member(uid) or self.client.get_user(uid)
                label = member.mention if member else f"<@{uid}>"
                note = record.get("value", "?")
                kind = record.get("kind", "submitted")
                submission_lines.append(f"{label}: {kind} {note}")
            embed.add_field(name="Submissions", value="\n".join(submission_lines), inline=False)
        if match.get("result"):
            result = match["result"]
            winner_id = result.get("winner_id")
            loser_id = result.get("loser_id")
            winner_member = guild.get_member(winner_id) or self.client.get_user(winner_id)
            loser_member = guild.get_member(loser_id) or self.client.get_user(loser_id)
            winner_label = winner_member.mention if winner_member else f"<@{winner_id}>"
            loser_label = loser_member.mention if loser_member else f"<@{loser_id}>"
            if mode_info.get("type") == "time":
                detail = f"{result.get('winner_value', '?')} vs {result.get('loser_value', '?')}"
            else:
                detail = f"{result.get('winner_value', '?')}-{result.get('loser_value', '?')}"
            embed.add_field(name="Result", value=f"{winner_label} defeated {loser_label}\n{detail}", inline=False)
        created_at = match.get("created_at")
        try:
            created_dt = datetime.fromisoformat(created_at) if created_at else datetime.now(timezone.utc)
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            embed.timestamp = created_dt
        except Exception:
            embed.timestamp = datetime.now(timezone.utc)
        footer_parts = [f"Match ID {match.get('id')}"]
        if match.get("thread_id"):
            footer_parts.append("Thread active")
        embed.set_footer(text="  |  ".join(footer_parts))
        return embed

    def build_match_view(self, guild_id: int, category: str, match_id: str) -> ChallengeControlView:
        return ChallengeControlView(self, guild_id, category, match_id)

    async def refresh_match_message(self, guild_id: int, category: str, match_id: str):
        match = self.get_match(guild_id, category, match_id)
        if not match:
            return
        channel_id = match.get("channel_id")
        message_id = match.get("message_id")
        if not channel_id or not message_id:
            return
        channel = self.client.get_channel(channel_id)
        if not channel:
            return
        try:
            message = await channel.fetch_message(message_id)
        except Exception:
            return
        guild = channel.guild
        embed = self.build_match_embed(guild, category, match)
        view = self.build_match_view(guild_id, category, match_id)
        view.refresh_buttons()
        try:
            await message.edit(embed=embed, view=view)
        except Exception:
            logger.exception("Failed to update match message %s in guild %s", match_id, guild_id)
            return
        try:
            self.client.add_view(view, message_id=message_id)
        except Exception:
            logger.debug("View registration failed for match %s in guild %s", match_id, guild_id)

    async def ensure_match_thread(self, guild: discord.Guild, category: str, match: Dict[str, Any], message: discord.Message):
        match_id = match.get("id") or match.get("match_id")
        thread_id = match.get("thread_id")
        if thread_id:
            existing = self.client.get_channel(thread_id)
            if isinstance(existing, discord.Thread):
                match.pop("thread_message_id", None)
                return existing
        board_cfg = self.get_leaderboard_config(guild.id, category)
        base_channel_id = board_cfg.get("challenge_channel_id") or message.channel.id
        base_channel = guild.get_channel(base_channel_id) or message.channel
        challenger_id = match.get("challenger_id")
        opponent_id = match.get("opponent_id")
        challenger_member = guild.get_member(challenger_id) or self.client.get_user(challenger_id)
        opponent_member = guild.get_member(opponent_id) or self.client.get_user(opponent_id) if opponent_id else None
        challenger_name = challenger_member.display_name if isinstance(challenger_member, discord.Member) else getattr(challenger_member, "name", str(challenger_id))
        opponent_name = opponent_member.display_name if isinstance(opponent_member, discord.Member) else getattr(opponent_member, "name", str(opponent_id)) if opponent_id else "TBD"
        thread_label = f"{board_cfg.get('name', category)} | {challenger_name}"
        if opponent_id:
            thread_label += f" vs {opponent_name}"
        thread_label = thread_label[:90]
        try:
            thread = await base_channel.create_thread(name=thread_label, type=discord.ChannelType.public_thread, message=message, auto_archive_duration=1440)
        except Exception:
            logger.exception("Failed to create thread for guild %s match %s", guild.id, match.get("id"))
            return None
        try:
            await thread.set_permissions(guild.default_role, send_messages=False, add_reactions=False)
            for role in guild.roles:
                perms = role.permissions
                if perms.administrator or perms.manage_guild or perms.manage_messages:
                    await thread.set_permissions(role, send_messages=True, add_reactions=True)
        except Exception:
            logger.debug("Thread permission update failed for guild %s match %s", guild.id, match.get("id"))
        try:
            await thread.send("This thread tracks the match. Only moderators can speak here. Use /leaderboard commands to manage the result.")
            if opponent_id and challenger_id:
                challenger_mention = challenger_member.mention if challenger_member else f"<@{challenger_id}>"
                opponent_mention = opponent_member.mention if opponent_member else f"<@{opponent_id}>"
                ping_text = f"{challenger_mention} {opponent_mention} good luck!"
                await thread.send(ping_text, allowed_mentions=discord.AllowedMentions(users=True))
        except Exception:
            logger.debug("Initial thread message failed for guild %s match %s", guild.id, match.get("id"))
        match["thread_id"] = thread.id
        match.pop("thread_message_id", None)
        bucket = self.get_active_bucket(guild.id, category)
        bucket["matches"][match_id] = match
        await self.save_active_fights_for(guild.id)
        await self.schedule_thread_deletion(guild.id, thread.id, category)
        return thread

    async def cancel_active_match(self, guild_id: int, category: str, match_id: str) -> bool:
        match = self.get_match(guild_id, category, match_id)
        if not match:
            return False
        channel_id = match.get("channel_id")
        message_id = match.get("message_id")
        thread_id = match.get("thread_id")
        channel = self.client.get_channel(channel_id) if channel_id else None
        message = None
        if channel and message_id:
            try:
                message = await channel.fetch_message(message_id)
            except Exception:
                message = None
        if thread_id:
            thread = self.client.get_channel(thread_id)
            if isinstance(thread, discord.Thread):
                try:
                    await thread.delete()
                except Exception:
                    logger.debug("Failed to delete thread %s in guild %s", thread_id, guild_id)
        if thread_id:
            bucket = self.get_active_bucket(guild_id, category)
            deletions = bucket.get("deletions", [])
            bucket["deletions"] = [entry for entry in deletions if entry.get("thread_id") != thread_id]
        if message:
            try:
                await message.delete()
            except Exception:
                logger.debug("Failed to delete match message %s in guild %s", message_id, guild_id)
        await self.delete_match(guild_id, category, match_id)
        return True

    async def handle_match_accept(self, interaction: discord.Interaction, guild_id: int, category: str, match_id: str):
        if interaction.guild is None or interaction.guild.id != guild_id:
            await interaction.response.send_message("This interaction is no longer valid.", ephemeral=True)
            return
        match = self.get_match(guild_id, category, match_id)
        if not match:
            await interaction.response.send_message("Match not found or already closed.", ephemeral=True)
            return
        status = match.get("status", "open")
        if status in {"completed", "cancelled"}:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return
        challenger_id = match.get("challenger_id")
        opponent_id = match.get("opponent_id")
        if interaction.user.id == challenger_id:
            await interaction.response.send_message("You cannot accept your own challenge.", ephemeral=True)
            return
        board_cfg = self.get_leaderboard_config(guild_id, category)
        member = interaction.guild.get_member(interaction.user.id)
        if not self.is_participant(member, board_cfg):
            await interaction.response.send_message("You are not registered for this leaderboard.", ephemeral=True)
            return
        if opponent_id and opponent_id != interaction.user.id:
            await interaction.response.send_message("This challenge is reserved for another player.", ephemeral=True)
            return
        existing_match = self.find_active_match_for(guild_id, interaction.user.id, exclude=match_id)
        if existing_match:
            existing_category, existing_match_id, existing_data = existing_match
            existing_status = existing_data.get("status", "open")
            is_self_queue = (
                existing_data.get("challenger_id") == interaction.user.id
                and existing_status in {"open", "pending"}
            )
            if is_self_queue:
                cancelled = await self.cancel_active_match(guild_id, existing_category, existing_match_id)
                if not cancelled:
                    await interaction.response.send_message("You already have an active challenge.", ephemeral=True)
                    return
            else:
                await interaction.response.send_message("You already have an active challenge.", ephemeral=True)
                return
        if opponent_id is None and interaction.user.id == challenger_id:
            await interaction.response.send_message("Waiting for another player to accept.", ephemeral=True)
            return
        if opponent_id is None:
            rank_range = match.get("rank_range")
            if rank_range:
                challenger_rank = self.get_player_rank(guild_id, category, challenger_id)
                opponent_rank = self.get_player_rank(guild_id, category, interaction.user.id)
                if challenger_rank and opponent_rank:
                    diff = abs(challenger_rank[0] - opponent_rank[0])
                    if diff > int(rank_range):
                        await interaction.response.send_message(f"You must be within {rank_range} ranks of the challenger to accept.", ephemeral=True)
                        return
            match["opponent_id"] = interaction.user.id
        match["status"] = "awaiting_result"
        match["accepted_at"] = datetime.now(timezone.utc).isoformat()
        match["cancel_votes"] = []
        match.setdefault("submissions", {})
        mode_payload = self.normalize_mode_value(match.get("mode") or board_cfg.get("mode") or {"key": "speedrun"})
        match["mode"] = {"key": mode_payload["key"], "target": mode_payload["target"]}
        match.pop("response_deadline", None)
        bucket = self.get_active_bucket(guild_id, category)
        bucket["matches"][match_id] = match
        await self.save_active_fights_for(guild_id)
        gid_s = str(guild_id)
        self.players_meta.setdefault(gid_s, {})[str(interaction.user.id)] = {
            "name": interaction.user.display_name,
            "avatar": interaction.user.avatar.url if interaction.user.avatar else None,
        }
        await self.persist_player_meta(guild_id)
        channel = self.client.get_channel(match.get("channel_id"))
        message = None
        if channel:
            try:
                message = await channel.fetch_message(match.get("message_id"))
            except Exception:
                message = None
        if message:
            await self.ensure_match_thread(interaction.guild, category, match, message)
        await self.refresh_match_message(guild_id, category, match_id)
        await interaction.response.send_message("Challenge accepted. Good luck!", ephemeral=True)

    
    async def handle_match_decline(self, interaction: discord.Interaction, guild_id: int, category: str, match_id: str):
        if interaction.guild is None or interaction.guild.id != guild_id:
            await interaction.response.send_message("This interaction is no longer valid.", ephemeral=True)
            return
        match = self.get_match(guild_id, category, match_id)
        if not match:
            await interaction.response.send_message("Match not found or already closed.", ephemeral=True)
            return
        status = match.get("status", "open")
        if status != "pending":
            await interaction.response.send_message("This challenge is not awaiting your response.", ephemeral=True)
            return
        opponent_id = match.get("opponent_id")
        if not opponent_id or interaction.user.id != opponent_id:
            await interaction.response.send_message("Only the challenged player can decline.", ephemeral=True)
            return
        guild = interaction.guild
        challenger_id = match.get("challenger_id")
        board_cfg = self.get_leaderboard_config(guild_id, category)
        mode_info = self.normalize_mode_value(match.get("mode") or board_cfg.get("mode") or self.get_category_mode(guild_id, category))
        if mode_info["type"] == "time":
            winner_metric = 0.001
            loser_metric = 30.0
        else:
            target = int(mode_info.get("target", 1))
            winner_metric = float(target)
            loser_metric = 0.0
        winner_entry = (challenger_id, {"metric": winner_metric})
        loser_entry = (opponent_id, {"metric": loser_metric})
        outcome = await self.complete_match(guild, category, match_id, winner_entry, loser_entry, override_notes="Declined")
        await self.refresh_match_message(guild_id, category, match_id)
        if outcome != "error":
            if mode_info["type"] == "time":
                await interaction.response.send_message("You declined and forfeited the match.", ephemeral=True)
            else:
                await interaction.response.send_message(f"You declined and lost {int(winner_metric)}-0.", ephemeral=True)
        else:
            await interaction.response.send_message("Unable to record the forfeit.", ephemeral=True)
    async def handle_match_cancel(self, interaction: discord.Interaction, guild_id: int, category: str, match_id: str):
        if interaction.guild is None or interaction.guild.id != guild_id:
            await interaction.response.send_message("This interaction is no longer valid.", ephemeral=True)
            return
        match = self.get_match(guild_id, category, match_id)
        if not match:
            await interaction.response.send_message("Match not found or already closed.", ephemeral=True)
            return
        status = match.get("status", "open")
        if status in {"completed", "cancelled"}:
            await interaction.response.send_message("This match is no longer active.", ephemeral=True)
            return
        challenger_id = match.get("challenger_id")
        opponent_id = match.get("opponent_id")
        user_id = interaction.user.id
        participants = {challenger_id}
        if opponent_id:
            participants.add(opponent_id)
        if user_id not in participants:
            await interaction.response.send_message("Only the players in this match can cancel it.", ephemeral=True)
            return
        if opponent_id is None:
            if user_id != challenger_id:
                await interaction.response.send_message("Only the challenger can cancel this request.", ephemeral=True)
                return
            await self.cancel_active_match(guild_id, category, match_id)
            await interaction.response.send_message("Challenge cancelled.", ephemeral=True)
            return
        if status == "pending":
            if user_id not in {challenger_id, opponent_id}:
                await interaction.response.send_message("Only the players in this match can cancel it.", ephemeral=True)
                return
            await self.cancel_active_match(guild_id, category, match_id)
            await interaction.response.send_message("Challenge cancelled.", ephemeral=True)
            return
        votes = set(match.get("cancel_votes", []))
        if user_id in votes:
            await interaction.response.send_message("You have already requested cancellation.", ephemeral=True)
            return
        votes.add(user_id)
        match["cancel_votes"] = list(votes)
        everyone_voted = all(pid in votes for pid in participants if pid)
        if everyone_voted:
            await self.cancel_active_match(guild_id, category, match_id)
            await interaction.response.send_message("Challenge cancelled.", ephemeral=True)
            return
        match["status"] = "pending_cancel"
        bucket = self.get_active_bucket(guild_id, category)
        bucket["matches"][match_id] = match
        await self.save_active_fights_for(guild_id)
        await self.refresh_match_message(guild_id, category, match_id)
        await interaction.response.send_message("Cancellation request sent. Waiting for the other player.", ephemeral=True)

    async def submit_match_result(self, interaction: discord.Interaction, kind: str, value: str):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            await interaction.followup.send("Server-only command.", ephemeral=True)
            return
        guild = interaction.guild
        active = self.find_active_match_for(guild.id, interaction.user.id)
        if not active:
            await interaction.followup.send("You do not have an active challenge.", ephemeral=True)
            return
        category, match_id, match = active
        status = match.get("status", "open")
        if status not in {"awaiting_result", "pending"}:
            if status == "pending_cancel":
                await interaction.followup.send("This match is pending cancellation. Ask a moderator to resolve it.", ephemeral=True)
            else:
                await interaction.followup.send("This match is not ready for results yet.", ephemeral=True)
            return
        board_cfg = self.get_leaderboard_config(guild.id, category)
        mode_info = self.normalize_mode_value(match.get("mode") or board_cfg.get("mode") or self.get_category_mode(guild.id, category))
        submissions = match.setdefault("submissions", {})
        value = value.strip()
        if mode_info["type"] == "time":
            metric = self.parse_time(value)
            if metric is None or metric <= 0:
                await interaction.followup.send("Invalid time format. Use MM:SS.sss or SS.sss", ephemeral=True)
                return
            value_formatted = self.format_time_value(metric)
        else:
            score = self.parse_score(value)
            if score is None:
                await interaction.followup.send("Scores must be whole numbers.", ephemeral=True)
                return
            target = mode_info.get("target", 1)
            if kind == "win" and score != target:
                await interaction.followup.send(f"Winning score must be {target}.", ephemeral=True)
                return
            if kind == "loss" and score >= target:
                await interaction.followup.send(f"Losing score must be less than {target}.", ephemeral=True)
                return
            metric = float(score)
            value_formatted = str(score)
        submissions[str(interaction.user.id)] = {
            "kind": "win" if kind == "win" else "loss",
            "value": value_formatted,
            "metric": metric,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
        bucket = self.get_active_bucket(guild.id, category)
        bucket["matches"][match_id] = match
        await self.save_active_fights_for(guild.id)
        gid_s = str(guild.id)
        self.players_meta.setdefault(gid_s, {})[str(interaction.user.id)] = {
            "name": interaction.user.display_name,
            "avatar": interaction.user.avatar.url if interaction.user.avatar else None,
        }
        await self.persist_player_meta(guild.id)
        other_id = match.get("opponent_id") if match.get("challenger_id") == interaction.user.id else match.get("challenger_id")
        other_record = submissions.get(str(other_id)) if other_id else None
        if other_record:
            if other_record.get("kind") == submissions[str(interaction.user.id)]["kind"]:
                match["status"] = "disputed"
                bucket["matches"][match_id] = match
                await self.save_active_fights_for(guild.id)
                await self.refresh_match_message(guild.id, category, match_id)
                await interaction.followup.send("Conflicting submissions detected. A moderator must override the result.", ephemeral=True)
                return
            outcome = await self.finalize_match_from_submissions(guild, category, match_id)
            await self.refresh_match_message(guild.id, category, match_id)
            if outcome == "disputed":
                await interaction.followup.send("Conflicting submissions detected. A moderator must override the result.", ephemeral=True)
                return
            if outcome == "error":
                await interaction.followup.send("Unable to finalize the match. Please contact a moderator.", ephemeral=True)
                return
            await interaction.followup.send(outcome, ephemeral=True)
            return
        match["status"] = "awaiting_result"
        bucket["matches"][match_id] = match
        await self.save_active_fights_for(guild.id)
        await self.refresh_match_message(guild.id, category, match_id)
        await interaction.followup.send("Result received. Waiting for the other player.", ephemeral=True)

    async def finalize_match_from_submissions(self, guild: discord.Guild, category: str, match_id: str):
        match = self.get_match(guild.id, category, match_id)
        if not match:
            return "error"
        submissions = match.get("submissions", {})
        winner_entry = None
        loser_entry = None
        for uid_str, record in submissions.items():
            try:
                uid = int(uid_str)
            except Exception:
                continue
            if record.get("kind") == "win":
                winner_entry = (uid, record)
            elif record.get("kind") == "loss":
                loser_entry = (uid, record)
        if not winner_entry or not loser_entry:
            match["status"] = "disputed"
            bucket = self.get_active_bucket(guild.id, category)
            bucket["matches"][match_id] = match
            await self.save_active_fights_for(guild.id)
            return "disputed"
        return await self.complete_match(guild, category, match_id, winner_entry, loser_entry)

    async def complete_match(self, guild: discord.Guild, category: str, match_id: str, winner_entry: Tuple[int, Dict[str, Any]], loser_entry: Tuple[int, Dict[str, Any]], override_notes: Optional[str] = None):
        match = self.get_match(guild.id, category, match_id)
        if not match:
            return "error"
        winner_id, winner_record = winner_entry
        loser_id, loser_record = loser_entry
        board_cfg = self.get_leaderboard_config(guild.id, category)
        mode_info = self.normalize_mode_value(match.get("mode") or board_cfg.get("mode") or self.get_category_mode(guild.id, category))
        players = self.load_players_for(guild.id, category)
        safe_cat = normalize_category(category)
        players.setdefault(winner_id, {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0})
        players.setdefault(loser_id, {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0})
        winner_stats = players[winner_id]
        loser_stats = players[loser_id]
        winner_old_elo = winner_stats["elo"]
        loser_old_elo = loser_stats["elo"]
        if mode_info["type"] == "time":
            winner_metric = float(winner_record.get("metric"))
            loser_metric = float(loser_record.get("metric"))
            winner_value = self.format_time_value(winner_metric)
            loser_value = self.format_time_value(loser_metric)
        else:
            winner_metric = float(winner_record.get("metric"))
            loser_metric = float(loser_record.get("metric"))
            target = mode_info.get("target", 1)
            winner_value = str(int(winner_metric))
            loser_value = str(int(loser_metric))
            if winner_metric < target:
                winner_metric = float(target)
                winner_value = str(target)
        winner_stats["wins"] += 1
        loser_stats["losses"] += 1
        elo_delta = self.compute_elo_change(mode_info, winner_stats["elo"], loser_stats["elo"], winner_metric, loser_metric)
        winner_stats["elo"] = max(0.0, winner_stats["elo"] + elo_delta)
        loser_stats["elo"] = max(0.0, loser_stats["elo"] - elo_delta)
        winner_new_elo = winner_stats["elo"]
        loser_new_elo = loser_stats["elo"]
        self.players_data.setdefault(str(guild.id), {})[safe_cat] = players
        await self.save_players_for(guild.id, category)
        now = datetime.now(timezone.utc)
        challenger_id = match.get("challenger_id")
        await self.save_match_for(
            guild.id,
            category,
            winner_id,
            now,
            loser_id,
            winner_id == challenger_id,
            winner_value,
            loser_value,
            "Win",
            elo_delta,
        )
        await self.save_match_for(
            guild.id,
            category,
            loser_id,
            now,
            winner_id,
            loser_id == challenger_id,
            loser_value,
            winner_value,
            "Loss",
            -elo_delta,
        )
        match["status"] = "completed"
        match["result"] = {
            "winner_id": winner_id,
            "loser_id": loser_id,
            "winner_value": winner_value,
            "loser_value": loser_value,
            "override_notes": override_notes,
            "completed_at": now.isoformat(),
            "winner_elo_change": elo_delta,
            "loser_elo_change": -elo_delta,
            "winner_new_elo": winner_new_elo,
            "loser_new_elo": loser_new_elo,
            "winner_old_elo": winner_old_elo,
            "loser_old_elo": loser_old_elo,
        }
        match["submissions"] = {}
        match["cancel_votes"] = []
        bucket = self.get_active_bucket(guild.id, category)
        bucket["matches"][match_id] = match
        await self.save_active_fights_for(guild.id)
        winner_member = guild.get_member(winner_id) or self.client.get_user(winner_id)
        loser_member = guild.get_member(loser_id) or self.client.get_user(loser_id)
        winner_name = winner_member.display_name if isinstance(winner_member, discord.Member) else getattr(winner_member, "name", f"User {winner_id}")
        loser_name = loser_member.display_name if isinstance(loser_member, discord.Member) else getattr(loser_member, "name", f"User {loser_id}")
        winner_avatar = winner_member.display_avatar.url if winner_member and getattr(winner_member, "display_avatar", None) else None
        loser_avatar = loser_member.display_avatar.url if loser_member and getattr(loser_member, "display_avatar", None) else None
        self.players_meta.setdefault(str(guild.id), {})[str(winner_id)] = {"name": winner_name, "avatar": winner_avatar}
        self.players_meta.setdefault(str(guild.id), {})[str(loser_id)] = {"name": loser_name, "avatar": loser_avatar}
        await self.persist_player_meta(guild.id)
        self.players_data[str(guild.id)][safe_cat] = players
        await self.update_leaderboard_message_for(guild.id, category)
        await self.refresh_match_message(guild.id, category, match_id)
        remove_map = self.removed.get(str(guild.id), {}).get(safe_cat, {})
        ranked = sorted(((uid, data) for uid, data in players.items() if uid not in remove_map), key=lambda item: item[1]["elo"], reverse=True)
        def rank_of(uid: int) -> Optional[int]:
            for idx, (entry_id, _) in enumerate(ranked, start=1):
                if entry_id == uid:
                    return idx
            return None
        winner_rank = rank_of(winner_id)
        loser_rank = rank_of(loser_id)
        board_name = board_cfg.get("name", category)
        detail = f"{winner_value} vs {loser_value}" if mode_info["type"] == "time" else f"{winner_value}-{loser_value}"
        summary = f"{winner_member.mention if winner_member else f'<@{winner_id}>'} defeated {loser_member.mention if loser_member else f'<@{loser_id}>'} in {board_name} ({detail})."
        announce_channel_id = board_cfg.get("announce_channel_id")
        if announce_channel_id:
            announce_channel = self.client.get_channel(announce_channel_id)
            if announce_channel:
                announce_embed = discord.Embed(title=f"{board_name} Result", color=discord.Color.green(), description=summary)
                winner_delta_display = self.format_elo_delta(match["result"].get("winner_elo_change", elo_delta))
                loser_delta_display = self.format_elo_delta(match["result"].get("loser_elo_change", -elo_delta))
                winner_field = f"{winner_stats['elo']:.1f} ({winner_delta_display})"
                loser_field = f"{loser_stats['elo']:.1f} ({loser_delta_display})"
                if winner_rank:
                    winner_field += f" (Rank #{winner_rank})"
                if loser_rank:
                    loser_field += f" (Rank #{loser_rank})"
                announce_embed.add_field(name="Winner Elo", value=winner_field, inline=True)
                announce_embed.add_field(name="Loser Elo", value=loser_field, inline=True)
                if override_notes:
                    announce_embed.add_field(name="Notes", value=override_notes, inline=False)
                try:
                    await announce_channel.send(embed=announce_embed)
                except Exception:
                    logger.debug("Failed to post announcement for guild %s match %s", guild.id, match_id)
        thread_id = match.get("thread_id")
        if thread_id:
            thread = self.client.get_channel(thread_id)
            if isinstance(thread, discord.Thread):
                try:
                    await thread.send(f"Result recorded: {winner_member.mention if winner_member else f'<@{winner_id}>'} defeated {loser_member.mention if loser_member else f'<@{loser_id}>'} ({detail}).")
                except Exception:
                    logger.debug("Failed to post result in thread %s for guild %s", thread_id, guild.id)
            await self.schedule_thread_deletion(guild.id, thread_id, category)
        removed_match = bucket["matches"].pop(match_id, None)
        if removed_match is not None:
            await self.save_active_fights_for(guild.id)
        winner_label = winner_member.display_name if isinstance(winner_member, discord.Member) else self.user_snapshot_name_for(guild.id, winner_id)
        loser_label = loser_member.display_name if isinstance(loser_member, discord.Member) else self.user_snapshot_name_for(guild.id, loser_id)
        return f"Match recorded: {winner_label} defeated {loser_label} ({detail})."

    async def count_member_matches(self, gid: int, category: str, member_id: int) -> int:
        return await asyncio.to_thread(self.storage.count_member_matches, gid, category, member_id)

    async def most_active_board(self, gid: int, boards: List[str], member_id: int) -> str:
        if not boards:
            raise ValueError("No boards available")

        def worker() -> str:
            best_board = boards[0]
            best_score = -1
            for board in boards:
                score = self.storage.count_member_matches(gid, board, member_id)
                if score > best_score or (score == best_score and board.lower() < best_board.lower()):
                    best_board = board
                    best_score = score
            return best_board

        return await asyncio.to_thread(worker)

    def get_profile_bio(self, gid: int, user_id: int) -> Optional[str]:
        return self.bios.get(str(gid), {}).get(GLOBAL_BIO_KEY, {}).get(str(user_id))

    async def build_profile_content(self, gid: int, category: str, member: discord.abc.User) -> Tuple[discord.Embed, List[List[str]]]:
        players = self.load_players_for(gid, category)
        stats = players.get(member.id, {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0})
        rank_info = self.get_player_rank(gid, category, member.id)
        display_name = member.display_name if isinstance(member, discord.Member) else getattr(member, "name", f"User {member.id}")
        embed = discord.Embed(title=f"{category} Profile - {display_name}", color=discord.Color.blurple())
        embed.add_field(name="Elo", value=f"{stats['elo']:.1f}", inline=True)
        embed.add_field(name="Record", value=f"W:{stats['wins']} L:{stats['losses']}", inline=True)
        rank_text = f"#{rank_info[0]}" if rank_info else "Unranked"
        embed.add_field(name="Rank", value=rank_text, inline=True)
        total_matches = stats["wins"] + stats["losses"]
        embed.add_field(name="Matches Played", value=str(total_matches), inline=True)
        avatar_url = self.user_snapshot_avatar_for(gid, member.id)
        if avatar_url:
            embed.set_thumbnail(url=avatar_url)
        bio = self.get_profile_bio(gid, member.id)
        if not bio:
            safe_current = normalize_category(category)
            bio = self.bios.get(str(gid), {}).get(safe_current, {}).get(str(member.id))
        if bio:
            embed.add_field(name="Bio", value=bio, inline=False)
        lines: List[str] = []
        latest_delta: Optional[float] = None
        try:
            rows = await asyncio.to_thread(self.storage.load_match_history, gid, category)
            entries: List[Tuple[datetime, str, float]] = []
            for row in rows:
                formatted = self.format_match_entry(gid, category, row, perspective_id=member.id)
                if formatted:
                    recorded_at, label = formatted
                    try:
                        delta_value = float(row.get("elo_change", "0"))
                    except (TypeError, ValueError):
                        delta_value = 0.0
                    entries.append((recorded_at, label, delta_value))
            entries.sort(key=lambda item: item[0], reverse=True)
            if entries:
                latest_delta = entries[0][2]
            lines = [item[1] for item in entries]
        except Exception:
            logger.debug("Failed reading match history for profile in guild %s board %s", gid, category)
        if latest_delta is not None and abs(latest_delta) > 0.0001 and len(embed.fields) > 0:
            elo_with_delta = f"{stats['elo']:.1f} ({self.format_elo_delta(latest_delta)})"
            embed.set_field_at(0, name="Elo", value=elo_with_delta, inline=True)
        pages = chunk_list(lines, 5) if lines else []
        return embed, pages

    @leaderboard.command(name="iwon")
    @app_commands.describe(result="Your completion time or score")
    async def iwon(self, interaction: discord.Interaction, result: str):
        await self.submit_match_result(interaction, "win", result)

    @leaderboard.command(name="ilost")
    @app_commands.describe(result="Your completion time or score")
    async def ilost(self, interaction: discord.Interaction, result: str):
        await self.submit_match_result(interaction, "loss", result)

    @leaderboard.command(name="override")
    @app_commands.describe(
        category="Leaderboard name",
        winner="Winner",
        loser="Loser",
        winner_value="Winner result",
        loser_value="Loser result",
        notes="Optional notes for the log",
    )
    @app_commands.autocomplete(category=category_autocomplete)
    async def override(self, interaction: discord.Interaction, category: str, winner: discord.Member, loser: discord.Member, winner_value: str, loser_value: str, notes: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)
        if not self.has_mod_permissions(interaction.user):
            await interaction.followup.send("You do not have permission to override results.", ephemeral=True)
            return
        if interaction.guild is None:
            await interaction.followup.send("Server-only command.", ephemeral=True)
            return
        guild = interaction.guild
        if winner.id == loser.id:
            await interaction.followup.send("Winner and loser must be different players.", ephemeral=True)
            return
        pair = self.find_match_between(guild.id, category, winner.id, loser.id)
        if not pair:
            await interaction.followup.send("No active match found between those players.", ephemeral=True)
            return
        match_id, match = pair
        board_cfg = self.get_leaderboard_config(guild.id, category)
        mode_info = self.normalize_mode_value(match.get("mode") or board_cfg.get("mode") or self.get_category_mode(guild.id, category))
        if mode_info["type"] == "time":
            winner_metric = self.parse_time(winner_value.strip())
            loser_metric = self.parse_time(loser_value.strip())
            if winner_metric is None or loser_metric is None:
                await interaction.followup.send("Invalid time format. Use MM:SS.sss or SS.sss", ephemeral=True)
                return
            winner_record = {"kind": "win", "value": self.format_time_value(winner_metric), "metric": winner_metric}
            loser_record = {"kind": "loss", "value": self.format_time_value(loser_metric), "metric": loser_metric}
        else:
            winner_score = self.parse_score(winner_value.strip())
            loser_score = self.parse_score(loser_value.strip())
            target = mode_info.get("target", 1)
            if winner_score is None or loser_score is None:
                await interaction.followup.send("Scores must be whole numbers.", ephemeral=True)
                return
            if winner_score != target:
                await interaction.followup.send(f"Winning score must be {target}.", ephemeral=True)
                return
            if loser_score >= target:
                await interaction.followup.send(f"Losing score must be less than {target}.", ephemeral=True)
                return
            winner_record = {"kind": "win", "value": str(winner_score), "metric": float(winner_score)}
            loser_record = {"kind": "loss", "value": str(loser_score), "metric": float(loser_score)}
        outcome = await self.complete_match(guild, category, match_id, (winner.id, winner_record), (loser.id, loser_record), override_notes=notes)
        if outcome == "error":
            await interaction.followup.send("Failed to override the match. Please try again.", ephemeral=True)
            return
        await interaction.followup.send(f"Override applied. {outcome}", ephemeral=True)


    @leaderboard.command(name="categories")
    async def categories(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        gid = interaction.guild.id
        cfg = self.get_gconfig(gid)
        boards = cfg.get("leaderboards", {})
        if not boards:
            return await interaction.followup.send("No leaderboards configured.", ephemeral=True)
        embed = discord.Embed(title="Leaderboard Summary", color=discord.Color.gold())
        for safe_name, data in sorted(boards.items(), key=lambda item: item[1].get("name", item[0]).lower()):
            name = data.get("name", safe_name.replace("_", " ").title())
            players = self.load_players_for(gid, name)
            safe_cat = normalize_category(name)
            removed_map = self.removed.get(str(gid), {}).get(safe_cat, {})
            active_players = [(uid, info) for uid, info in players.items() if uid not in removed_map]
            active_count = len(active_players)
            removed_count = len(removed_map)
            summary_parts = [f"{active_count} active"]
            if removed_count:
                summary_parts.append(f"{removed_count} removed")
            player_line = " | ".join(summary_parts)
            mode_label = self.mode_label(self.get_category_mode(gid, name))
            role = interaction.guild.get_role(data.get("participant_role_id")) if data.get("participant_role_id") else None
            participant = role.mention if role else "Not set"
            board_channel = self.client.get_channel(data.get("leaderboard_channel_id")) if data.get("leaderboard_channel_id") else None
            leaderboard_target = board_channel.mention if board_channel else "Not set"
            message_status = "Posted" if data.get("leaderboard_message_id") else "Not posted"
            challenge_channel = self.client.get_channel(data.get("challenge_channel_id")) if data.get("challenge_channel_id") else None
            challenge_target = challenge_channel.mention if challenge_channel else "Not set"
            outgoing_channel = self.client.get_channel(data.get("outgoing_channel_id")) if data.get("outgoing_channel_id") else None
            outgoing_target = outgoing_channel.mention if outgoing_channel else "Not set"
            announce_channel = self.client.get_channel(data.get("announce_channel_id")) if data.get("announce_channel_id") else None
            announce_target = announce_channel.mention if announce_channel else "Not set"
            cleanup_seconds = int(data.get("thread_cleanup_seconds", 21600))
            cleanup_hours = cleanup_seconds / 3600
            summary_lines = [
                f"Players: {player_line}",
                f"Mode: {mode_label}",
                f"Participant Role: {participant}",
                f"Leaderboard Channel: {leaderboard_target} ({message_status})",
                f"Challenge Channel: {challenge_target}",
                f"Outgoing Channel: {outgoing_target}",
                f"Announcements: {announce_target}",
                f"Thread Cleanup: {cleanup_hours:.1f}h",
            ]
            if active_players:
                top_uid, top_info = max(active_players, key=lambda item: item[1]["elo"])
                top_member = self.client.get_user(top_uid) or interaction.guild.get_member(top_uid)
                top_label = top_member.display_name if top_member else self.user_snapshot_name_for(gid, top_uid)
                summary_lines.append(f"Top Player: {top_label} ({top_info['elo']:.1f})")
            embed.add_field(name=name, value="\n".join(summary_lines), inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @leaderboard.command(name="help")
    async def help(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        embed = discord.Embed(title="Leaderboard Help", color=discord.Color.gold(), description="Quick reference for configuring and running competitions.")
        embed.add_field(
            name="Setup Checklist",
            value="- /leaderboard setleaderboard to configure a board\n- /leaderboard editboard to adjust channels, mode, or rename later\n- /leaderboard categories to review setup and player counts",
            inline=False,
        )
        mode_lines = []
        for key, data in MODES.items():
            if data["type"] == "time":
                desc = "Track fastest completion times. Log winner and loser times."
            else:
                desc = "First to a chosen score. Set the winning score through /leaderboard editboard mode_target:<number>."
            mode_lines.append(f"{data['label']}: {desc}")
        embed.add_field(name="Modes", value="\n".join(mode_lines), inline=False)
        embed.add_field(
            name="Editing & Maintenance",
            value="- /leaderboard editboard player:<member> to tweak stats\n- /leaderboard editboard leaderboard_channel:<channel> to move the board message\n- /leaderboard editboard mode:<mode> mode_target:<score> for rule updates\n- /leaderboard removeplayer or /leaderboard readd to manage roster",
            inline=False,
        )
        embed.add_field(
            name="Logging Results",
            value="- Use /leaderboard challenge opponent or /leaderboard challenge anyone to post a match\n- Players submit with /leaderboard iwon and /leaderboard ilost\n- Moderators can fix outcomes with /leaderboard override",
            inline=False,
        )
        embed.add_field(
            name="Viewing Data",
            value="- /leaderboard profile for player stats\n- /leaderboard history for server-wide results\n- /leaderboard activefights to monitor ongoing matches",
            inline=False,
        )
        embed.set_footer(text="DM oddsnothere for help or to request features :)")
        await interaction.followup.send(embed=embed, ephemeral=True)

    @leaderboard.command(name="setleaderboard")
    @app_commands.describe(
        name="Leaderboard name",
        leaderboard_channel="Channel to post leaderboard",
        participant_role="Role for participants",
        challenge_channel="Channel for challenges",
        mode="Match mode",
        target="Winning score for First-to mode",
        outgoing_channel="Outgoing matches channel (optional)",
        announcement_channel="Announcements channel (optional)",
        thread_cleanup_hours="Hours before challenge threads are cleaned up (default 6)",
    )
    @app_commands.choices(mode=MODE_TYPE_CHOICES)
    async def setleaderboard(
        self,
        interaction: discord.Interaction,
        name: str,
        leaderboard_channel: discord.TextChannel,
        participant_role: discord.Role,
        challenge_channel: discord.TextChannel,
        mode: Optional[app_commands.Choice[str]] = None,
        target: Optional[int] = None,
        outgoing_channel: Optional[discord.TextChannel] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        thread_cleanup_hours: Optional[float] = None,
    ):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission to configure leaderboards.", ephemeral=True)
        gid = interaction.guild.id
        self.ensure_gconfig(gid)
        gid_s = str(gid)
        previous = self.get_leaderboard_config(gid, name)
        cleanup_seconds = previous.get("thread_cleanup_seconds", 21600) if previous else 21600
        if thread_cleanup_hours is not None:
            try:
                cleanup_seconds = max(0, int(float(thread_cleanup_hours) * 3600))
            except Exception:
                return await interaction.followup.send("Invalid value for thread cleanup hours.", ephemeral=True)
        view = self.build_leaderboard_view(gid, name)
        embed = view.create_embed()
        message = None
        try:
            message = await leaderboard_channel.send(embed=embed, view=view)
        except Exception:
            logger.exception("Failed to post leaderboard message for %s in guild %s", name, gid)
        if message:
            try:
                self.client.add_view(view, message_id=message.id)
            except Exception:
                logger.debug("Failed to register leaderboard view for %s in guild %s", name, gid)
        safe_name = normalize_category(name)
        self.players_data.setdefault(gid_s, {}).setdefault(safe_name, self.players_data.get(gid_s, {}).get(safe_name, {}))
        legacy_categories = self.guild_configs.setdefault(gid_s, {}).setdefault("categories", [])
        if not any(existing.lower() == name.lower() for existing in legacy_categories):
            legacy_categories.append(name)
        mode_info = None
        if mode:
            key = mode.value
            if key == "score":
                if target is None or int(target) < 1:
                    return await interaction.followup.send("Target must be at least 1.", ephemeral=True)
                mode_info = self.set_category_mode(gid, name, "score", int(target))
            else:
                mode_info = self.set_category_mode(gid, name, "speedrun")
        else:
            if not previous:
                mode_info = self.set_category_mode(gid, name, "speedrun")
            else:
                mode_info = self.get_category_mode(gid, name)
        payload = {
            "name": name,
            "leaderboard_channel_id": leaderboard_channel.id,
            "leaderboard_message_id": message.id if message else (previous.get("leaderboard_message_id") if previous else None),
            "participant_role_id": participant_role.id,
            "challenge_channel_id": challenge_channel.id,
            "outgoing_channel_id": outgoing_channel.id if outgoing_channel else (previous.get("outgoing_channel_id") if previous else None),
            "announce_channel_id": announcement_channel.id if announcement_channel else (previous.get("announce_channel_id") if previous else None),
            "thread_cleanup_seconds": cleanup_seconds,
            "mode": {"key": mode_info["key"], "target": mode_info["target"]} if mode_info else (previous.get("mode") if previous else None),
        }
        self.upsert_leaderboard_config(gid, name, payload)
        if message:
            await interaction.followup.send(f"{name} configured. Leaderboard posted in {leaderboard_channel.mention}.", ephemeral=True)
        else:
            await interaction.followup.send(
                f"{name} configured. Unable to post leaderboard in {leaderboard_channel.mention}. Fix permissions and re-run this command to post.",
                ephemeral=True,
            )

    @leaderboard.command(name="remove-leaderboard")
    async def remove_leaderboard(self, interaction: discord.Interaction, name: str):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission to remove a leaderboard.", ephemeral=True)
        gid = interaction.guild.id
        board_cfg = self.get_leaderboard_config(gid, name)
        if not board_cfg:
            legacy_list = self.get_gconfig(gid).get("categories", [])
            if any(entry.lower() == name.lower() for entry in legacy_list):
                return await interaction.followup.send(
                    "That leaderboard was created before the refactor. Please recreate it with /leaderboard setleaderboard.",
                    ephemeral=True,
                )
            return await interaction.followup.send("That leaderboard is not configured.", ephemeral=True)
        channel_id = board_cfg.get("leaderboard_channel_id")
        message_id = board_cfg.get("leaderboard_message_id")
        if channel_id and message_id:
            channel = self.client.get_channel(channel_id)
            if channel:
                try:
                    message = await channel.fetch_message(message_id)
                    await message.delete()
                except Exception:
                    logger.debug("Failed to delete leaderboard message for %s in guild %s", name, gid)
        safe = normalize_category(name)
        gid_s = str(gid)
        self.players_data.get(gid_s, {}).pop(safe, None)
        self.bios.get(gid_s, {}).pop(safe, None)
        self.removed.get(gid_s, {}).pop(safe, None)
        bucket = self.active_fights.get(gid_s, {})
        if name in bucket:
            bucket.pop(name, None)
            await self.save_active_fights_for(gid)
        config_entry = self.guild_configs.setdefault(gid_s, {})
        config_entry.get("leaderboards", {}).pop(safe, None)
        legacy_categories = config_entry.setdefault("categories", [])
        config_entry["categories"] = [entry for entry in legacy_categories if entry.lower() != name.lower()]
        config_entry.setdefault("category_modes", {}).pop(safe, None)
        await asyncio.to_thread(self.storage.save_guild_configs, copy.deepcopy(self.guild_configs))
        await asyncio.to_thread(self.storage.delete_category, gid, name)
        await interaction.followup.send(f"Removed {name} from this server.", ephemeral=True)

    @leaderboard.command(name="editboard")
    @app_commands.describe(
        category="Leaderboard name",
        player="Player to edit stats for",
        elo="New Elo",
        wins="Wins",
        losses="Losses",
        new_name="Optional new name for the leaderboard",
        participant_role="Participant role",
        leaderboard_channel="Channel to post the leaderboard message",
        challenge_channel="Channel for issuing challenges",
        outgoing_channel="Channel for outgoing challenge posts",
        announcement_channel="Channel for announcements",
        thread_cleanup_hours="Hours before challenge threads are cleaned up",
        mode_target="Winning score for First to X",
    )
    @app_commands.autocomplete(category=category_autocomplete)
    @app_commands.choices(mode=MODE_TYPE_CHOICES)
    async def editboard(
        self,
        interaction: discord.Interaction,
        category: str,
        player: Optional[discord.Member] = None,
        elo: Optional[float] = None,
        wins: Optional[int] = None,
        losses: Optional[int] = None,
        new_name: Optional[str] = None,
        participant_role: Optional[discord.Role] = None,
        leaderboard_channel: Optional[discord.TextChannel] = None,
        challenge_channel: Optional[discord.TextChannel] = None,
        outgoing_channel: Optional[discord.TextChannel] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        thread_cleanup_hours: Optional[float] = None,
        mode: Optional[app_commands.Choice[str]] = None,
        mode_target: Optional[int] = None,
    ):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission.", ephemeral=True)
        gid = interaction.guild.id
        board_cfg = self.get_leaderboard_config(gid, category)
        if not board_cfg:
            return await interaction.followup.send("That leaderboard is not configured.", ephemeral=True)
        updates = []
        current_name = category
        gid_s = str(gid)
        if new_name:
            new_name_clean = new_name.strip()
            if not new_name_clean:
                return await interaction.followup.send("New name cannot be empty.", ephemeral=True)
            if self.get_leaderboard_config(gid, new_name_clean):
                return await interaction.followup.send("A leaderboard with that name already exists.", ephemeral=True)
            old_safe = normalize_category(category)
            new_safe = normalize_category(new_name_clean)
            try:
                data_map = self.players_data.setdefault(gid_s, {})
                if old_safe in data_map:
                    data_map[new_safe] = data_map.pop(old_safe)
                else:
                    data_map.setdefault(new_safe, {})
                bios_map = self.bios.setdefault(gid_s, {})
                if old_safe in bios_map:
                    bios_map[new_safe] = bios_map.pop(old_safe)
                removed_map = self.removed.setdefault(gid_s, {})
                if old_safe in removed_map:
                    removed_map[new_safe] = removed_map.pop(old_safe)
                fights_map = self.active_fights.setdefault(gid_s, {})
                if current_name in fights_map:
                    fights_map[new_name_clean] = fights_map.pop(current_name)
                    new_bucket = fights_map[new_name_clean]
                    for match in new_bucket.get("matches", {}).values():
                        match["leaderboard"] = new_safe
                    await self.save_active_fights_for(gid)
                await asyncio.to_thread(self.storage.rename_category, gid, category, new_name_clean)
                boards = self.guild_configs.setdefault(gid_s, {}).setdefault("leaderboards", {})
                board_data = boards.pop(old_safe, board_cfg)
                board_data["name"] = new_name_clean
                boards[new_safe] = board_data
                modes_map = self.guild_configs.setdefault(gid_s, {}).setdefault("category_modes", {})
                if old_safe in modes_map:
                    modes_map[new_safe] = modes_map.pop(old_safe)
                legacy_categories = self.guild_configs.setdefault(gid_s, {}).setdefault("categories", [])
                replaced = False
                for idx, existing in enumerate(legacy_categories):
                    if existing.lower() == category.lower():
                        legacy_categories[idx] = new_name_clean
                        replaced = True
                        break
                if not replaced:
                    legacy_categories.append(new_name_clean)
                await asyncio.to_thread(self.storage.save_guild_configs, copy.deepcopy(self.guild_configs))
                current_name = new_name_clean
                board_cfg = board_data
                updates.append(f"renamed to {new_name_clean}")
            except Exception:
                logger.exception("Failed renaming leaderboard %s to %s in guild %s", category, new_name_clean, gid)
                return await interaction.followup.send("Unable to rename the leaderboard.", ephemeral=True)
        config_updates: Dict[str, Any] = {}
        if participant_role:
            board_cfg["participant_role_id"] = participant_role.id
            config_updates["participant_role_id"] = participant_role.id
            updates.append(f"participant role set to {participant_role.mention}")
        if challenge_channel:
            board_cfg["challenge_channel_id"] = challenge_channel.id
            config_updates["challenge_channel_id"] = challenge_channel.id
            updates.append(f"challenge channel set to {challenge_channel.mention}")
        if outgoing_channel:
            board_cfg["outgoing_channel_id"] = outgoing_channel.id
            config_updates["outgoing_channel_id"] = outgoing_channel.id
            updates.append(f"outgoing channel set to {outgoing_channel.mention}")
        if announcement_channel:
            board_cfg["announce_channel_id"] = announcement_channel.id
            config_updates["announce_channel_id"] = announcement_channel.id
            updates.append(f"announcements channel set to {announcement_channel.mention}")
        if thread_cleanup_hours is not None:
            try:
                cleanup_seconds = max(0, int(float(thread_cleanup_hours) * 3600))
            except Exception:
                return await interaction.followup.send("Invalid value for thread cleanup hours.", ephemeral=True)
            board_cfg["thread_cleanup_seconds"] = cleanup_seconds
            config_updates["thread_cleanup_seconds"] = cleanup_seconds
            updates.append(f"thread cleanup set to {thread_cleanup_hours:.1f}h")
        if mode is not None or mode_target is not None:
            current_mode = self.get_category_mode(gid, current_name)
            selected_key = mode.value if mode else current_mode["key"]
            if selected_key not in MODES:
                return await interaction.followup.send("Unknown mode selection.", ephemeral=True)
            target_value: Optional[int]
            template = MODES[selected_key]
            if template["type"] == "score":
                if mode_target is not None:
                    if mode_target < 1:
                        return await interaction.followup.send("Winning score must be at least 1.", ephemeral=True)
                    target_value = int(mode_target)
                else:
                    target_value = current_mode.get("target") or template.get("default_target", 1)
            else:
                target_value = None
            info = self.set_category_mode(gid, current_name, selected_key, target_value)
            board_cfg["mode"] = {"key": info["key"], "target": info["target"]}
            config_updates["mode"] = {"key": info["key"], "target": info["target"]}
            updates.append(f"mode set to {self.mode_label(info)}")
        leaderboard_moved = False
        if leaderboard_channel:
            old_channel_id = board_cfg.get("leaderboard_channel_id")
            old_message_id = board_cfg.get("leaderboard_message_id")
            if old_channel_id == leaderboard_channel.id and old_message_id:
                await self.update_leaderboard_message_for(gid, current_name)
                updates.append(f"refreshed leaderboard in {leaderboard_channel.mention}")
            else:
                view = self.build_leaderboard_view(gid, current_name)
                embed = view.create_embed()
                try:
                    message = await leaderboard_channel.send(embed=embed, view=view)
                except Exception:
                    logger.exception("Failed to move leaderboard message for %s in guild %s", current_name, gid)
                    return await interaction.followup.send("Unable to post the leaderboard in the new channel.", ephemeral=True)
                try:
                    self.client.add_view(view, message_id=message.id)
                except Exception:
                    logger.debug("Failed to register leaderboard view for %s in guild %s", current_name, gid)
                board_cfg["leaderboard_channel_id"] = leaderboard_channel.id
                board_cfg["leaderboard_message_id"] = message.id
                config_updates["leaderboard_channel_id"] = leaderboard_channel.id
                config_updates["leaderboard_message_id"] = message.id
                updates.append(f"leaderboard moved to {leaderboard_channel.mention}")
                leaderboard_moved = True
                if old_channel_id and old_message_id and old_channel_id != leaderboard_channel.id:
                    old_channel = self.client.get_channel(old_channel_id)
                    if isinstance(old_channel, discord.TextChannel):
                        try:
                            old_message = await old_channel.fetch_message(old_message_id)
                            await old_message.delete()
                        except Exception:
                            logger.debug("Failed to delete old leaderboard message for %s in guild %s", current_name, gid)
        if config_updates:
            board_cfg = self.upsert_leaderboard_config(gid, current_name, board_cfg)
        stat_fields = [elo, wins, losses]
        if any(value is not None for value in stat_fields):
            if player is None:
                return await interaction.followup.send("Specify a player when changing stats.", ephemeral=True)
            players = self.load_players_for(gid, current_name)
            pdata = players.setdefault(player.id, {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0})
            if elo is not None:
                if elo < 0:
                    return await interaction.followup.send("Elo cannot be negative.", ephemeral=True)
                pdata["elo"] = elo
            if wins is not None:
                if wins < 0:
                    return await interaction.followup.send("Wins cannot be negative.", ephemeral=True)
                pdata["wins"] = wins
            if losses is not None:
                if losses < 0:
                    return await interaction.followup.send("Losses cannot be negative.", ephemeral=True)
                pdata["losses"] = losses
            safe_current = normalize_category(current_name)
            self.players_data.setdefault(gid_s, {})[safe_current] = players
            await self.save_players_for(gid, current_name)
            total = pdata["wins"] + pdata["losses"]
            winrate = pdata["wins"] / total * 100 if total else 0.0
            updates.append(f"updated {player.display_name}'s stats (Elo {pdata['elo']:.1f}, W:{pdata['wins']}, L:{pdata['losses']} - {winrate:.1f}%)")
        if not updates:
            await interaction.followup.send("No changes provided.", ephemeral=True)
            return
        if not leaderboard_moved:
            await self.update_leaderboard_message_for(gid, current_name)
        await interaction.followup.send("Changes applied: " + "; ".join(updates), ephemeral=True)
    @challenge_group.command(name="anyone")
    @app_commands.describe(category="Leaderboard name", rank_range="Rank window", automatch="Try to auto-match with an existing open challenge")
    @app_commands.autocomplete(category=category_autocomplete)
    async def challenge_anyone(self, interaction: discord.Interaction, category: str, rank_range: Optional[int] = None, automatch: Optional[bool] = False):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        guild = interaction.guild
        gid = guild.id
        board_cfg = self.get_leaderboard_config(gid, category)
        if not board_cfg:
            return await interaction.followup.send("Leaderboard not configured. Use /leaderboard setleaderboard first.", ephemeral=True)
        role_id = board_cfg.get("participant_role_id")
        if role_id and role_id not in [role.id for role in interaction.user.roles]:
            return await interaction.followup.send("You need the participant role to issue challenges.", ephemeral=True)
        if self.find_active_match_for(gid, interaction.user.id):
            return await interaction.followup.send("You already have an active challenge.", ephemeral=True)
        if automatch:
            bucket = self.get_active_bucket(gid, category)
            challenger_rank = self.get_player_rank(gid, category, interaction.user.id)
            for mid, m in list(bucket.get("matches", {}).items()):
                if m.get("status") != "open":
                    continue
                if m.get("opponent_id"):
                    continue
                if m.get("challenger_id") == interaction.user.id:
                    continue
                other_rank = self.get_player_rank(gid, category, m.get("challenger_id"))
                if challenger_rank and other_rank:
                    diff = abs(challenger_rank[0] - other_rank[0])
                    window_ok = True
                    if m.get("rank_range"):
                        window_ok = window_ok and diff <= int(m.get("rank_range"))
                    if rank_range:
                        window_ok = window_ok and diff <= int(rank_range)
                    if not window_ok:
                        continue
                m["opponent_id"] = interaction.user.id
                m["status"] = "awaiting_result"
                m.pop("response_deadline", None)
                m["accepted_at"] = datetime.now(timezone.utc).isoformat()
                m["cancel_votes"] = []
                mode_payload = self.normalize_mode_value(m.get("mode") or board_cfg.get("mode") or {"key": "speedrun"})
                m["mode"] = {"key": mode_payload["key"], "target": mode_payload["target"]}
                bucket["matches"][mid] = m
                await self.save_active_fights_for(gid)
                channel = self.client.get_channel(m.get("channel_id"))
                message = None
                if channel:
                    try:
                        message = await channel.fetch_message(m.get("message_id"))
                    except Exception:
                        message = None
                if message:
                    await self.ensure_match_thread(guild, category, m, message)
                await self.refresh_match_message(gid, category, mid)
                gid_s = str(gid)
                self.players_meta.setdefault(gid_s, {})[str(interaction.user.id)] = {
                    "name": interaction.user.display_name,
                    "avatar": interaction.user.avatar.url if interaction.user.avatar else None,
                }
                await self.persist_player_meta(gid)
                return await interaction.followup.send("Matched with an open challenge.", ephemeral=True)
        outgoing_channel_id = board_cfg.get("outgoing_channel_id") or board_cfg.get("challenge_channel_id")
        if not outgoing_channel_id:
            return await interaction.followup.send("No outgoing or challenge channel configured for this leaderboard.", ephemeral=True)
        channel = self.client.get_channel(outgoing_channel_id)
        if channel is None:
            return await interaction.followup.send("Configured channel could not be found.", ephemeral=True)
        match_id = uuid.uuid4().hex
        mode_info = self.normalize_mode_value(board_cfg.get("mode") or self.get_category_mode(gid, category))
        match_data = {
            "id": match_id,
            "leaderboard": normalize_category(category),
            "challenger_id": interaction.user.id,
            "opponent_id": None,
            "status": "open",
            "channel_id": channel.id,
            "message_id": None,
            "thread_id": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "rank_range": int(rank_range) if rank_range else None,
            "mode": {"key": mode_info["key"], "target": mode_info["target"]},
            "submissions": {},
            "cancel_votes": [],
        }
        embed = self.build_match_embed(guild, category, match_data)
        message = await channel.send(embed=embed)
        match_data["message_id"] = message.id
        bucket = self.get_active_bucket(gid, category)
        bucket["matches"][match_id] = match_data
        await self.save_active_fights_for(gid)
        view = self.build_match_view(gid, category, match_id)
        view.refresh_buttons()
        embed = self.build_match_embed(guild, category, match_data)
        await message.edit(embed=embed, view=view)
        try:
            self.client.add_view(view, message_id=message.id)
        except Exception:
            logger.debug("Failed to register view for match %s in guild %s", match_id, gid)
        self.players_meta.setdefault(str(gid), {})[str(interaction.user.id)] = {
            "name": interaction.user.display_name,
            "avatar": interaction.user.avatar.url if interaction.user.avatar else None,
        }
        await self.persist_player_meta(gid)
        await interaction.followup.send(f"Challenge posted in {channel.mention}.", ephemeral=True)

    @challenge_group.command(name="cancel")
    @app_commands.describe(category="Leaderboard name")
    @app_commands.autocomplete(category=category_autocomplete)
    async def challenge_cancel(self, interaction: discord.Interaction, category: str):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            await interaction.followup.send("Server-only command.", ephemeral=True)
            return
        gid = interaction.guild.id
        bucket = self.get_active_bucket(gid, category)
        target_match: Optional[Tuple[str, Dict[str, Any]]] = None
        for match_id, data in bucket.get("matches", {}).items():
            if data.get("challenger_id") == interaction.user.id and data.get("status") == "open" and not data.get("opponent_id"):
                target_match = (match_id, data)
                break
        if not target_match:
            await interaction.followup.send("You do not have an open queue for this leaderboard.", ephemeral=True)
            return
        match_id, _ = target_match
        await self.cancel_active_match(gid, category, match_id)
        await interaction.followup.send("Your open challenge has been cancelled.", ephemeral=True)

    @challenge_group.command(name="opponent")
    @app_commands.describe(category="Leaderboard name", opponent="Specific opponent to challenge")
    @app_commands.autocomplete(category=category_autocomplete)
    async def challenge_opponent(self, interaction: discord.Interaction, category: str, opponent: discord.Member):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        guild = interaction.guild
        gid = guild.id
        board_cfg = self.get_leaderboard_config(gid, category)
        if not board_cfg:
            return await interaction.followup.send("Leaderboard not configured. Use /leaderboard setleaderboard first.", ephemeral=True)
        role_id = board_cfg.get("participant_role_id")
        if role_id and role_id not in [role.id for role in interaction.user.roles]:
            return await interaction.followup.send("You need the participant role to issue challenges.", ephemeral=True)
        if self.find_active_match_for(gid, interaction.user.id):
            return await interaction.followup.send("You already have an active challenge.", ephemeral=True)
        if opponent.id == interaction.user.id:
            return await interaction.followup.send("You cannot challenge yourself.", ephemeral=True)
        if role_id and role_id not in [role.id for role in opponent.roles]:
            return await interaction.followup.send(f"{opponent.display_name} is not registered for this leaderboard.", ephemeral=True)
        if self.find_active_match_for(gid, opponent.id):
            return await interaction.followup.send(f"{opponent.display_name} already has an active challenge.", ephemeral=True)
        outgoing_channel_id = board_cfg.get("outgoing_channel_id") or board_cfg.get("challenge_channel_id")
        if not outgoing_channel_id:
            return await interaction.followup.send("No outgoing or challenge channel configured for this leaderboard.", ephemeral=True)
        channel = self.client.get_channel(outgoing_channel_id)
        if channel is None:
            return await interaction.followup.send("Configured channel could not be found.", ephemeral=True)
        match_id = uuid.uuid4().hex
        mode_info = self.normalize_mode_value(board_cfg.get("mode") or self.get_category_mode(gid, category))
        match_data = {
            "id": match_id,
            "leaderboard": normalize_category(category),
            "challenger_id": interaction.user.id,
            "opponent_id": opponent.id,
            "status": "pending",
            "channel_id": channel.id,
            "message_id": None,
            "thread_id": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "rank_range": None,
            "mode": {"key": mode_info["key"], "target": mode_info["target"]},
            "submissions": {},
            "cancel_votes": [],
            "response_deadline": (datetime.now(timezone.utc) + PENDING_CHALLENGE_TIMEOUT).isoformat(),
        }
        embed = self.build_match_embed(guild, category, match_data)
        allowed_mentions = discord.AllowedMentions(users=True)
        message = await channel.send(content=opponent.mention, embed=embed, allowed_mentions=allowed_mentions)
        match_data["message_id"] = message.id
        bucket = self.get_active_bucket(gid, category)
        bucket["matches"][match_id] = match_data
        await self.save_active_fights_for(gid)
        view = self.build_match_view(gid, category, match_id)
        view.refresh_buttons()
        embed = self.build_match_embed(guild, category, match_data)
        await message.edit(embed=embed, view=view)
        try:
            self.client.add_view(view, message_id=message.id)
        except Exception:
            logger.debug("Failed to register view for match %s in guild %s", match_id, gid)
        self.players_meta.setdefault(str(gid), {})[str(interaction.user.id)] = {
            "name": interaction.user.display_name,
            "avatar": interaction.user.avatar.url if interaction.user.avatar else None,
        }
        self.players_meta.setdefault(str(gid), {})[str(opponent.id)] = {
            "name": opponent.display_name,
            "avatar": opponent.avatar.url if opponent.avatar else None,
        }
        await self.persist_player_meta(gid)
        await interaction.followup.send(f"Challenge posted in {channel.mention}.", ephemeral=True)

    @leaderboard.command(name="removeplayer")
    @app_commands.describe(category="Leaderboard name", player="Player to remove")
    @app_commands.autocomplete(category=category_autocomplete)
    async def removeplayer(self, interaction: discord.Interaction, category: str, player: discord.Member):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission.", ephemeral=True)
        gid = interaction.guild.id
        board_cfg = self.get_leaderboard_config(gid, category)
        if not board_cfg:
            return await interaction.followup.send("That leaderboard is not configured.", ephemeral=True)
        players = self.load_players_for(gid, category)
        if player.id not in players:
            return await interaction.followup.send(f"{player.display_name} is not on this leaderboard.", ephemeral=True)
        removed_entry = players.pop(player.id, {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0})
        gid_s = str(gid)
        safe_cat = normalize_category(category)
        self.players_data.setdefault(gid_s, {})[safe_cat] = players
        removed_map = self.removed.setdefault(gid_s, {}).setdefault(safe_cat, {})
        removed_map[player.id] = removed_entry
        await self.save_players_for(gid, category)
        await self.update_leaderboard_message_for(gid, category)
        await interaction.followup.send(f"Removed {player.display_name} from {category}.", ephemeral=True)

    @leaderboard.command(name="purge-threads")
    @app_commands.describe(category="Optional leaderboard name to target; omit to scan all")
    @app_commands.autocomplete(category=category_autocomplete)
    async def purge_threads(self, interaction: discord.Interaction, category: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission.", ephemeral=True)
        gid = interaction.guild.id
        gid_s = str(gid)
        cat_map_all = self.active_fights.get(gid_s, {})
        if not cat_map_all:
            return await interaction.followup.send("No challenge threads to review.", ephemeral=True)
        target_categories: Dict[str, Dict[str, Any]] = {}
        if category:
            matched_key = None
            for existing_key in cat_map_all.keys():
                if existing_key.lower() == category.lower():
                    matched_key = existing_key
                    break
            if matched_key is None:
                return await interaction.followup.send("That leaderboard has no tracked threads.", ephemeral=True)
            target_categories[matched_key] = cat_map_all[matched_key]
        else:
            target_categories = cat_map_all
        statuses_inactive = {"completed", "cancelled", "disputed"}
        removed_threads: List[str] = []
        missing_threads: List[str] = []
        failures: List[str] = []
        for cat_key, cat_map in target_categories.items():
            matches = cat_map.get("matches", {})
            deletions = cat_map.get("deletions", [])
            updated_deletions = [entry for entry in deletions if entry.get("thread_id")]
            for match_id, match in list(matches.items()):
                thread_id = match.get("thread_id")
                status = match.get("status", "open")
                if not thread_id:
                    continue
                should_purge = status in statuses_inactive or match.get("message_id") is None
                if not should_purge:
                    continue
                thread = self.client.get_channel(thread_id)
                if isinstance(thread, discord.Thread):
                    if getattr(thread, "owner_id", None) != self.client.user.id:
                        continue
                    try:
                        await thread.delete()
                        removed_threads.append(f"{cat_key}:{match_id}")
                    except Exception:
                        failures.append(f"{cat_key}:{match_id}")
                        continue
                else:
                    missing_threads.append(f"{cat_key}:{match_id}")
                match["thread_id"] = None
                matches[match_id] = match
                updated_deletions = [entry for entry in updated_deletions if entry.get("thread_id") != thread_id]
            cat_map["matches"] = matches
            cat_map["deletions"] = updated_deletions
        await self.save_active_fights_for(gid)
        lines: List[str] = []
        if removed_threads:
            lines.append(f"Deleted {len(removed_threads)} inactive thread(s).")
        if missing_threads:
            lines.append(f"Cleared {len(missing_threads)} missing thread reference(s).")
        if failures:
            lines.append(f"Failed removing {len(failures)} thread(s); check logs.")
        if not lines:
            lines.append("No inactive bot threads found.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    async def active_autocomplete(self, interaction: discord.Interaction, current: str):
        if interaction.guild is None:
            return []
        choices = []
        lowered = current.lower()
        for m in interaction.guild.members:
            if lowered in m.display_name.lower():
                choices.append(app_commands.Choice(name=m.display_name, value=str(m.id)))
        return choices[:25]

    @leaderboard.command(name="profile")
    @app_commands.describe(member="Member to view")
    async def profile(self, interaction: discord.Interaction, member: Optional[discord.Member] = None):
        await interaction.response.defer(ephemeral=False)
        if interaction.guild is None:
            return await interaction.followup.send("Server-only command.", ephemeral=True)
        gid = interaction.guild.id
        target = member or interaction.user
        boards = self.list_leaderboards(gid)
        if not boards:
            return await interaction.followup.send("No leaderboards configured.", ephemeral=True)
        initial_board = await self.most_active_board(gid, boards, target.id)
        embed, pages = await self.build_profile_content(gid, initial_board, target)
        history = "\n".join(pages[0]) if pages else "No matches recorded."
        embed.description = history
        embed.set_footer(text=f"Page 1/{max(len(pages), 1)}")
        view = ProfileView(self, gid, target, boards, initial_board)
        view.pages = pages
        view.page_index = 0
        view.update_select_defaults()
        view._sync_buttons()
        await interaction.followup.send(embed=embed, view=view, ephemeral=False)

    @leaderboard.command(name="history")
    @app_commands.describe(category="Optional category to filter", player="Optional player to filter")
    @app_commands.autocomplete(category=category_autocomplete, player=member_autocomplete)
    async def history(self, interaction: discord.Interaction, category: Optional[str] = None, player: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)
        gid = interaction.guild.id
        boards: List[str] = []
        if category:
            boards = [category]
        else:
            boards.extend(self.list_leaderboards(gid))
            for name in self.storage.list_categories(gid):
                boards.append(name)
        boards = [name for name in dict.fromkeys(board for board in boards if board)]
        if not boards:
            return await interaction.followup.send("No categories available.", ephemeral=True)
        player_id = None
        if player is not None:
            try:
                player_id = int(player)
            except Exception:
                return await interaction.followup.send("Invalid player selection.", ephemeral=True)
        entries: List[Tuple[datetime, str]] = []
        for board_name in boards:
            try:
                rows = await asyncio.to_thread(self.storage.load_match_history, gid, board_name)
            except Exception:
                continue
            for row in rows:
                result_token = row.get("result", "")
                if result_token not in ["Win", "DeclineWin"]:
                    continue
                try:
                    winner_id = int(row["user_id"])
                    loser_id = int(row["opponent_id"])
                except Exception:
                    continue
                if player_id is not None and player_id not in {winner_id, loser_id}:
                    continue
                formatted = self.format_match_entry(gid, board_name, row, perspective_id=None, include_category=True)
                if formatted:
                    entries.append(formatted)
        if not entries:
            return await interaction.followup.send("No matches found for the selected filters.", ephemeral=True)
        entries.sort(key=lambda item: item[0], reverse=True)
        lines = [item[1] for item in entries]
        pages = chunk_list(lines, 10)
        header_parts = []
        if category:
            header_parts.append(f"Category filter: {category}")
        if player_id:
            member = interaction.guild.get_member(player_id) or self.client.get_user(player_id)
            header_parts.append(f"Player filter: {member.display_name if member else self.user_snapshot_name_for(gid, player_id)}")
        header_text = "\n".join(header_parts) if header_parts else None
        view = PagedListView(
            title="Server Match History",
            pages=pages,
            color=discord.Color.blue(),
            footer_note=f"{len(entries)} matches",
            header=header_text,
        )
        await interaction.followup.send(embed=view.create_embed(), view=view, ephemeral=True)

    @leaderboard.command(name="profilebio")
    @app_commands.describe(bio="Your bio (<=100 chars)")
    async def profilebio(self, interaction: discord.Interaction, bio: str):
        await interaction.response.defer(ephemeral=True)
        if len(bio) > 100:
            return await interaction.followup.send("Bio must be <=100 chars.", ephemeral=True)
        gid = interaction.guild.id
        gid_s = str(gid)
        self.bios.setdefault(gid_s, {}).setdefault(GLOBAL_BIO_KEY, {})[str(interaction.user.id)] = bio
        bios_snapshot = copy.deepcopy(self.bios[gid_s][GLOBAL_BIO_KEY])
        await asyncio.to_thread(
            self.storage.save_bios,
            gid,
            GLOBAL_BIO_KEY,
            bios_snapshot,
        )
        await interaction.followup.send("Bio updated.", ephemeral=True)

    @leaderboard.command(name="activefights")
    @app_commands.describe(category="Category", scope="'all' or 'personal'", target="Member ID (if personal)")
    @app_commands.autocomplete(category=category_autocomplete, scope=scope_autocomplete, target=member_autocomplete)
    async def activefights(self, interaction: discord.Interaction, category: str, scope: str, target: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)
        scope = scope.lower()
        if scope not in ["all", "personal"]:
            return await interaction.followup.send("Scope must be 'all' or 'personal'.", ephemeral=True)
        gid = interaction.guild.id
        bucket = self.get_active_bucket(gid, category)
        matches = bucket.get("matches", {})
        fights_data: List[Tuple[int, Optional[int], str, str]] = []
        guild = interaction.guild
        for match in matches.values():
            status = match.get("status", "open")
            if status in {"completed", "cancelled"}:
                continue
            challenger_id = match.get("challenger_id")
            opponent_id = match.get("opponent_id")
            created_at = match.get("created_at")
            fights_data.append((challenger_id, opponent_id, created_at, status))
        member_obj = None
        if scope == "personal":
            member_obj = guild.get_member(int(target)) if target else interaction.user
            if member_obj is None:
                return await interaction.followup.send("Member not found.", ephemeral=True)
            fights_data = [data for data in fights_data if member_obj.id in {data[0], data[1]}]
        fights_lines = []
        for challenger_id, opponent_id, recorded_at, status in fights_data:
            try:
                started = datetime.fromisoformat(recorded_at) if recorded_at else datetime.now(timezone.utc)
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
            except Exception:
                started = datetime.now(timezone.utc)
            start_time = started.astimezone(TZ).strftime("%m/%d/%Y %H:%M")
            challenger = guild.get_member(challenger_id) or self.client.get_user(challenger_id)
            challenger_label = challenger.display_name if isinstance(challenger, discord.Member) else getattr(challenger, "name", self.user_snapshot_name_for(gid, challenger_id))
            if opponent_id:
                opponent = guild.get_member(opponent_id) or self.client.get_user(opponent_id)
                opponent_label = opponent.display_name if isinstance(opponent, discord.Member) else getattr(opponent, "name", self.user_snapshot_name_for(gid, opponent_id))
            else:
                opponent_label = "Awaiting opponent"
            status_text = status.replace("_", " ").title()
            fights_lines.append(f"[{category}] {challenger_label} vs {opponent_label} | {status_text} | Started: {start_time}")
        if not fights_lines:
            return await interaction.followup.send("No active runs found.", ephemeral=True)
        pages = chunk_list(fights_lines, 10)
        avatar_url = member_obj.avatar.url if member_obj and member_obj.avatar else None
        title = f"Active Runs for {member_obj.display_name}" if member_obj else "All Active Runs"
        view = PagedListView(
            title=title,
            pages=pages,
            color=discord.Color.blue(),
            footer_note=f"{len(fights_lines)} runs",
            thumbnail=avatar_url,
        )
        await interaction.followup.send(embed=view.create_embed(), view=view, ephemeral=True)

    @leaderboard.command(name="cancelfight")
    @app_commands.describe(category="Category", player1="Player 1", player2="Player 2")
    @app_commands.autocomplete(category=category_autocomplete)
    async def cancelfight(self, interaction: discord.Interaction, category: str, player1: discord.Member, player2: discord.Member):
        await interaction.response.defer()
        gid = interaction.guild.id
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission.", ephemeral=True)
        bucket = self.get_active_bucket(gid, category)
        matches = bucket.get("matches", {})
        target_ids = {player1.id, player2.id}
        removed_any = False
        for match_id, data in list(matches.items()):
            participants = {data.get("challenger_id"), data.get("opponent_id")}
            if None in participants:
                participants.discard(None)
            if participants == target_ids:
                thread_id = data.get("thread_id")
                matches.pop(match_id, None)
                if thread_id:
                    await self.schedule_thread_deletion(gid, thread_id, category)
                removed_any = True
        if not removed_any:
            return await interaction.followup.send("No active run found.", ephemeral=True)
        await self.save_active_fights_for(gid)
        await interaction.followup.send(f"Cancelled run between {player1.display_name} and {player2.display_name}.", ephemeral=True)

    async def removed_autocomplete(self, interaction: discord.Interaction, current: str):
        if interaction.guild is None:
            return []
        gid = interaction.guild.id
        choices = []
        gid_s = str(gid)
        for entries in self.removed.get(gid_s, {}).values():
            for uid in entries.keys():
                member = interaction.guild.get_member(uid) or self.client.get_user(uid)
                name = member.display_name if member else self.user_snapshot_name_for(gid, uid)
                if current.lower() in name.lower():
                    choices.append(app_commands.Choice(name=name, value=str(uid)))
                    if len(choices) >= 25:
                        break
            if len(choices) >= 25:
                break
        return choices[:25]

    @leaderboard.command(name="readd")
    @app_commands.describe(category="Category", player="Player to re-add")
    @app_commands.autocomplete(category=category_autocomplete, player=removed_player_autocomplete)
    async def readd(self, interaction: discord.Interaction, category: str, player: str):
        await interaction.response.defer()
        gid = interaction.guild.id
        if not self.has_mod_permissions(interaction.user):
            return await interaction.followup.send("You do not have permission.", ephemeral=True)
        try:
            uid = int(player)
        except Exception:
            return await interaction.followup.send("Invalid player selection.", ephemeral=True)
        gid_s = str(gid)
        safe_cat = normalize_category(category)
        removed_map = self.removed.setdefault(gid_s, {}).setdefault(safe_cat, {})
        if uid not in removed_map:
            return await interaction.followup.send("That player is not currently removed.", ephemeral=True)
        player_data = removed_map.pop(uid) or {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0}
        players = self.load_players_for(gid, category)
        players[uid] = player_data
        self.players_data.setdefault(gid_s, {})[safe_cat] = players
        await self.save_players_for(gid, category)
        member = self.client.get_user(uid)
        data = players[uid]
        await self.update_leaderboard_message_for(gid, category)
        await interaction.followup.send(f"Re-added {member.display_name if member else uid} to {category} with Elo {data['elo']:.1f}, W:{data['wins']}, L:{data['losses']}.", ephemeral=True)

    async def schedule_thread_deletion(self, gid: int, thread_id: int, category: str):
        bucket = self.get_active_bucket(gid, category)
        board_cfg = self.get_leaderboard_config(gid, category)
        delay = int(board_cfg.get("thread_cleanup_seconds", 21600)) if board_cfg else 21600
        delete_at = (datetime.now(timezone.utc) + timedelta(seconds=max(0, delay))).isoformat()
        deletions = bucket.setdefault("deletions", [])
        for entry in deletions:
            if entry.get("thread_id") == thread_id:
                entry["delete_at"] = delete_at
                break
        else:
            deletions.append({"thread_id": thread_id, "delete_at": delete_at})
        await self.save_active_fights_for(gid)

    async def save_active_fights_for(self, gid: int):
        gid_s = str(gid)
        payload = copy.deepcopy(self.active_fights.get(gid_s, {}))
        await asyncio.to_thread(self.storage.save_active_fights, gid, payload)

    def _schedule_config_save(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self.storage.save_guild_configs(copy.deepcopy(self.guild_configs))
            return
        existing = self._config_save_task
        if existing and not existing.done():
            self._config_save_pending = True
            return

        async def runner():
            await asyncio.to_thread(self.storage.save_guild_configs, copy.deepcopy(self.guild_configs))

        task = loop.create_task(runner())
        self._config_save_task = task

        def _cleanup(t: asyncio.Task):
            self._config_save_task = None
            try:
                t.result()
            except Exception:
                logger.exception("Failed saving guild configs")
            if self._config_save_pending:
                self._config_save_pending = False
                self._schedule_config_save()

        task.add_done_callback(_cleanup)

    def _schedule_active_fights_save(self, gid: int) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop; fall back to synchronous save.
            asyncio.run(self.save_active_fights_for(gid))
            return
        existing = self._active_fight_save_tasks.get(gid)
        if existing and not existing.done():
            return

        async def runner():
            await self.save_active_fights_for(gid)

        task = loop.create_task(runner())
        self._active_fight_save_tasks[gid] = task

        def _cleanup(t: asyncio.Task, *, guild_id: int) -> None:
            self._active_fight_save_tasks.pop(guild_id, None)
            try:
                t.result()
            except Exception:
                logger.exception("Failed saving active matches for guild %s", guild_id)

        task.add_done_callback(functools.partial(_cleanup, guild_id=gid))

    async def _deletion_loop(self):
        while True:
            await asyncio.sleep(30)
            now = datetime.now(timezone.utc)
            for gid_s, af in list(self.active_fights.items()):
                gid = int(gid_s)
                changed = False
                for category, cat_map in list(af.items()):
                    deletions = cat_map.get("deletions", [])
                    category_changed = False
                    for entry in list(deletions):
                        try:
                            delete_at = datetime.fromisoformat(entry["delete_at"])
                            if delete_at.tzinfo is None:
                                delete_at = delete_at.replace(tzinfo=timezone.utc)
                        except Exception:
                            deletions.remove(entry)
                            category_changed = True
                            changed = True
                            continue
                        if delete_at <= now:
                            tid = entry.get("thread_id")
                            if tid:
                                thread = self.client.get_channel(tid)
                                if thread:
                                    try:
                                        await thread.delete()
                                    except Exception:
                                        logger.exception("Failed deleting thread %s for guild %s", tid, gid_s)
                                for match_entry in cat_map.get("matches", {}).values():
                                    if match_entry.get("thread_id") == tid:
                                        match_entry.pop("thread_id", None)
                                        match_entry.pop("thread_message_id", None)
                            deletions.remove(entry)
                            category_changed = True
                            changed = True
                    if category_changed:
                        cat_map["deletions"] = deletions
                    matches = cat_map.get("matches", {})
                    for match_id, match_data in list(matches.items()):
                        status = match_data.get("status")
                        opponent_id = match_data.get("opponent_id")
                        if status == "pending" and opponent_id:
                            deadline_raw = match_data.get("response_deadline")
                            deadline_dt = None
                            if deadline_raw:
                                try:
                                    deadline_dt = datetime.fromisoformat(deadline_raw)
                                    if deadline_dt.tzinfo is None:
                                        deadline_dt = deadline_dt.replace(tzinfo=timezone.utc)
                                except Exception:
                                    deadline_dt = None
                            if deadline_dt is None:
                                created_raw = match_data.get("created_at")
                                created_dt = None
                                if created_raw:
                                    try:
                                        created_dt = datetime.fromisoformat(created_raw)
                                        if created_dt.tzinfo is None:
                                            created_dt = created_dt.replace(tzinfo=timezone.utc)
                                    except Exception:
                                        created_dt = None
                                if created_dt is None:
                                    created_dt = now
                                deadline_dt = created_dt + PENDING_CHALLENGE_TIMEOUT
                                match_data["response_deadline"] = deadline_dt.isoformat()
                                category_changed = True
                                changed = True
                            if deadline_dt and deadline_dt <= now:
                                challenger_id = match_data.get("challenger_id")
                                channel_id = match_data.get("channel_id")
                                channel = self.client.get_channel(channel_id) if channel_id else None
                                notify_text = "Challenge expired due to no response."
                                if challenger_id and opponent_id:
                                    challenger_mention = f"<@{challenger_id}>"
                                    opponent_mention = f"<@{opponent_id}>"
                                    notify_text = f"Challenge between {challenger_mention} and {opponent_mention} expired after no response."
                                if channel:
                                    try:
                                        await channel.send(
                                            notify_text,
                                            allowed_mentions=discord.AllowedMentions(users=True),
                                        )
                                    except Exception:
                                        logger.debug("Failed to post timeout notice for match %s in guild %s", match_id, gid_s)
                                await self.cancel_active_match(gid, category, match_id)
                                changed = True
                if changed:
                    await self.save_active_fights_for(int(gid_s))
            await asyncio.sleep(0)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if before.guild.id != after.guild.id:
            return
        before_ids = {role.id for role in before.roles}
        after_ids = {role.id for role in after.roles}
        removed_roles = before_ids - after_ids
        added_roles = after_ids - before_ids
        if not removed_roles and not added_roles:
            return
        guild = before.guild
        gid = guild.id
        cfg = self.get_gconfig(gid)
        boards = cfg.get("leaderboards", {})
        if not boards:
            return
        gid_s = str(gid)
        removed_categories: List[str] = []
        restored_categories: List[str] = []
        for safe_name, data in boards.items():
            role_id = data.get("participant_role_id")
            if not role_id:
                continue
            category_name = data.get("name", safe_name.replace("_", " ").title())
            if role_id in removed_roles:
                players = self.load_players_for(gid, category_name)
                if before.id not in players:
                    continue
                removed_map = self.removed.setdefault(gid_s, {}).setdefault(safe_name, {})
                removed_map[before.id] = players.pop(before.id)
                self.players_data.setdefault(gid_s, {})[safe_name] = players
                await self.save_players_for(gid, category_name)
                removed_categories.append(category_name)
            elif role_id in added_roles:
                removed_map = self.removed.setdefault(gid_s, {}).setdefault(safe_name, {})
                if before.id not in removed_map:
                    continue
                restored_data = removed_map.pop(before.id) or {"elo": DEFAULT_START_ELO, "wins": 0, "losses": 0}
                players = self.load_players_for(gid, category_name)
                players[before.id] = restored_data
                self.players_data.setdefault(gid_s, {})[safe_name] = players
                await self.save_players_for(gid, category_name)
                restored_categories.append(category_name)
        for category in set(removed_categories + restored_categories):
            await self.update_leaderboard_message_for(gid, category)
        if removed_categories:
            logger.info("Member %s removed from %s due to participant role removal in guild %s", before.id, ", ".join(removed_categories), gid)
        if restored_categories:
            logger.info("Member %s restored to %s due to participant role addition in guild %s", before.id, ", ".join(restored_categories), gid)

    @commands.Cog.listener()
    async def on_ready(self):
        if self._cleanup_task is None:
            self._cleanup_task = self.client.loop.create_task(self._deletion_loop())
        restored = 0
        for gid_s, cat_map in self.active_fights.items():
            try:
                gid = int(gid_s)
            except Exception:
                continue
            for category, payload in cat_map.items():
                matches = payload.get("matches", {})
                for match_id, match in matches.items():
                    message_id = match.get("message_id")
                    if not message_id:
                        continue
                    view = self.build_match_view(gid, category, match_id)
                    view.refresh_buttons()
                    try:
                        self.client.add_view(view, message_id=message_id)
                        restored += 1
                    except Exception:
                        logger.debug("Failed to restore view for guild %s match %s", gid_s, match_id)
                    match.pop("thread_message_id", None)
        if restored:
            logger.info("Restored %s challenge controls after reconnect.", restored)

async def setup(client: commands.Bot):
    await client.add_cog(LeaderboardCog(client))
