import asyncio
import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Literal

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands


API_BASE = "https://www.speedrun.com/api/v1"
DB_FILE = "data/src_watchers.sqlite3"
ALL_CATEGORIES_VALUE = "__all__"


def ensure_db():
    import os
    os.makedirs("data", exist_ok=True)
    con = sqlite3.connect(DB_FILE)
    try:
        with con:
            con.execute(
                "CREATE TABLE IF NOT EXISTS watchers (\n"
                "  guild_id INTEGER NOT NULL,\n"
                "  channel_id INTEGER NOT NULL,\n"
                "  role_id INTEGER NOT NULL,\n"
                "  game_id TEXT NOT NULL,\n"
                "  category_id TEXT NOT NULL,\n"
                "  last_checked TEXT NOT NULL,\n"
                "  last_seen_ids TEXT NOT NULL,\n"
                "  PRIMARY KEY (guild_id, channel_id, game_id, category_id)\n"
                ")"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_watchers_channel ON watchers(channel_id)"
            )
    finally:
        con.close()


def format_duration(seconds: float) -> str:
    if seconds is None:
        return "?"
    seconds = float(seconds)
    ms = int(round((seconds - int(seconds)) * 1000))
    total = int(seconds)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h:d}:{m:02d}:{s:02d}.{ms:03d}"
    return f"{m:d}:{s:02d}.{ms:03d}"


async def game_autocomplete(interaction: discord.Interaction, current: str):
    cog = interaction.client.get_cog("SRCLeaderboardCog")
    if cog is None:
        return []
    term = (current or "").strip()
    if not term:
        term = "minecraft"
    try:
        games = await cog.src_search_games_cached(term)
    except Exception:
        return []
    choices = []
    for g in games[:25]:
        game_id = g.get("id")
        if not game_id:
            continue
        name = g.get("names", {}).get("international") or g.get("name") or game_id
        if name:
            choices.append(app_commands.Choice(name=name, value=str(game_id)))
    return choices[:25]


async def category_autocomplete(interaction: discord.Interaction, current: str):
    cog = interaction.client.get_cog("SRCLeaderboardCog")
    if cog is None:
        return []
    ns = getattr(interaction, "namespace", None)
    game_id = None
    if ns is not None:
        game_id = getattr(ns, "game", None)
    if not game_id:
        return []
    try:
        cats = await cog.src_get_game_categories_cached(game_id)
    except Exception:
        return []
    lowered = (current or "").lower()
    choices = []
    for c in cats:
        cat_id = c.get("id")
        if not cat_id:
            continue
        name = c.get("name")
        if not name:
            continue
        if lowered and lowered not in name.lower():
            continue
        choices.append(app_commands.Choice(name=name, value=str(cat_id)))
        if len(choices) >= 25:
            break
    return choices


class LeaderboardView(discord.ui.View):
    def __init__(self, user_id: int, make_embed, on_category_change, categories: List[Dict[str, Any]], page_count: int, current_category_id: Optional[str], *, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.make_embed = make_embed
        self.on_category_change = on_category_change
        self.page = 1
        self.total_pages = max(1, int(page_count))
        self.categories = categories
        self.current_category_id = current_category_id if current_category_id is not None else ""
        self.category_select = discord.ui.Select(placeholder="Select category", min_values=1, max_values=1)
        for c in categories[:25]:
            raw_value = c.get("id")
            if raw_value is None:
                continue
            value = str(raw_value)
            if not value or len(value) > 100:
                continue
            label_raw = c.get("name") or value
            label = str(label_raw)[:100]
            self.category_select.add_option(label=label, value=value)
        self.category_select.callback = self._on_select
        self._refresh_category_defaults()
        self.add_item(self.category_select)
        self.add_item(self._back_button())
        self.add_item(self._jump_button())
        self.add_item(self._next_button())

    async def _on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This menu isn't yours.", ephemeral=True)
            return
        cid = self.category_select.values[0]
        await interaction.response.defer()
        await self.on_category_change(cid)
        self.page = 1
        self.current_category_id = cid
        self._refresh_category_defaults()
        embed, self.total_pages = await self.make_embed(self.page)
        if interaction.message:
            await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

    def _refresh_category_defaults(self):
        value = self.current_category_id or ""
        for option in self.category_select.options:
            option.default = option.value == value

    def _back_button(self):
        button = discord.ui.Button(style=discord.ButtonStyle.secondary, label="Back")

        async def cb(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This menu isn't yours.", ephemeral=True)
                return
            if self.total_pages <= 1:
                await interaction.response.defer()
                return
            await interaction.response.defer()
            self.page = max(1, self.page - 1)
            embed, total = await self.make_embed(self.page)
            self.total_pages = total
            if interaction.message:
                await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

        button.callback = cb
        return button

    def _next_button(self):
        button = discord.ui.Button(style=discord.ButtonStyle.secondary, label="Next")

        async def cb(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This menu isn't yours.", ephemeral=True)
                return
            if self.total_pages <= 1:
                await interaction.response.defer()
                return
            await interaction.response.defer()
            self.page = min(self.total_pages, self.page + 1)
            embed, total = await self.make_embed(self.page)
            self.total_pages = total
            if interaction.message:
                await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

        button.callback = cb
        return button

    def _jump_button(self):
        button = discord.ui.Button(style=discord.ButtonStyle.primary, label="Jump To")

        async def cb(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This menu isn't yours.", ephemeral=True)
                return

            class JumpModal(discord.ui.Modal, title="Jump to page"):
                page_input = discord.ui.TextInput(label="Page", placeholder="1", required=True)

                async def on_submit(self_inner, modal_interaction: discord.Interaction):
                    try:
                        val = int(str(self_inner.page_input.value).strip())
                    except Exception:
                        await modal_interaction.response.send_message("Invalid page.", ephemeral=True)
                        return
                    if val < 1 or val > self.total_pages:
                        await modal_interaction.response.send_message("Out of range.", ephemeral=True)
                        return
                    self.page = val
                    embed, total = await self.make_embed(self.page)
                    self.total_pages = total
                    await modal_interaction.response.edit_message(embed=embed, view=self)

            await interaction.response.send_modal(JumpModal())

        button.callback = cb
        return button


class SRCLeaderboardCog(commands.Cog):
    src_group = app_commands.Group(name="src", description="Speedrun.com commands")
    app_commands.allowed_installs(guilds=True, users=True)(src_group)
    app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)(src_group)

    leaderboard_group = app_commands.Group(name="leaderboard", description="Leaderboard commands", parent=src_group)

    def __init__(self, client: commands.Bot):
        self.client = client
        self.session: Optional[aiohttp.ClientSession] = None
        self._watch_task: Optional[asyncio.Task] = None
        self._watchers: List[Dict[str, Any]] = []
        self._game_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._category_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._user_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._game_info_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._cache_ttl = 300.0
        ensure_db()
        self._load_watchers()

    async def cog_load(self):
        self.session = aiohttp.ClientSession()
        if self._watch_task is None:
            self._watch_task = asyncio.create_task(self._watch_loop())

    def cog_unload(self):
        if self._watch_task:
            self._watch_task.cancel()
            self._watch_task = None
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

    def _normalize_cat(self, cat: Optional[str]) -> str:
        return cat or ""

    def _denorm_cat(self, cat: str) -> Optional[str]:
        return cat or None

    def _load_watchers(self):
        con = sqlite3.connect(DB_FILE)
        try:
            cur = con.execute(
                "SELECT guild_id, channel_id, role_id, game_id, category_id, last_checked, last_seen_ids FROM watchers"
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "guild_id": r[0],
                        "channel_id": r[1],
                        "role_id": r[2],
                        "game_id": r[3],
                        "category_id": self._denorm_cat(r[4]),
                        "last_checked": r[5],
                        "last_seen_ids": json.loads(r[6]) if r[6] else [],
                    }
                )
            self._watchers = out
        finally:
            con.close()

    def _save_watchers(self):
        con = sqlite3.connect(DB_FILE)
        try:
            with con:
                for w in self._watchers:
                    guild_id = int(w.get("guild_id"))
                    channel_id = int(w.get("channel_id"))
                    role_id = int(w.get("role_id"))
                    game_id = str(w.get("game_id"))
                    category_id = self._normalize_cat(w.get("category_id"))
                    last_checked = str(w.get("last_checked"))
                    last_seen_ids = json.dumps(w.get("last_seen_ids") or [])
                    cur = con.execute(
                        "UPDATE watchers SET role_id=?, last_checked=?, last_seen_ids=? WHERE guild_id=? AND channel_id=? AND game_id=? AND category_id=?",
                        (
                            role_id,
                            last_checked,
                            last_seen_ids,
                            guild_id,
                            channel_id,
                            game_id,
                            category_id,
                        ),
                    )
                    if cur.rowcount == 0:
                        con.execute(
                            "INSERT INTO watchers (guild_id, channel_id, role_id, game_id, category_id, last_checked, last_seen_ids) VALUES (?,?,?,?,?,?,?)",
                            (
                                guild_id,
                                channel_id,
                                role_id,
                                game_id,
                                category_id,
                                last_checked,
                                last_seen_ids,
                            ),
                        )
        finally:
            con.close()

    async def src_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{API_BASE}/{path.lstrip('/')}"
        if self.session is None:
            self.session = aiohttp.ClientSession()
        async with self.session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def src_search_games(self, term: str) -> List[Dict[str, Any]]:
        params = {"name": term, "max": 25}
        data = await self.src_get("games", params=params)
        return data.get("data", [])

    async def src_get_game_categories(self, game_id: str) -> List[Dict[str, Any]]:
        data = await self.src_get(f"games/{game_id}/categories")
        items = data.get("data", [])
        return [c for c in items if c.get("type") == "per-game"]

    def _trim_cache(self, cache: Dict[str, Tuple[float, Any]], limit: int = 64):
        if len(cache) <= limit:
            return
        oldest_key = min(cache.items(), key=lambda item: item[1][0])[0]
        cache.pop(oldest_key, None)

    async def src_search_games_cached(self, term: str) -> List[Dict[str, Any]]:
        key = term.lower().strip()
        now = time.monotonic()
        cached = self._game_cache.get(key)
        if cached and now - cached[0] < self._cache_ttl:
            return cached[1]
        data = await self.src_search_games(term)
        self._game_cache[key] = (now, data)
        self._trim_cache(self._game_cache)
        return data

    async def src_get_game_categories_cached(self, game_id: str) -> List[Dict[str, Any]]:
        key = str(game_id)
        now = time.monotonic()
        cached = self._category_cache.get(key)
        if cached and now - cached[0] < self._cache_ttl:
            return cached[1]
        data = await self.src_get_game_categories(game_id)
        self._category_cache[key] = (now, data)
        self._trim_cache(self._category_cache)
        return data

    async def src_get_user_cached(self, user_id: str) -> Optional[Dict[str, Any]]:
        key = str(user_id)
        now = time.monotonic()
        cached = self._user_cache.get(key)
        if cached and now - cached[0] < self._cache_ttl:
            return cached[1]
        try:
            data = await self.src_get(f"users/{user_id}")
        except aiohttp.ClientResponseError:
            return None
        user = data.get("data") if isinstance(data, dict) else None
        if user:
            self._user_cache[key] = (now, user)
            self._trim_cache(self._user_cache, limit=128)
        return user

    async def src_get_game_cached(self, game_id: str) -> Optional[Dict[str, Any]]:
        key = str(game_id)
        now = time.monotonic()
        cached = self._game_info_cache.get(key)
        if cached and now - cached[0] < self._cache_ttl:
            return cached[1]
        try:
            data = await self.src_get(f"games/{game_id}")
        except aiohttp.ClientResponseError:
            return None
        game_info = data.get("data") if isinstance(data, dict) else None
        if game_info:
            self._game_info_cache[key] = (now, game_info)
            self._trim_cache(self._game_info_cache, limit=64)
        return game_info

    async def src_get_leaderboard(self, game_id: str, category_id: str) -> Dict[str, Any]:
        params = {"embed": "players,category,game,variables"}
        return await self.src_get(f"leaderboards/{game_id}/category/{category_id}", params=params)

    async def src_get_rejected_runs(self, game_id: str, category_id: Optional[str], *, sort_order: str, max_runs: Optional[int] = None) -> List[Dict[str, Any]]:
        order = sort_order if sort_order in {"newest", "oldest"} else "newest"
        out: List[Dict[str, Any]] = []
        offset = 0
        while True:
            if offset >= 10000:
                break
            p = {
                "game": game_id,
                "status": "rejected",
                "max": 200,
                "offset": offset,
                "embed": "players,category,examiner",
            }
            if category_id:
                p["category"] = category_id
            try:
                page = await self.src_get("runs", params=p)
            except aiohttp.ClientResponseError as exc:
                if exc.status == 400:
                    break
                raise
            runs = page.get("data", [])
            if not runs:
                break
            out.extend(runs)
            if order != "oldest" and max_runs is not None and len(out) >= max_runs:
                out = out[:max_runs]
                break
            offset += len(runs)
            if len(runs) < 200:
                break
        return out

    def _players_from_run(self, run: Dict[str, Any], lookup: Optional[Dict[str, str]] = None) -> List[str]:
        names: List[str] = []
        entries: List[Any] = []
        raw_players = run.get("players")
        if isinstance(raw_players, dict):
            data = raw_players.get("data")
            if isinstance(data, list):
                entries.extend(data)
        elif isinstance(raw_players, list):
            entries.extend(raw_players)
        for entry in entries:
            if not isinstance(entry, dict):
                pid = str(entry)
                name = lookup.get(pid) if lookup else None
            else:
                pid = entry.get("id") or entry.get("name")
                name = entry.get("names", {}).get("international") or entry.get("name")
                if not name and lookup and pid:
                    name = lookup.get(pid)
                if not name and lookup and entry.get("id"):
                    name = lookup.get(entry.get("id"))
            if name:
                names.append(name)
        if not names and lookup and isinstance(raw_players, list):
            for entry in raw_players:
                if isinstance(entry, dict):
                    pid = entry.get("id") or entry.get("name")
                    if pid and lookup.get(pid):
                        names.append(lookup[pid])
        return names or ["Unknown"]

    async def _leaderboard_pages(self, game_id: str, category_id: str, page_size: int) -> Tuple[List[List[Dict[str, Any]]], Dict[str, str]]:
        data = await self.src_get_leaderboard(game_id, category_id)
        payload = data.get("data", {})
        runs = payload.get("runs", [])
        slots = [r.get("run") or r for r in runs]
        pages: List[List[Dict[str, Any]]] = []
        for i in range(0, len(slots), page_size):
            pages.append(slots[i : i + page_size])
        lookup: Dict[str, str] = {}
        player_container = payload.get("players") or {}
        player_data = player_container.get("data") if isinstance(player_container, dict) else []
        if isinstance(player_data, list):
            for p in player_data:
                if not isinstance(p, dict):
                    continue
                display = p.get("names", {}).get("international") or p.get("name") or p.get("id")
                keys: List[str] = []
                pid = p.get("id")
                if pid:
                    keys.append(str(pid))
                pname = p.get("name")
                if pname:
                    keys.append(str(pname))
                for key in keys:
                    if display:
                        lookup[key] = display
        return pages, lookup

    def _parse_timestamp(self, value: Optional[str]) -> datetime:
        if not value:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            processed = value
            if processed.endswith("Z"):
                processed = processed[:-1] + "+00:00"
            dt = datetime.fromisoformat(processed)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

    def _run_timestamp(self, run: Dict[str, Any]) -> datetime:
        submitted = run.get("submitted")
        if submitted:
            return self._parse_timestamp(submitted)
        status = run.get("status", {})
        verify_date = status.get("verify-date") if isinstance(status, dict) else None
        if verify_date:
            return self._parse_timestamp(verify_date)
        date_only = run.get("date")
        if date_only:
            return self._parse_timestamp(date_only)
        return datetime.min.replace(tzinfo=timezone.utc)

    def _format_timestamp_field(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        dt = self._parse_timestamp(raw)
        if dt == datetime.min.replace(tzinfo=timezone.utc):
            return raw
        month_name = dt.strftime("%B")
        display = f"{month_name} {dt.day}, {dt.year}"
        unix = int(dt.timestamp())
        return f"{display} • <t:{unix}:t>"

    async def _rejected_pages(self, game_id: str, category_id: Optional[str], page_size: int, max_pages: Optional[int], sort_order: str) -> Tuple[List[List[Dict[str, Any]]], int]:
        max_runs = None if sort_order == "oldest" else (page_size * max_pages if max_pages is not None else None)
        runs = await self.src_get_rejected_runs(game_id, category_id, sort_order=sort_order, max_runs=max_runs)
        reverse = sort_order != "oldest"
        runs.sort(key=self._run_timestamp, reverse=reverse)
        if max_pages is not None:
            runs = runs[: page_size * max_pages]
        pages: List[List[Dict[str, Any]]] = []
        for i in range(0, len(runs), page_size):
            pages.append(runs[i : i + page_size])
        return pages, len(pages)

    @leaderboard_group.command(name="view")
    @app_commands.describe(game="Game", category="Category", pages="How many pages to include (all if omitted)")
    @app_commands.autocomplete(game=game_autocomplete, category=category_autocomplete)
    async def leaderboard_view(self, interaction: discord.Interaction, game: str, category: Optional[str] = None, pages: Optional[int] = None):
        await interaction.response.defer()
        cats = await self.src_get_game_categories_cached(game)
        if not cats:
            await interaction.followup.send("No categories found.")
            return
        cat_map: Dict[str, Dict[str, Any]] = {}
        ordered_ids: List[str] = []
        for c in cats:
            cat_id = c.get("id")
            if not cat_id:
                continue
            cat_id = str(cat_id)
            cat_map[cat_id] = c
            ordered_ids.append(cat_id)
        if not cat_map:
            await interaction.followup.send("No categories found.")
            return
        current_category = category if category in cat_map else ordered_ids[0]
        items_per_page = 10
        page_limit = None if not pages or pages <= 0 else pages

        cache: Dict[str, Tuple[List[List[Dict[str, Any]]], Dict[str, str]]] = {}

        async def fetch_pages(cat_id: str) -> Tuple[List[List[Dict[str, Any]]], Dict[str, str]]:
            if cat_id not in cache:
                cache[cat_id] = await self._leaderboard_pages(game, cat_id, items_per_page)
            return cache[cat_id]

        async def on_category_change(new_cid: str):
            nonlocal current_category
            if new_cid in cat_map:
                current_category = new_cid

        async def make_embed(page_index: int):
            pages_data, lookup = await fetch_pages(current_category)
            display_pages = pages_data[:page_limit] if page_limit is not None else pages_data
            total_pages = len(display_pages) or 1
            page_index = max(1, min(page_index, total_pages))
            entries = display_pages[page_index - 1] if display_pages else []
            title = f"Leaderboard - {cat_map[current_category].get('name')}"
            embed = discord.Embed(title=title, colour=discord.Colour.blurple())
            lines = []
            base_index = (page_index - 1) * items_per_page
            for idx, run in enumerate(entries, start=1):
                pos = base_index + idx
                t = run.get("times", {}).get("primary_t")
                time_s = format_duration(t)
                date = run.get("date") or run.get("submitted") or "?"
                vs = run.get("videos", {})
                link = None
                if isinstance(vs, dict):
                    links = vs.get("links") or []
                    if links:
                        link = links[0].get("uri")
                players = ", ".join(self._players_from_run(run, lookup))
                parts = [f"#{pos}", time_s, players, date]
                if link:
                    parts.insert(3, f"[Video]({link})")
                lines.append(" | ".join(parts))
            if not lines:
                lines = ["No runs found."]
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"Page {page_index}/{total_pages}")
            return embed, total_pages

        categories_payload = [{"id": cat_id, "name": cat_map[cat_id].get("name")} for cat_id in ordered_ids]
        categories_payload.sort(key=lambda item: (item.get("id") != current_category))
        embed, total_pages = await make_embed(1)
        view = LeaderboardView(
            interaction.user.id,
            make_embed,
            on_category_change,
            categories_payload,
            total_pages,
            current_category,
        )
        await interaction.followup.send(embed=embed, view=view)

    @leaderboard_group.command(name="rejected-runs")
    @app_commands.describe(game="Game", category="Category", pages="How many pages to include (all if omitted)", sort="Sort order")
    @app_commands.autocomplete(game=game_autocomplete, category=category_autocomplete)
    async def leaderboard_rejected(self, interaction: discord.Interaction, game: str, category: Optional[str] = None, pages: Optional[int] = None, sort: Literal["newest", "oldest"] = "newest"):
        await interaction.response.defer()
        items_per_page = 1
        page_limit = None if not pages or pages <= 0 else pages
        game_info = await self.src_get_game_cached(game)
        game_name = (
            (game_info or {}).get("names", {}).get("international")
            or (game_info or {}).get("name")
            or game
        )
        cats = await self.src_get_game_categories_cached(game)
        cat_map: Dict[str, Dict[str, Any]] = {}
        ordered_ids: List[str] = []
        for c in cats:
            cat_id = c.get("id")
            if not cat_id:
                continue
            cat_id = str(cat_id)
            cat_map[cat_id] = c
            ordered_ids.append(cat_id)
        current_category = category if category in cat_map else None

        sort_order = sort or "newest"
        cache: Dict[str, Tuple[List[List[Dict[str, Any]]], int]] = {}

        async def fetch_pages(cat_id: Optional[str]) -> Tuple[List[List[Dict[str, Any]]], int]:
            key = f"{cat_id or ALL_CATEGORIES_VALUE}:{sort_order}:{page_limit or 'all'}"
            if key not in cache:
                cache[key] = await self._rejected_pages(game, cat_id, items_per_page, page_limit, sort_order)
            return cache[key]

        async def on_category_change(new_cid: str):
            nonlocal current_category
            if new_cid == ALL_CATEGORIES_VALUE:
                current_category = None
            elif new_cid in cat_map:
                current_category = new_cid

        async def make_embed(page_index: int):
            pages_data, cached_total = await fetch_pages(current_category)
            total_pages = cached_total or len(pages_data) or 1
            page_index = max(1, min(page_index, total_pages))
            entries = pages_data[page_index - 1] if pages_data else []
            title_cat = cat_map[current_category].get("name") if current_category in cat_map else "All Categories"
            embed_title = f"Rejected Runs for {game_name}"
            if title_cat:
                embed_title = f"{embed_title} - {title_cat}"
            embed = discord.Embed(title=embed_title, colour=discord.Colour.red())
            if not entries:
                embed.description = "No rejected runs found."
            else:
                run = entries[0]
                t = format_duration(run.get("times", {}).get("primary_t"))
                pl = ", ".join(self._players_from_run(run))
                st = run.get("status", {})
                reason = st.get("reason") or "No reason provided"
                examiner_name = "?"
                examiner_embed = run.get("examiner")
                if isinstance(examiner_embed, dict):
                    data = examiner_embed.get("data")
                    if isinstance(data, dict):
                        examiner_name = (
                            data.get("names", {}).get("international")
                            or data.get("name")
                            or data.get("id")
                            or examiner_name
                        )
                    elif isinstance(data, list) and data:
                        first = data[0]
                        if isinstance(first, dict):
                            examiner_name = (
                                first.get("names", {}).get("international")
                                or first.get("name")
                                or first.get("id")
                                or examiner_name
                            )
                examiner_id = None
                if isinstance(st, dict):
                    examiner_id = st.get("examiner")
                if examiner_name == "?" and examiner_id:
                    cached_user = await self.src_get_user_cached(examiner_id)
                    if cached_user:
                        examiner_name = (
                            cached_user.get("names", {}).get("international")
                            or cached_user.get("name")
                            or str(examiner_id)
                        )
                if examiner_name == "?" and examiner_id:
                    examiner_name = str(examiner_id)
                vs = run.get("videos", {})
                v = None
                if isinstance(vs, dict):
                    links = vs.get("links") or []
                    if links:
                        v = links[0].get("uri")
                submitted_raw = run.get("submitted") or run.get("date")
                submitted_value = self._format_timestamp_field(submitted_raw) or "?"
                rejected_raw = st.get("verify-date") if isinstance(st, dict) else None
                rejected_value = self._format_timestamp_field(rejected_raw)
                embed.add_field(name="Rejection Reason", value=reason, inline=False)
                embed.add_field(name="Time", value=t, inline=True)
                embed.add_field(name="Players", value=pl, inline=True)
                embed.add_field(name="Examiner", value=examiner_name, inline=True)
                embed.add_field(name="Submitted", value=submitted_value, inline=True)
                if rejected_value:
                    embed.add_field(name="Rejected", value=rejected_value, inline=True)
                if v:
                    embed.add_field(name="Videos", value=v, inline=False)
                category_name = None
                cat_payload = run.get("category")
                if isinstance(cat_payload, dict):
                    category_name = (
                        cat_payload.get("data", {}).get("name")
                        or cat_payload.get("name")
                    )
                if not category_name:
                    cat_id = run.get("category")
                    if isinstance(cat_id, dict):
                        category_name = cat_id.get("name")
                    elif isinstance(cat_id, str):
                        category_name = cat_map.get(cat_id, {}).get("name")
                if category_name:
                    embed.add_field(name="Category", value=category_name, inline=True)
            embed.set_footer(text=f"Page {page_index}/{total_pages}")
            return embed, total_pages

        actual_categories = [{"id": cat_id, "name": cat_map[cat_id].get("name")} for cat_id in ordered_ids]
        actual_categories.sort(key=lambda item: (item.get("id") != current_category))
        categories_payload = [{"id": ALL_CATEGORIES_VALUE, "name": "All Categories"}] + actual_categories
        embed, total_pages = await make_embed(1)
        view = LeaderboardView(
            interaction.user.id,
            make_embed,
            on_category_change,
            categories_payload,
            total_pages,
            current_category if current_category is not None else ALL_CATEGORIES_VALUE,
        )
        await interaction.followup.send(embed=embed, view=view)

    @leaderboard_group.command(name="utils")
    @app_commands.describe(game="Game", category="Category", role="Role to ping for new runs", channel="Channel to post updates in")
    @app_commands.autocomplete(game=game_autocomplete, category=category_autocomplete)
    async def leaderboard_utils(self, interaction: discord.Interaction, game: str, role: discord.Role, category: Optional[str] = None, channel: Optional[discord.TextChannel] = None):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            await interaction.followup.send("Server-only command.", ephemeral=True)
            return
        target_channel = channel or interaction.channel
        if not isinstance(target_channel, discord.abc.GuildChannel) or target_channel.guild != interaction.guild:
            await interaction.followup.send("Select a channel from this server.", ephemeral=True)
            return
        cats = await self.src_get_game_categories_cached(game)
        if category is not None and category not in {c.get("id") for c in cats}:
            category = None
        watcher = {
            "guild_id": interaction.guild.id,
            "channel_id": target_channel.id,
            "role_id": role.id,
            "game_id": game,
            "category_id": category,
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "last_seen_ids": [],
        }
        replaced = False
        for i, w in enumerate(self._watchers):
            if w.get("guild_id") == watcher["guild_id"] and w.get("channel_id") == watcher["channel_id"] and w.get("game_id") == watcher["game_id"] and w.get("category_id") == watcher["category_id"]:
                self._watchers[i] = watcher
                replaced = True
                break
        if not replaced:
            self._watchers.append(watcher)
        self._save_watchers()
        await interaction.followup.send("Notifications configured.", ephemeral=True)

    @leaderboard_group.command(name="utils-edit")
    @app_commands.describe(game="Game", category="Category", role="Role to ping", channel="Channel currently receiving updates")
    @app_commands.autocomplete(game=game_autocomplete, category=category_autocomplete)
    async def leaderboard_utils_edit(self, interaction: discord.Interaction, game: str, role: discord.Role, category: Optional[str] = None, channel: Optional[discord.TextChannel] = None):
        await interaction.response.defer(ephemeral=True)
        if interaction.guild is None:
            await interaction.followup.send("Server-only command.", ephemeral=True)
            return
        target_channel = channel or interaction.channel
        if not isinstance(target_channel, discord.abc.GuildChannel) or target_channel.guild != interaction.guild:
            await interaction.followup.send("Select a channel from this server.", ephemeral=True)
            return
        found = False
        for w in self._watchers:
            if w.get("guild_id") == interaction.guild.id and w.get("channel_id") == target_channel.id and w.get("game_id") == game and w.get("category_id") == category:
                w["role_id"] = role.id
                found = True
                break
        if not found:
            await interaction.followup.send("No existing watcher to edit for this channel/game/category.", ephemeral=True)
            return
        self._save_watchers()
        await interaction.followup.send("Updated.", ephemeral=True)

    async def _watch_loop(self):
        await self.client.wait_until_ready()
        while not self.client.is_closed():
            try:
                await self._tick_watchers()
            except asyncio.CancelledError:
                break
            except Exception:
                pass
            await asyncio.sleep(30)

    async def _tick_watchers(self):
        if not self._watchers:
            return
        for w in list(self._watchers):
            guild = self.client.get_guild(int(w.get("guild_id")))
            if not guild:
                continue
            channel = guild.get_channel(int(w.get("channel_id")))
            if channel is None:
                continue
            role_id = w.get("role_id")
            game_id = w.get("game_id")
            category_id = w.get("category_id")
            new_runs = await self._fetch_new_runs(game_id, category_id, w.get("last_seen_ids") or [])
            if not new_runs:
                continue
            w["last_seen_ids"] = [r.get("id") for r in new_runs[:50]]
            self._save_watchers()
            role_mention = f"<@&{role_id}>" if role_id else ""
            for run in new_runs:
                await self._announce_new_run(channel, role_mention, run)

    async def _fetch_new_runs(self, game_id: str, category_id: Optional[str], seen_ids: List[str]) -> List[Dict[str, Any]]:
        p = {"game": game_id, "status": "new", "max": 100, "embed": "players,category"}
        if category_id:
            p["category"] = category_id
        data = await self.src_get("runs", params=p)
        runs = data.get("data", [])
        fresh = []
        for r in runs:
            rid = r.get("id")
            if not rid or rid in seen_ids:
                continue
            fresh.append(r)
        return list(reversed(fresh))

    async def _announce_new_run(self, channel: discord.abc.Messageable, role_mention: str, run: Dict[str, Any]):
        t = format_duration(run.get("times", {}).get("primary_t"))
        pl = ", ".join(self._players_from_run(run))
        cat = None
        cat_data = run.get("category")
        if isinstance(cat_data, dict):
            cat = cat_data.get("data", {}).get("name")
        if not cat:
            cat = "Unknown Category"
        vs = run.get("videos", {})
        v = None
        if isinstance(vs, dict):
            links = vs.get("links") or []
            if links:
                v = links[0].get("uri")
        embed = discord.Embed(title=f"New Run Submitted - {cat}", colour=discord.Colour.green())
        embed.add_field(name="Players", value=pl, inline=False)
        embed.add_field(name="Time", value=t, inline=True)
        if v:
            embed.add_field(name="Videos", value=v, inline=False)
        embed.timestamp = datetime.now(timezone.utc)
        content = role_mention if role_mention else None
        await channel.send(content=content, embed=embed)


async def setup(client: commands.Bot) -> None:
    await client.add_cog(SRCLeaderboardCog(client))
