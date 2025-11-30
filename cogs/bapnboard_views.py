from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from .bapnboard import LeaderboardCog


class JumpToPageModal(discord.ui.Modal):
    def __init__(self, parent_view: "PagedListView"):
        super().__init__(title="Jump to page")
        self.parent_view = parent_view
        total = parent_view.total_pages
        placeholder = "1" if total <= 1 else f"1-{total}"
        self.page_input = discord.ui.TextInput(label="Page number", placeholder=placeholder, required=True)
        self.add_item(self.page_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            requested = int(self.page_input.value.strip())
        except Exception:
            await interaction.response.send_message("Invalid page number.", ephemeral=True)
            return
        await self.parent_view.handle_jump(interaction, requested - 1)


class PagedListView(discord.ui.View):
    def __init__(
        self,
        title: str,
        pages: List[List[str]],
        color: discord.Color = discord.Color.blue(),
        footer_note: Optional[str] = None,
        thumbnail: Optional[str] = None,
        header: Optional[str] = None,
    ):
        super().__init__(timeout=None)
        self.title = title
        self.pages = pages if pages else [["No entries."]]
        self.color = color
        self.footer_note = footer_note
        self.thumbnail = thumbnail
        self.header = header
        self.current = 0
        self.total_pages = max(1, len(self.pages))
        self._sync_buttons()

    def create_embed(self) -> discord.Embed:
        embed = discord.Embed(title=self.title, color=self.color)
        body = "\n".join(self.pages[self.current])
        if self.header:
            embed.description = f"{self.header}\n\n{body}" if body else self.header
        else:
            embed.description = body
        footer = f"Page {self.current + 1}/{self.total_pages}"
        if self.footer_note:
            footer = f"{footer} | {self.footer_note}"
        embed.set_footer(text=footer)
        if self.thumbnail:
            embed.set_thumbnail(url=self.thumbnail)
        return embed

    def _sync_buttons(self) -> None:
        back_button = None
        jump_button = None
        next_button = None
        for child in self.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if child.custom_id == "bapn_lb_back":
                back_button = child
            elif child.custom_id == "bapn_lb_jump":
                jump_button = child
            elif child.custom_id == "bapn_lb_next":
                next_button = child
        if back_button:
            back_button.disabled = self.current == 0
        if next_button:
            next_button.disabled = self.current >= self.total_pages - 1
        if jump_button:
            jump_button.disabled = self.total_pages <= 1

    async def show_page(self, interaction: discord.Interaction, index: int) -> None:
        index = max(0, min(index, self.total_pages - 1))
        self.current = index
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    async def handle_jump(self, interaction: discord.Interaction, index: int) -> None:
        index = max(0, min(index, self.total_pages - 1))
        self.current = index
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, custom_id="bapn_lb_back")
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.show_page(interaction, self.current - 1)

    @discord.ui.button(label="Jump to...", style=discord.ButtonStyle.primary, custom_id="bapn_lb_jump")
    async def jump(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        modal = JumpToPageModal(self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, custom_id="bapn_lb_next")
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.show_page(interaction, self.current + 1)


class ProfileBoardSelect(discord.ui.Select):
    def __init__(self, parent: "ProfileView", boards: List[str]):
        options = [
            discord.SelectOption(label=board, value=board, default=(board == parent.current_board))
            for board in boards
        ]
        super().__init__(placeholder="Select leaderboard", options=options, min_values=1, max_values=1)
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction) -> None:
        self.parent_view.current_board = self.values[0]
        self.parent_view.page_index = 0
        self.parent_view.update_select_defaults()
        await self.parent_view.refresh(interaction)


class ProfileView(discord.ui.View):
    def __init__(self, cog: "LeaderboardCog", guild_id: int, member: discord.abc.User, boards: List[str], initial_board: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.member = member
        self.boards = boards
        self.current_board = initial_board
        self.page_index = 0
        self.pages: List[List[str]] = []
        self.select = ProfileBoardSelect(self, boards)
        self.select.row = 0
        self.add_item(self.select)
        self.back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, custom_id="profile_back", row=1)
        self.next_button = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, custom_id="profile_next", row=1)
        self.back_button.callback = self._on_back
        self.next_button.callback = self._on_next
        self.add_item(self.back_button)
        self.add_item(self.next_button)

    def update_select_defaults(self) -> None:
        for option in self.select.options:
            option.default = option.value == self.current_board

    def _sync_buttons(self) -> None:
        total_pages = len(self.pages)
        if self.back_button:
            self.back_button.disabled = self.page_index <= 0 or total_pages <= 1
        if self.next_button:
            self.next_button.disabled = self.page_index >= total_pages - 1 or total_pages <= 1

    async def refresh(self, interaction: discord.Interaction) -> None:
        embed, pages = await self.cog.build_profile_content(self.guild_id, self.current_board, self.member)
        self.pages = pages
        if not self.pages:
            self.page_index = 0
        else:
            self.page_index = max(0, min(self.page_index, len(self.pages) - 1))
        history = "\n".join(self.pages[self.page_index]) if self.pages else "No matches recorded."
        embed.description = history
        embed.set_footer(text=f"Page {self.page_index + 1}/{max(len(self.pages), 1)}")
        self.update_select_defaults()
        self._sync_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def _on_back(self, interaction: discord.Interaction) -> None:
        if self.page_index > 0:
            self.page_index -= 1
        await self.refresh(interaction)

    async def _on_next(self, interaction: discord.Interaction) -> None:
        if self.page_index < len(self.pages) - 1:
            self.page_index += 1
        await self.refresh(interaction)


class ChallengeControlView(discord.ui.View):
    def __init__(self, cog: "LeaderboardCog", guild_id: int, category: str, match_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.category = category
        self.match_id = match_id
        self.accept_button = discord.ui.Button(
            style=discord.ButtonStyle.green,
            label="Accept",
            custom_id=f"bapn_accept:{match_id}",
        )
        self.accept_button.callback = self._on_accept
        self.cancel_button = discord.ui.Button(
            style=discord.ButtonStyle.danger,
            label="Cancel Match",
            custom_id=f"bapn_cancel:{match_id}",
        )
        self.cancel_button.callback = self._on_cancel
        self.add_item(self.accept_button)
        self.add_item(self.cancel_button)
        self.decline_button = discord.ui.Button(
            style=discord.ButtonStyle.red,
            label="Decline",
            custom_id=f"bapn_decline:{match_id}",
        )
        self.decline_button.callback = self._on_decline
        self.add_item(self.decline_button)
        self.refresh_buttons()

    def refresh_buttons(self) -> None:
        match = self.cog.get_match(self.guild_id, self.category, self.match_id)
        if not match:
            self.accept_button.disabled = True
            self.cancel_button.disabled = True
            return
        status = match.get("status", "open")
        opponent_id = match.get("opponent_id")
        if status in {"completed", "cancelled"}:
            self.accept_button.disabled = True
            self.cancel_button.disabled = True
            return
        if opponent_id:
            self.accept_button.disabled = status != "pending"
        else:
            self.accept_button.disabled = status != "open"
        self.cancel_button.disabled = False
        cancel_votes = set(match.get("cancel_votes", []))
        if len(cancel_votes) >= 2 or status == "pending_cancel":
            self.cancel_button.label = "Cancel Pending"
            self.cancel_button.disabled = False
        else:
            self.cancel_button.label = "Cancel Match"
        if opponent_id and status == "pending":
            self.decline_button.disabled = False
        else:
            self.decline_button.disabled = True

    async def _on_accept(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_match_accept(interaction, self.guild_id, self.category, self.match_id)

    async def _on_cancel(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_match_cancel(interaction, self.guild_id, self.category, self.match_id)

    async def _on_decline(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_match_decline(interaction, self.guild_id, self.category, self.match_id)
