import io
import re
import discord
from discord import app_commands
from discord.ext import commands

class TranslateCog(commands.Cog):
    translate_group = app_commands.Group(name="translate", description="Translate between machine cipher and plain text.")
    app_commands.allowed_installs(guilds=True, users=True)(translate_group)
    app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)(translate_group)

    def __init__(self, client: commands.Bot):
        self.client = client
        self.translate_to_english_context = app_commands.ContextMenu(
            name="Translate Message to English",
            callback=self.translate_message_to_english_context,
        )
        self.translate_to_english_context.allowed_installs = app_commands.AppInstallationType(guild=True, user=True)
        self.translate_to_english_context.allowed_contexts = app_commands.AppCommandContext(
            guild=True,
            dm_channel=True,
            private_channel=True,
        )

        self.translate_to_cipher_context = app_commands.ContextMenu(
            name="Translate Message to Cipher",
            callback=self.translate_message_to_cipher_context,
        )
        self.translate_to_cipher_context.allowed_installs = app_commands.AppInstallationType(guild=True, user=True)
        self.translate_to_cipher_context.allowed_contexts = app_commands.AppCommandContext(
            guild=True,
            dm_channel=True,
            private_channel=True,
        )

    cipher_map = {chr(i): f":MachineCipher{chr(i).upper()}:" for i in range(97, 123)}
    reverse_cipher_map = {v: k for k, v in cipher_map.items()}

    async def cog_load(self):
        self.client.tree.add_command(self.translate_to_english_context, override=True)
        self.client.tree.add_command(self.translate_to_cipher_context, override=True)

    async def cog_unload(self):
        self.client.tree.remove_command(
            self.translate_to_english_context.name,
            type=self.translate_to_english_context.type,
        )
        self.client.tree.remove_command(
            self.translate_to_cipher_context.name,
            type=self.translate_to_cipher_context.type,
        )

    def to_plain_text(self, cipher_message):
        emote_pattern = r'<a?:MachineCipher([A-Z]):\d+>'
        translated_message = re.sub(emote_pattern, lambda m: self.reverse_cipher_map[f":MachineCipher{m.group(1)}:"], cipher_message)
        return translated_message

    def to_machine_cipher(self, plain_text):
        translated_chars = []
        for char in plain_text:
            lower_char = char.lower()
            if lower_char in self.cipher_map:
                translated_chars.append(self.cipher_map[lower_char])
            else:
                translated_chars.append(char)
        return ''.join(translated_chars)
    
    @translate_group.command(name="to-english", description="Translates machine cipher text to plain text.")
    async def translate_to_english(self, interaction: discord.Interaction, cipher_text: str, ephemeral: bool = False):
        if not cipher_text:
            await interaction.response.send_message("Please provide a machine cipher text to translate!", ephemeral=ephemeral)
            return

        translated_message = self.to_plain_text(cipher_text)
        if len(translated_message) > 2000:
            await interaction.response.send_message(
                file=discord.File(
                    fp=io.BytesIO(translated_message.encode("utf-8")),
                    filename="translation.txt",
                ),
                ephemeral=ephemeral,
            )
            return

        await interaction.response.send_message(translated_message, ephemeral=ephemeral)

    @translate_group.command(name="to-cipher", description="Translates plain English text to machine cipher.")
    async def to_machine_cipher_command(self, interaction: discord.Interaction, plain_text: str, ephemeral: bool = False):
        if not plain_text:
            await interaction.response.send_message("Please provide a plain text to translate!", ephemeral=ephemeral)
            return

        translated_message = self.to_machine_cipher(plain_text)
        if len(translated_message) > 2000:
            await interaction.response.send_message(
                file=discord.File(
                    fp=io.BytesIO(translated_message.encode("utf-8")),
                    filename="translation.txt",
                ),
                ephemeral=ephemeral,
            )
            return

        await interaction.response.send_message(translated_message, ephemeral=ephemeral)

    async def translate_message_to_english_context(self, interaction: discord.Interaction, message: discord.Message):
        if not message.content:
            await interaction.response.send_message("That message doesn't have any text to translate.", ephemeral=True)
            return

        if not re.search(r"<a?:MachineCipher([A-Z]):\d+>", message.content):
            await interaction.response.send_message("I couldn't find any machine cipher text in that message.", ephemeral=True)
            return

        translated_message = self.to_plain_text(message.content)
        if len(translated_message) > 2000:
            await interaction.response.send_message(
                file=discord.File(
                    fp=io.BytesIO(translated_message.encode("utf-8")),
                    filename="translation.txt",
                ),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(translated_message, ephemeral=True)

    async def translate_message_to_cipher_context(self, interaction: discord.Interaction, message: discord.Message):
        if not message.content:
            await interaction.response.send_message("That message doesn't have any text to translate.", ephemeral=True)
            return

        if not any(char.lower() in self.cipher_map for char in message.content):
            await interaction.response.send_message("I couldn't find any characters to convert to machine cipher.", ephemeral=True)
            return

        translated_message = self.to_machine_cipher(message.content)
        if not translated_message:
            await interaction.response.send_message("I couldn't convert that message to machine cipher.", ephemeral=True)
            return

        if len(translated_message) > 2000:
            await interaction.response.send_message(
                file=discord.File(
                    fp=io.BytesIO(translated_message.encode("utf-8")),
                    filename="translation.txt",
                ),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(translated_message, ephemeral=True)

async def setup(client: commands.Bot) -> None:
    await client.add_cog(TranslateCog(client))
