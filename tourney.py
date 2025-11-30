import pathlib
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv
import os
import random
import datetime
import time
import platform
import csv

load_dotenv()

TOKEN = os.getenv('TOKEN')

class Client(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=commands.when_mentioned_or('.'), intents=discord.Intents().all())
        self.token_expiry = datetime.datetime.utcnow()
        self.cogslist = [
            "cogs.bapnboard",
            "cogs.translate",
            "cogs.src_leaderboard"
        ]

    async def setup_hook(self):
        for ext in self.cogslist:
            await self.load_extension(ext)

    async def on_ready(self):
        print(f"Logged in as {self.user.name}")
        print(f"Bot ID: {self.user.id}")
        print(f"Discord Version: {discord.__version__}")
        print(f"Python Version: {platform.python_version()}")
        self.status_task.start()
        await self.tree.sync() 

    @tasks.loop(seconds=60)
    async def status_task(self):
        phrases = ["keep being strange.... but dont be a stranger"]
        new_status = random.choice(phrases)
        await self.change_presence(activity=discord.Game(name=new_status))

client = Client()

client.run(TOKEN)