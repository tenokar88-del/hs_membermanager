"""봇 실행 파일: 디스코드 접속, DB 연결, 기능 등록."""

import os

import asyncpg
import discord
from discord.ext import commands

from voice_tracker import VoiceTracker


class VoiceBot(commands.Bot):
    def __init__(self, database_url: str, guild_id: int):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.voice_states = True
        intents.members = True  # 개발자 포털에서 Server Members Intent 켜기
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self.database_url = database_url
        self.guild_id = guild_id
        self.pool: asyncpg.Pool | None = None

    async def setup_hook(self):
        self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=3)
        await self.add_cog(VoiceTracker(self, self.pool, self.guild_id))
        guild = discord.Object(id=self.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)  # 지정한 서버에만 명령어 등록

    async def close(self):
        await super().close()
        if self.pool:
            await self.pool.close()


def main():
    bot = VoiceBot(os.environ["DATABASE_URL"], int(os.environ["GUILD_ID"]))
    bot.run(os.environ["DISCORD_TOKEN"], root_logger=True)


if __name__ == "__main__":
    main()
