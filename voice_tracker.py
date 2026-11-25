"""음성방 게임 횟수 집계 기능 (설정, DB, 정산, 명령어).

게임방 카테고리에서 다른 사람과 함께 있던 시간을 쌓고,
MIN_MEMBERS명 이상 함께한 시간이 방(카테고리) 하나에서 MIN_MINUTES 이상이면 게임 1회로 인정한다.
"""

import asyncio
import csv
import io
import logging
import re
from collections import Counter
from datetime import datetime, timedelta

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands, tasks

# ───────────── 설정 ─────────────
MIN_MINUTES = 60                  # 함께한 시간이 이 분 이상이면 게임 1회 인정
MIN_MEMBERS = 3                   # 방에 이 인원 이상 있어야 시간을 셈 (본인 포함, 봇 제외)
TICK_SECONDS = 120                 # 중간 정산 주기(초)
RANKING_SIZE = 20                 # /랭킹 에 보여줄 인원
ROOM_ROLE_PATTERN = re.compile(r"Room-\d+-Member")  # 게임방을 알아보는 역할 이름
EXCLUDED_CATEGORY_IDS: set[int] = {1508118244727197887, 1509798130760155259, 1508468808849821798}             # 무조건 제외할 카테고리 ID
# ────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS voice_session (
    user_id          BIGINT  NOT NULL,
    category_id      BIGINT  NOT NULL,
    together_since   TIMESTAMPTZ,
    together_seconds INTEGER NOT NULL DEFAULT 0,
    counted          BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, category_id)
);
CREATE TABLE IF NOT EXISTS voice_stats (
    user_id       BIGINT  PRIMARY KEY,
    games         INTEGER NOT NULL DEFAULT 0,
    total_seconds BIGINT  NOT NULL DEFAULT 0
);
"""

ADD_STATS = """
INSERT INTO voice_stats (user_id, games, total_seconds) VALUES ($1, $2, $3)
ON CONFLICT (user_id) DO UPDATE
SET games = voice_stats.games + EXCLUDED.games,
    total_seconds = voice_stats.total_seconds + EXCLUDED.total_seconds
"""

log = logging.getLogger("voicebot")


# ───────────── 디스코드 상태 읽기 ─────────────
def has_room_role(channel) -> bool:
    return any(
        isinstance(target, discord.Role) and (ROOM_ROLE_PATTERN.fullmatch(target.name) or target.name == 'Room-Spare')
        for target in channel.overwrites
    )


def read_voice_state(guild) -> tuple[dict[int, int], set[int]]:
    """실제 접속 상태를 읽는다.

    present  : {유저 ID: 카테고리 ID}  게임방에 접속 중인 사람
    occupied : 사람이 한 명이라도 있는 카테고리 ID (게임방 여부와 무관)
    """
    voice_channels = guild.voice_channels

    # 카테고리나 그 안의 음성 채널 하나라도 Room 역할이 걸려 있으면 게임방
    room_categories = {
        ch.category.id
        for ch in voice_channels
        if ch.category and (has_room_role(ch) or has_room_role(ch.category))
    } - EXCLUDED_CATEGORY_IDS

    present: dict[int, int] = {}
    occupied: set[int] = set()
    for ch in voice_channels:
        if ch.category is None:
            continue
        humans = [m for m in ch.members if not m.bot]
        if not humans:
            continue
        occupied.add(ch.category.id)
        if ch.category.id in room_categories and ch != guild.afk_channel:
            for member in humans:
                present[member.id] = ch.category.id
    return present, occupied


# ───────────── 정산 ─────────────
async def apply_snapshot(
    conn: asyncpg.Connection,
    present: dict[int, int],
    occupied: set[int],
    now: datetime,
) -> None:
    """직전 상태의 시간을 정산하고, 현재 접속 상태를 DB에 반영한다.

    입장·퇴장·이동·주기 정산이 모두 이 함수 하나를 거친다.
    반드시 트랜잭션 안에서 호출할 것.
    """
    headcount = Counter(present.values())

    def is_together(user_id: int, category_id: int) -> bool:
        return present.get(user_id) == category_id and headcount[category_id] >= MIN_MEMBERS

    rows = await conn.fetch("SELECT * FROM voice_session FOR UPDATE")
    known = set()

    for row in rows:
        user_id, category_id = row["user_id"], row["category_id"]
        known.add((user_id, category_id))

        since = row["together_since"]
        seconds = row["together_seconds"]
        counted = row["counted"]

        gained = 0
        if since is not None:
            gained = max(int((now - since).total_seconds()), 0)
            since += timedelta(seconds=gained)  # 1초 미만 자투리는 다음 정산으로
        seconds += gained

        if not is_together(user_id, category_id):
            since = None
        elif since is None:
            since = now

        newly_counted = not counted and seconds > 0 and seconds >= MIN_MINUTES * 60
        if gained or newly_counted:
            await conn.execute(ADD_STATS, user_id, int(newly_counted), gained)
        if newly_counted:
            counted = True
            log.info("게임 1회 인정: user=%s category=%s", user_id, category_id)

        if category_id not in occupied:  # 방이 비었으면 정산 후 삭제
            await conn.execute(
                "DELETE FROM voice_session WHERE user_id = $1 AND category_id = $2",
                user_id, category_id,
            )
        elif (since, seconds, counted) != (
            row["together_since"], row["together_seconds"], row["counted"]
        ):
            await conn.execute(
                "UPDATE voice_session SET together_since = $3, together_seconds = $4, "
                "counted = $5 WHERE user_id = $1 AND category_id = $2",
                user_id, category_id, since, seconds, counted,
            )

    for user_id, category_id in present.items():
        if (user_id, category_id) not in known:
            await conn.execute(
                "INSERT INTO voice_session (user_id, category_id, together_since) "
                "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                user_id, category_id,
                now if is_together(user_id, category_id) else None,
            )


# ───────────── 봇 ─────────────
def format_duration(seconds: int) -> str:
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}시간 {minutes}분"
    return f"{minutes}분 {secs}초"


def csv_safe(text: str) -> str:
    """엑셀이 닉네임을 수식으로 실행하지 않게 막는다."""
    return "'" + text if text.startswith(("=", "+", "-", "@")) else text


def admin_only(func):
    func = app_commands.checks.has_permissions(manage_guild=True)(func)
    func = app_commands.default_permissions(manage_guild=True)(func)
    return app_commands.guild_only()(func)


class VoiceTracker(commands.Cog):
    def __init__(self, bot: commands.Bot, pool: asyncpg.Pool, guild_id: int):
        self.bot = bot
        self.pool = pool
        self.guild_id = guild_id
        self.lock = asyncio.Lock()  # 정산이 겹치지 않게
        self.online = False         # 연결이 끊긴 동안에는 정산하지 않음
        self.warned: set[int] = set()

    async def cog_load(self):
        await self.pool.execute(SCHEMA)

    async def cog_unload(self):
        self.tick.cancel()

    async def sync_voice(self):
        async with self.lock:
            guild = self.bot.get_guild(self.guild_id)
            if not self.online or guild is None or guild.unavailable:
                return
            self.warn_hidden(guild)
            present, occupied = read_voice_state(guild)
            async with self.pool.acquire() as conn, conn.transaction():
                await apply_snapshot(conn, present, occupied, discord.utils.utcnow())

    async def safe_sync(self):
        try:
            await self.sync_voice()
        except Exception:
            log.exception("정산 실패 (다음 주기에 다시 시도)")

    def warn_hidden(self, guild):
        for ch in guild.voice_channels:
            if ch.id not in self.warned and not ch.permissions_for(guild.me).view_channel:
                self.warned.add(ch.id)
                log.warning(
                    "채널 %s 를 볼 권한이 없어 게임방인지 알 수 없습니다. "
                    "봇에게 '채널 보기' 권한을 주세요.", ch.id,
                )

    @tasks.loop(seconds=TICK_SECONDS)
    async def tick(self):
        await self.safe_sync()

    # ── 연결 상태 ──
    @commands.Cog.listener()
    async def on_ready(self):
        # 새 연결: 꺼져 있던 시간은 알 수 없으므로 더하지 않는다
        async with self.lock:
            await self.pool.execute("UPDATE voice_session SET together_since = NULL")
            self.online = True
        await self.safe_sync()
        if not self.tick.is_running():
            self.tick.start()
        log.info("준비 완료 (인정 기준 %s명, %s분)", MIN_MEMBERS, MIN_MINUTES)

    @commands.Cog.listener()
    async def on_resumed(self):
        self.online = True
        await self.safe_sync()

    @commands.Cog.listener()
    async def on_disconnect(self):
        self.online = False

    # ── 음성/채널 이벤트 ──
    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        # 마이크 끄기 같은 변화는 무시
        if member.guild.id == self.guild_id and before.channel != after.channel:
            await self.safe_sync()

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before, after):
        # 방이 만들어진 뒤에 역할 권한이 붙는 경우
        if after.guild.id == self.guild_id:
            await self.safe_sync()

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel):
        if channel.guild.id == self.guild_id:
            await self.safe_sync()

    # ── 명령어 (서버 관리 권한 필요, 본인에게만 보임) ──
    async def cog_app_command_error(self, interaction, error):
        if isinstance(error, app_commands.CheckFailure):
            message = "서버 관리 권한이 필요해요."
        else:
            log.error("명령어 오류", exc_info=error)
            message = "처리 중 오류가 났어요. 봇 로그를 확인해 주세요."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    @app_commands.command(name="랭킹", description="게임 횟수 순위를 봅니다")
    @admin_only
    async def ranking(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.safe_sync()
        rows = await self.pool.fetch(
            "SELECT * FROM voice_stats ORDER BY games DESC, total_seconds DESC LIMIT $1",
            RANKING_SIZE,
        )
        lines = [
            f"{i}. <@{r['user_id']}> · **{r['games']}회** · {format_duration(r['total_seconds'])}"
            for i, r in enumerate(rows, 1)
        ]
        embed = discord.Embed(
            title="게임 횟수 랭킹",
            description="\n".join(lines) or "아직 기록이 없어요.",
        )
        embed.set_footer(text=f"{MIN_MEMBERS}명 이상 {MIN_MINUTES}분 이상 함께하면 1회")
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="내보내기", description="전체 유저 기록을 CSV로 받습니다")
    @admin_only
    async def export(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.safe_sync()
        rows = await self.pool.fetch(
            "SELECT * FROM voice_stats ORDER BY games DESC, total_seconds DESC"
        )
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["유저ID", "계정명", "닉네임", "게임횟수", "함께한_분"])
        for r in rows:
            member = interaction.guild.get_member(r["user_id"])
            writer.writerow([
                r["user_id"],
                csv_safe(member.name) if member else "",
                csv_safe(member.display_name) if member else "(서버에 없음)",
                r["games"],
                r["total_seconds"] // 60,
            ])
        data = io.BytesIO(buffer.getvalue().encode("utf-8-sig"))  # 엑셀 한글 깨짐 방지
        await interaction.followup.send(
            file=discord.File(data, filename="voice_stats.csv"), ephemeral=True
        )

    @app_commands.command(name="상태", description="지금 집계 중인 방과 누적 시간을 봅니다")
    @admin_only
    async def status(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.safe_sync()
        rows = await self.pool.fetch(
            "SELECT * FROM voice_session ORDER BY category_id, user_id"
        )
        lines = []
        for r in rows:
            category = interaction.guild.get_channel(r["category_id"])
            state = "누적 중" if r["together_since"] else "멈춤"
            done = " · 인정됨" if r["counted"] else ""
            lines.append(
                f"{category.name if category else r['category_id']} · <@{r['user_id']}> · "
                f"{format_duration(r['together_seconds'])} · {state}{done}"
            )
        hidden = sum(
            not ch.permissions_for(interaction.guild.me).view_channel
            for ch in interaction.guild.voice_channels
        )
        if hidden:
            lines.insert(0, f"⚠️ 봇이 볼 수 없는 음성 채널 {hidden}개")
        text = "\n".join(lines) or "집계 중인 방이 없어요."
        if len(text) > 1900:
            text = text[:1900] + "\n…"
        await interaction.followup.send(
            text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
        )