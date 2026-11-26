"""음성방 게임 횟수 집계 기능 (설정, DB, 정산, 명령어).

게임방 카테고리에서 다른 사람과 함께 있던 시간을 쌓고,
MIN_MEMBERS명 이상 함께한 시간이 방(카테고리) 하나에서 MIN_MINUTES 이상이면 게임 1회로 인정한다.
"""

import asyncio
import csv
import io
import logging
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands, tasks

def env_id(name: str) -> int:
    """환경 변수의 디스코드 ID. 없으면 0."""
    return int(os.getenv(name, "").strip() or 0)


# ───────────── 설정 ─────────────
MIN_MINUTES = 60                  # 함께한 시간이 이 분 이상이면 게임 1회 인정
MIN_MEMBERS = 3                   # 방에 이 인원 이상 있어야 시간을 셈 (본인 포함, 봇 제외)
TICK_SECONDS = 120                 # 중간 정산 주기(초)
RANKING_SIZE = 50                 # /게임랭킹 에 보여줄 인원
ROOM_ROLE_PATTERN = re.compile(r"Room-\d+-Member")  # 게임방을 알아보는 역할 이름
EXCLUDED_CATEGORY_IDS: set[int] = {1508118244727197887, 1509798130760155259, 1508468808849821798}             # 무조건 제외할 카테고리 ID
# 부관리자 역할 ID (환경 변수, 없으면 서버 관리자만 사용)
SUB_MANAGER_ROLE_ID = env_id("SUB_MANAGER_ROLE_ID")

# 월간 정산: 매월 1일, 한국 시간
MONTHLY_FIRST_RUN = "2026-12"     # 이 달(YYYY-MM)의 1일부터 실행
MONTHLY_HOUR = 6                  # 실행 시각(시)
ELITE_REMOVE_MAX = 3              # 이 횟수 이하면 Elite 역할 해제
ELITE_GRANT_MIN = 8               # 이 횟수 이상이면 Elite 역할 부여
ELITE_GM_ROLE_ID = env_id("ELITE_GM_ROLE_ID")
ELITE_PLAYER_ROLE_ID = env_id("ELITE_PLAYER_ROLE_ID")
NORMAL_GM_ROLE_ID = env_id("NORMAL_GM_ROLE_ID")
NORMAL_PLAYER_ROLE_ID = env_id("NORMAL_PLAYER_ROLE_ID")
LOG_CHANNEL_ID = env_id("LOG_CHANNEL_ID")
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
CREATE TABLE IF NOT EXISTS voice_monthly (
    period TEXT        PRIMARY KEY,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

ADD_STATS = """
INSERT INTO voice_stats (user_id, games, total_seconds) VALUES ($1, $2, $3)
ON CONFLICT (user_id) DO UPDATE
SET games = voice_stats.games + EXCLUDED.games,
    total_seconds = voice_stats.total_seconds + EXCLUDED.total_seconds
"""

log = logging.getLogger("voicebot")
KST = timezone(timedelta(hours=9))


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


def user_label(guild, user_id: int) -> str:
    """멘션 옆에 사용자명을 붙인다. 예: <@123> (username)"""
    member = guild.get_member(user_id)
    name = discord.utils.escape_markdown(member.name) if member else "서버에 없음"
    return f"<@{user_id}> ({name})"


def split_lines(lines: list[str], limit: int) -> list[str]:
    """디스코드 글자 수 제한에 맞게 줄 단위로 나눈다."""
    chunks: list[str] = []
    for line in lines:
        if chunks and len(chunks[-1]) + 1 + len(line) <= limit:
            chunks[-1] += "\n" + line
        else:
            chunks.append(line)
    return chunks


def build_csv(guild, rows) -> discord.File:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["유저ID", "계정명", "닉네임", "게임횟수", "누적시간(분)"])
    for r in rows:
        member = guild.get_member(r["user_id"])
        writer.writerow([
            r["user_id"],
            csv_safe(member.name) if member else "",
            csv_safe(member.display_name) if member else "(서버에 없음)",
            r["games"],
            r["total_seconds"] // 60,
        ])
    data = io.BytesIO(buffer.getvalue().encode("utf-8-sig"))  # 엑셀 한글 깨짐 방지
    return discord.File(data, filename="voice_stats.csv")


# ───────────── 월간 정산 ─────────────
def due_period(now: datetime) -> str | None:
    """지금이 정산할 때면 "YYYY-MM", 아니면 None. 1일 당일에만 실행한다."""
    local = now.astimezone(KST)
    if local.day != 1 or local.hour < MONTHLY_HOUR:
        return None
    period = f"{local:%Y-%m}"
    return period if period >= MONTHLY_FIRST_RUN else None


def monthly_problems(guild) -> list[str]:
    """정산을 시작하기 전에 설정과 권한을 확인한다."""
    problems = []
    if not guild.me.guild_permissions.manage_roles:
        problems.append("봇에게 '역할 관리' 권한이 없음")
    for name, role_id, editable in (
        ("ELITE_GM_ROLE_ID", ELITE_GM_ROLE_ID, True),
        ("ELITE_PLAYER_ROLE_ID", ELITE_PLAYER_ROLE_ID, True),
        ("NORMAL_GM_ROLE_ID", NORMAL_GM_ROLE_ID, False),
        ("NORMAL_PLAYER_ROLE_ID", NORMAL_PLAYER_ROLE_ID, False),
    ):
        role = guild.get_role(role_id)
        if role is None:
            problems.append(f"{name} 역할을 찾을 수 없음")
        elif editable and role >= guild.me.top_role:
            problems.append(f"봇의 역할이 '{role.name}' 역할보다 아래에 있음")
    channel = guild.get_channel(LOG_CHANNEL_ID)
    if channel is None:
        problems.append("LOG_CHANNEL_ID 채널을 찾을 수 없음")
    else:
        perms = channel.permissions_for(guild.me)
        if not (perms.view_channel and perms.send_messages and perms.attach_files):
            problems.append("로그 채널의 보기/메시지 보내기/파일 첨부 권한이 부족함")
    return problems


def monthly_plan(guild, games: dict[int, int]) -> tuple[list, list]:
    """(해제 목록, 부여 목록). 각 항목은 (멤버, 역할 목록, 횟수)."""
    elite_gm = guild.get_role(ELITE_GM_ROLE_ID)
    elite_player = guild.get_role(ELITE_PLAYER_ROLE_ID)
    pairs = (
        (guild.get_role(NORMAL_PLAYER_ROLE_ID), elite_player),
        (guild.get_role(NORMAL_GM_ROLE_ID), elite_gm),
    )

    removals = []
    for member in set(elite_gm.members) | set(elite_player.members):
        count = games.get(member.id, 0)  # 기록이 없으면 0회
        if not member.bot and count <= ELITE_REMOVE_MAX:
            roles = [r for r in (elite_gm, elite_player) if r in member.roles]
            removals.append((member, roles, count))

    grants = []
    for user_id, count in games.items():
        member = guild.get_member(user_id)
        if member is None or member.bot or count < ELITE_GRANT_MIN:
            continue
        roles = [
            elite for normal, elite in pairs
            if normal in member.roles and elite not in member.roles
        ]
        if roles:
            grants.append((member, roles, count))
    return removals, grants


def preview_lines(guild, games: dict[int, int]) -> list[str]:
    """지금 정산한다면 어떻게 되는지. 아무것도 바꾸지 않는다."""
    lines = ["**역할 변경 미리보기** (현재 기록 기준, 아무것도 바꾸지 않았어요)"]
    problems = monthly_problems(guild)
    if problems:
        lines += [f"⚠️ 이대로면 정산이 실행되지 않아요: {problem}" for problem in problems]
    role_ids = (ELITE_GM_ROLE_ID, ELITE_PLAYER_ROLE_ID, NORMAL_GM_ROLE_ID, NORMAL_PLAYER_ROLE_ID)
    if any(guild.get_role(role_id) is None for role_id in role_ids):
        return lines  # 역할을 모르면 대상을 계산할 수 없음
    if not any(games.values()):
        lines.append("⚠️ 1회 이상 참여한 기록이 없어 정산이 중단돼요.")
        return lines

    removals, grants = monthly_plan(guild, games)
    for title, plan in (("해제", removals), ("부여", grants)):
        lines.append(f"**Elite {title} 예정 {len(plan)}명**")
        for member, roles, count in sorted(plan, key=lambda x: (-x[2], x[0].name)):
            names = discord.utils.escape_markdown(", ".join(r.name for r in roles))
            lines.append(f"- {user_label(guild, member.id)} · {count}회 · {names}")
        if not plan:
            lines.append("- 없음")
    return lines


def removal_message(guild_name: str, roles: str, count: int) -> str:
    return (
        f"안녕하세요, **{guild_name}** 서버에서 알려드려요.\n"
        f"지난 집계 기간의 게임 참여가 활동 기준에 미치지 못해 "
        f"**{roles}** 역할이 해제되었어요.\n"
        "앞으로도 게임에서 자주 만나요!"
    )


def grant_message(guild_name: str, roles: str, count: int) -> str:
    return (
        f"축하해요! 🎉 **{guild_name}** 서버에서 알려드려요.\n"
        f"지난 집계 기간에 게임 참여가 활동 기준을 채워서 "
        f"**{roles}** 역할이 부여되었어요.\n"
        "함께해 주셔서 감사해요!"
    )


async def change_roles(guild, plan, *, grant: bool) -> list[str]:
    """역할을 바꾸고 DM을 보낸 뒤, 로그에 남길 줄을 돌려준다."""
    lines = []
    for member, roles, count in plan:
        names = ", ".join(r.name for r in roles)
        line = (
            f"- {user_label(guild, member.id)} · {count}회 · "
            f"{discord.utils.escape_markdown(names)}"
        )
        try:
            change = member.add_roles if grant else member.remove_roles
            await change(*roles, reason="월간 게임 활동 정산")
        except discord.HTTPException:
            log.exception("역할 변경 실패: user=%s", member.id)
            lines.append(line + " · ⚠️ 역할 변경 실패")
            continue
        log.info("역할 %s: user=%s roles=%s", "부여" if grant else "해제", member.id, names)
        try:
            message = grant_message if grant else removal_message
            await member.send(message(guild.name, names, count))
        except discord.HTTPException:
            line += " · DM 전송 실패"
        lines.append(line)
    return lines


def admin_only(func):
    func = app_commands.checks.has_permissions(manage_guild=True)(func)
    func = app_commands.default_permissions(manage_guild=True)(func)
    return app_commands.guild_only()(func)


def is_staff(interaction) -> bool:
    """서버 관리자 또는 부관리자 역할."""
    if interaction.permissions.manage_guild:
        return True
    return any(
        role.id == SUB_MANAGER_ROLE_ID for role in getattr(interaction.user, "roles", [])
    )


def staff_only(func):
    """관리자 + 부관리자용. 부관리자에게 보이게 하려면 서버 설정 → 연동에서도 허용해야 한다."""
    func = app_commands.check(is_staff)(func)
    func = app_commands.default_permissions(manage_guild=True)(func)
    return app_commands.guild_only()(func)


class ResetConfirmView(discord.ui.View):
    """기록 초기화 전 [삭제] / [취소] 확인."""

    def __init__(self, cog: "VoiceTracker", interaction: discord.Interaction):
        super().__init__(timeout=60)
        self.cog = cog
        self.origin = interaction  # 시간 초과 때 확인 메시지를 고치기 위해 보관

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return (
            interaction.user.id == self.origin.user.id
            and interaction.permissions.manage_guild
        )

    @discord.ui.button(label="삭제", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()  # 두 번 눌러도 한 번만 실행
        await interaction.response.edit_message(content="삭제하는 중…", view=None)
        try:
            await self.cog.reset_stats()
        except Exception:
            log.exception("기록 초기화 실패")
            result = "초기화하지 못했어요. 봇 로그를 확인해 주세요."
        else:
            log.warning("게임 기록 초기화: 실행한 유저=%s", interaction.user.id)
            result = "게임 기록을 모두 삭제했어요."
        await interaction.edit_original_response(content=result)

    @discord.ui.button(label="취소", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(content="취소했어요. 기록은 그대로예요.", view=None)

    async def on_timeout(self):
        try:
            await self.origin.edit_original_response(
                content="시간이 지나 취소됐어요. 기록은 그대로예요.", view=None
            )
        except discord.HTTPException:
            pass  # 확인 메시지를 이미 닫은 경우


class VoiceTracker(commands.Cog):
    def __init__(self, bot: commands.Bot, pool: asyncpg.Pool, guild_id: int):
        self.bot = bot
        self.pool = pool
        self.guild_id = guild_id
        self.lock = asyncio.Lock()  # 정산이 겹치지 않게
        self.online = False         # 연결이 끊긴 동안에는 정산하지 않음
        self.warned: set[int] = set()
        self.monthly_error: str | None = None  # 같은 오류를 반복해서 알리지 않게

    async def cog_load(self):
        await self.pool.execute(SCHEMA)

    async def cog_unload(self):
        self.tick.cancel()
        self.monthly.cancel()

    async def sync_voice(self):
        async with self.lock:
            await self._sync_locked()

    async def _sync_locked(self):
        """self.lock 을 잡은 상태에서 호출한다."""
        guild = self.bot.get_guild(self.guild_id)
        if not self.online or guild is None or guild.unavailable:
            return
        self.warn_hidden(guild)
        present, occupied = read_voice_state(guild)
        async with self.pool.acquire() as conn, conn.transaction():
            await apply_snapshot(conn, present, occupied, discord.utils.utcnow())

    async def reset_stats(self):
        async with self.lock:
            # 지금까지의 시간을 먼저 정산해야 초기화 뒤 기록에 섞이지 않는다
            await self._sync_locked()
            await self.pool.execute("TRUNCATE voice_stats")

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

    # ── 월간 정산 ──
    @tasks.loop(minutes=1)
    async def monthly(self):
        period = due_period(discord.utils.utcnow())
        if period is None:
            return
        try:
            await self.run_monthly(period)
        except Exception:
            log.exception("월간 정산 실패 (period=%s)", period)

    async def run_monthly(self, period: str):
        async with self.lock:
            if await self.pool.fetchval("SELECT 1 FROM voice_monthly WHERE period = $1", period):
                return  # 이번 달은 이미 실행함
            guild = self.bot.get_guild(self.guild_id)
            if not self.online or guild is None or guild.unavailable or not guild.chunked:
                return  # 다음 분에 다시 시도

            channel = guild.get_channel(LOG_CHANNEL_ID)
            problems = monthly_problems(guild)
            if problems:
                await self.report_monthly_error(channel, ", ".join(problems))
                return

            await self._sync_locked()
            rows = await self.pool.fetch(
                "SELECT * FROM voice_stats ORDER BY games DESC, total_seconds DESC"
            )
            games = {r["user_id"]: r["games"] for r in rows}
            if not any(games.values()):
                # 집계 고장이나 직전 초기화로 모두가 해제되는 것을 막는다
                await self.report_monthly_error(channel, "1회 이상 참여한 기록이 하나도 없음")
                return
            removals, grants = monthly_plan(guild, games)

            # 1) CSV 업로드. 실패하면 여기서 멈추고 아무것도 바꾸지 않는다
            await channel.send(
                f"📄 {period} 월간 정산: 초기화 전 게임 기록",
                file=build_csv(guild, [r for r in rows if r["games"] > 0]),
            )
            # 2) 실행 기록과 초기화를 한 번에: 두 번 실행되지 않게 한다
            async with self.pool.acquire() as conn, conn.transaction():
                claimed = await conn.fetchval(
                    "INSERT INTO voice_monthly (period) VALUES ($1) "
                    "ON CONFLICT DO NOTHING RETURNING period", period,
                )
                if claimed is None:
                    return
                await conn.execute("TRUNCATE voice_stats")
            log.warning("월간 정산 %s: 게임 기록 초기화", period)

        # 3) 역할 변경과 DM (집계가 멈추지 않도록 잠금 밖에서)
        removed = await change_roles(guild, removals, grant=False)
        granted = await change_roles(guild, grants, grant=True)

        # 4) 결과 로그
        lines = [f"**{period} 월간 정산 결과**", f"**Elite 해제 {len(removed)}명**"]
        lines += removed or ["- 없음"]
        lines += [f"**Elite 부여 {len(granted)}명**"]
        lines += granted or ["- 없음"]
        lines += ["게임 기록을 초기화했어요."]
        for text in split_lines(lines, 2000):
            await channel.send(text, allowed_mentions=discord.AllowedMentions.none())

    async def report_monthly_error(self, channel, reason: str):
        if reason == self.monthly_error:
            return
        self.monthly_error = reason
        log.error("월간 정산을 실행하지 못함: %s", reason)
        if channel is not None:
            try:
                await channel.send(f"⚠️ 월간 정산을 실행하지 못했어요: {reason}")
            except discord.HTTPException:
                log.exception("로그 채널에 알리지 못함")

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
        if not self.monthly.is_running():
            self.monthly.start()
        guild = self.bot.get_guild(self.guild_id)
        if guild is not None:
            problems = monthly_problems(guild)
            if problems:
                log.warning("월간 정산 설정 확인 필요: %s", ", ".join(problems))
            else:
                log.info("월간 정산 설정 정상 (첫 실행 %s-01 %s시)", MONTHLY_FIRST_RUN, MONTHLY_HOUR)
        log.info(
            "준비 완료 (인정 기준 %s명, %s분, 부관리자 역할 %s)",
            MIN_MEMBERS, MIN_MINUTES, SUB_MANAGER_ROLE_ID or "없음",
        )

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
            message = "이 명령어를 쓸 권한이 없어요."
        else:
            log.error("명령어 오류", exc_info=error)
            message = "처리 중 오류가 났어요. 봇 로그를 확인해 주세요."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    @app_commands.command(name="게임랭킹", description="게임 횟수 순위를 봅니다")
    @staff_only
    async def ranking(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.safe_sync()
        rows = await self.pool.fetch(
            "SELECT * FROM voice_stats WHERE games > 0 "
            "ORDER BY games DESC, total_seconds DESC LIMIT $1",
            RANKING_SIZE,
        )
        lines = [
            f"{i}. {user_label(interaction.guild, r['user_id'])} · **{r['games']}회** · "
            f"{format_duration(r['total_seconds'])}"
            for i, r in enumerate(rows, 1)
        ]
        pages = split_lines(lines, 4096) or ["아직 기록이 없어요."]
        for number, page in enumerate(pages, 1):
            embed = discord.Embed(title="게임 횟수 랭킹", description=page)
            footer = "" #f"{MIN_MEMBERS}명 이상 {MIN_MINUTES}분 이상 함께하면 1회"
            if len(pages) > 1:
                footer += f" · {number}/{len(pages)}"
            embed.set_footer(text=footer)
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="내보내기", description="전체 유저 기록을 CSV로 받습니다")
    @staff_only
    async def export(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.safe_sync()
        rows = await self.pool.fetch(
            "SELECT * FROM voice_stats WHERE games > 0 "
            "ORDER BY games DESC, total_seconds DESC"
        )
        await interaction.followup.send(
            file=build_csv(interaction.guild, rows), ephemeral=True
        )

    @app_commands.command(name="게임기록초기화", description="게임 횟수와 누적 시간을 모두 삭제합니다")
    @admin_only
    async def reset(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "⚠️ **모든 유저의 게임 횟수와 누적 시간을 삭제할까요?**\n"
            "되돌릴 수 없어요. 보관이 필요하면 먼저 `/내보내기`를 실행하세요.",
            view=ResetConfirmView(self, interaction),
            ephemeral=True,
        )

    @app_commands.command(name="역할변경미리보기", description="월간 정산에서 해제·부여될 대상을 미리 봅니다")
    @staff_only
    async def preview(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.safe_sync()
        rows = await self.pool.fetch("SELECT user_id, games FROM voice_stats")
        games = {r["user_id"]: r["games"] for r in rows}
        for text in split_lines(preview_lines(interaction.guild, games), 2000):
            await interaction.followup.send(
                text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
            )

    @app_commands.command(name="게임상태", description="지금 집계 중인 방과 누적 시간을 봅니다")
    @staff_only
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
                f"{category.name if category else r['category_id']} · "
                f"{user_label(interaction.guild, r['user_id'])} · "
                f"{format_duration(r['together_seconds'])} · {state}{done}"
            )
        hidden = sum(
            not ch.permissions_for(interaction.guild.me).view_channel
            for ch in interaction.guild.voice_channels
        )
        if hidden:
            lines.insert(0, f"⚠️ 봇이 볼 수 없는 음성 채널 {hidden}개")
        for text in split_lines(lines, 2000) or ["집계 중인 방이 없어요."]:
            await interaction.followup.send(
                text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none()
            )