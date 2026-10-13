import asyncio
import csv
import io
import logging
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import math
import os
from pathlib import Path
import queue
import re
import sys
import tempfile
import threading
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Tuple, Any

import discord
from discord.ext import commands
from discord import app_commands

BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger("millionlords")

# =========================
# 기본 설정
# =========================

GUILD_ID = 1362775927444078593 #서버 ID
GUILD = discord.Object(id=GUILD_ID)

# 역할 ID
Builder = 1430983480527028356 # 빌더 # 결과 메시지에 태그할 빌더 역할 ID (@Builder)
Captain = 1431016116460720272 # 관리자 ID
Admin = 332469454669611019  # 내 ID

# 채팅 청소방
Chat_Cleaning_Room_ID = [
    1431013744086876261,  #다이아 방
    1431015469590118500,  #레전드 방
    1430986350886387911,  #빌드 방
    1362844075224596744   #봇 실험실
]

#빌드 허용 방
building_chat_ID = [1430986350886387911, 1362844075224596744]   # 공격력 명령 허용 채널

# ✅ 채팅 로그 출력 채널
LOG_CHANNEL_ID = 1364934035750650008


# =========================
#  명령어 한국어로 변환작업 ( 원래는 안해도 되는 작업이지만 코드인식을 안하는건지 길드에 등록이 안되어 내가 직접 넣음 )
# =========================

class KoTranslator(app_commands.Translator):
    TABLE = {
        # 명령 이름
        "cc": "ㅊㅊ",
        "_add_member": "팀원추가",
        "_delete_team": "팀원삭제",
        "sync": "동기화",
        "botstop": "봇종료",
        "jewel" : "보석",
        "help" : "명령어",
        "lb": "레벨빌드",

        # 명령 설명
        "Clean this channel": "현재 채팅방의 모든 메시지를 청소합니다.",
        "Add a team": "팀을 추가합니다.",
        "Add a member to team": "팀에 팀원을 추가합니다.",
        "Remove a team": "팀을 삭제합니다.",
        "Remove a member from a team (team optional)": "팀원 삭제 (팀명 생략 시 모든 팀에서 닉네임 삭제)",
        "Sync slash commands to this server": "슬래시 명령어를 이 서버에 다시 동기화합니다.",
        "Shut down the bot (admin only)": "봇을 종료합니다. (관리자 전용)",
        "Lookup by date or type" : "원하는 보석날짜를 확인합니다.",
        "Show all bot commands and examples":"봇 명령어들에 대한 사용법을 알려줍니다",

        # 옵션 라벨
        "Team name": "팀명",
        "Nickname": "닉네임",
        "Team name (optional)": "소속팀 (선택)",
    }

    async def translate(
        self,
        string: app_commands.locale_str,
        locale: discord.Locale,
        context: app_commands.TranslationContext,
    ) -> str | None:
        # 한국어 클라이언트에게만 한글 제공
        if locale is discord.Locale.korean:
            key = str(string)
            return self.TABLE.get(key)
        return None

# 로그 파일은 봇 파일과 같은 폴더에 저장합니다.
LOG_DIR = BASE_DIR / "Log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
_command_log_handler = RotatingFileHandler(
    LOG_DIR / "bot_log.txt", maxBytes=5 * 1024 * 1024,
    backupCount=5, encoding="utf-8", delay=True,
)
_command_log_handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))


def _write_file_log(line: str):
    record = logging.LogRecord("commands", logging.INFO, __file__, 0, line, (), None)
    _command_log_handler.handle(record)


# ---------- CSV 로그 파일 (엑셀용) ----------


_csv_lock = threading.Lock()

def _current_month_csv_file() -> Path:
    """현재 월 기준 CSV 파일 경로 반환 (예: bot_log_2025-09.csv)"""
    month_str = datetime.now().strftime("%Y-%m")
    return LOG_DIR / f"bot_log_{month_str}.csv"

def _ensure_month_csv_header(csv_path: Path):
    """월별 CSV 파일의 헤더를 보장"""
    need_header = (not csv_path.exists()) or (csv_path.stat().st_size == 0)
    if need_header:
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "command", "user_name", "user_id", "input", "result", "channel"])

def _write_month_csv_row(row: list[str]):
    """월별 CSV 파일에 1행 추가"""
    csv_path = _current_month_csv_file()
    with _csv_lock:
        _ensure_month_csv_header(csv_path)
        with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)


# 큐 크기와 항목 길이를 모두 제한합니다. 로그마다 Task를 생성하지 않습니다.
LOG_QUEUE_LIMIT = 500
LOG_TEXT_LIMIT = 12000
_disk_log_queue: asyncio.Queue = asyncio.Queue(maxsize=LOG_QUEUE_LIMIT)
_channel_log_queue: asyncio.Queue = asyncio.Queue(maxsize=LOG_QUEUE_LIMIT)


def _enqueue_log(target: asyncio.Queue, item) -> None:
    if target.full():
        target.get_nowait()
        target.task_done()
        logger.warning("로그 큐가 가득 차 가장 오래된 항목을 버렸습니다: %s",
                       "disk" if target is _disk_log_queue else "channel")
    target.put_nowait(item)


def _persist_command_log(line: str, row) -> None:
    try:
        _write_file_log(line)
    except Exception:
        logger.exception("명령 로그 파일 기록 실패")
    if row is not None:
        try:
            _write_month_csv_row(row)
        except Exception:
            logger.exception("CSV 로그 기록 실패")


async def _disk_log_worker():
    while True:
        line, row = await _disk_log_queue.get()
        try:
            await asyncio.to_thread(_persist_command_log, line, row)
        finally:
            _disk_log_queue.task_done()


async def _channel_log_worker():
    while True:
        await bot.wait_until_ready()
        text = await _channel_log_queue.get()
        try:
            # 실패한 항목을 무한 재적재하지 않습니다. 원문은 파일 로그에 남습니다.
            for attempt in range(3):
                try:
                    await bot.wait_until_ready()
                    async with asyncio.timeout(30):
                        channel = bot.get_channel(LOG_CHANNEL_ID)
                        if channel is None:
                            channel = await bot.fetch_channel(LOG_CHANNEL_ID)
                        for i in range(0, len(text), 1900):
                            await channel.send(text[i:i + 1900],
                                               allowed_mentions=discord.AllowedMentions.none())
                    break
                except (discord.HTTPException, OSError, asyncio.TimeoutError):
                    if attempt == 2:
                        logger.exception("채널 로그 전송 실패: 파일 로그를 확인하세요")
                    else:
                        await asyncio.sleep(5 * (attempt + 1))
                except Exception:
                    logger.exception("채널 로그 처리 오류")
                    break
        finally:
            _channel_log_queue.task_done()


def _emit_log(line: str, row=None):
    line = line[:LOG_TEXT_LIMIT]
    if row is not None:
        row = [str(value)[:LOG_TEXT_LIMIT] for value in row]
    _enqueue_log(_disk_log_queue, (line, row))
    _enqueue_log(_channel_log_queue, line)


def log_command(command_label: str, user: discord.abc.User, input_text: str, result_text: str, channel: discord.abc.GuildChannel):
    nick = getattr(user, "display_name", str(user))
    ch_name = getattr(channel, "name", str(channel.id))

    stamp_txt = datetime.now().strftime("%Y-%m-%d %H:%M")
    stamp_csv = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def replace_mentions(text: str) -> str:
        guild = getattr(channel, "guild", None)
        if not guild:
            return text

        for uid in set(re.findall(r"<@!?(\d+)>", text)):
            member = guild.get_member(int(uid))
            if member:
                display = member.display_name
                text = text.replace(f"<@{uid}>", f"@{display}")
                text = text.replace(f"<@!{uid}>", f"@{display}")

        for rid in set(re.findall(r"<@&(\d+)>", text)):
            role = guild.get_role(int(rid))
            if role:
                text = text.replace(f"<@&{rid}>", f"@{role.name}")

        for cid in set(re.findall(r"<#(\d+)>", text)):
            ch = guild.get_channel(int(cid))
            if ch and getattr(ch, "name", None):
                text = text.replace(f"<#{cid}>", f"#{ch.name}")
        return text

    def neutralize_at_signs(text: str) -> str:
        text = text.replace("@everyone", "@\u200beveryone")
        text = text.replace("@here", "@\u200bhere")
        return re.sub(r"@(?=\S)", "@\u200b", text)

    result_text_clean = neutralize_at_signs(replace_mentions(result_text))

    line = (
        f"[{stamp_txt}] [명령어:{command_label}] "
        f"[사용자 : {nick} ({user.id})] "
        f"[입력값 : {input_text}] "
        f"[결과값 : {result_text_clean}] "
        f"[ 사용위치 : {ch_name} ]"
    )

    _emit_log(line, [
        stamp_csv,
        command_label,
        nick,
        str(user.id),
        input_text,
        result_text_clean,
        ch_name,
    ])


# =========================
# 디스코드 봇 초기화
# =========================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

allowed_mentions = discord.AllowedMentions(everyone=False, users=True, roles=True)
class MillionLordsBot(commands.Bot):
    async def setup_hook(self):
        await self.tree.set_translator(KoTranslator())
        try:
            cmds = await self.tree.sync(guild=GUILD)
            logger.info("슬래시 명령 %d개 동기화 완료", len(cmds))
        except discord.HTTPException:
            logger.exception("슬래시 명령 동기화 실패 (/sync로 재시도 가능)")
        self.log_workers = [
            asyncio.create_task(_disk_log_worker(), name="disk-log-worker"),
            asyncio.create_task(_channel_log_worker(), name="channel-log-worker"),
        ]

    async def close(self):
        workers = getattr(self, "log_workers", [])
        if workers:
            try:
                await asyncio.wait_for(_disk_log_queue.join(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("종료 시 파일 로그 대기 시간 초과")
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self.log_workers = []
        await super().close()


bot = MillionLordsBot(command_prefix="!", intents=intents,
                       allowed_mentions=allowed_mentions, max_messages=500)
tree = bot.tree

# =========================
# 공격력 기준표 (레벨, "공격력")  ✅ (기존 그대로)
# =========================
LEVEL_ATTACK_RAW = [
    (61, "3.94M"), (62, "4.73M"), (63, "5.68M"), (64, "6.81M"), (65, "8.18M"),
    (66, "9.81M"), (67, "11.78M"), (68, "14.13M"), (69, "16.96M"), (70, "20.35M"),
    (71, "24.42M"), (72, "29.3M"), (73, "35.16M"), (74, "42.2M"), (75, "50.63M"),
    (76, "60.76M"), (77, "72.91M"), (78, "87.5M"), (79, "105M"), (80, "126M"),
    (81, "151M"), (82, "181M"), (83, "218M"), (84, "261M"), (85, "314M"),
    (86, "376M"), (87, "451M"), (88, "542M"), (89, "650M"), (90, "780M"),
    (91, "936M"), (92, "1.12G"), (93, "1.35G"), (94, "1.62G"), (95, "1.94G"),
    (96, "2.33G"), (97, "2.80G"), (98, "3.35G"), (99, "4.03G"), (100, "4.83G"),
    (101, "5.80G"), (102, "6.96G"), (103, "8.35G"), (104, "10.02G"), (105, "12.02G"),
    (106, "14.42G"), (107, "17.31G"), (108, "20.77G"), (109, "24.92G"), (110, "29.91G"),
    (111, "35.89G"), (112, "43.07G"), (113, "51.68G"), (114, "62.02G"), (115, "74.42G"),
    (116, "89.31G"), (117, "107.17G"), (118, "128.61G"), (119, "154.33G"), (120, "185.19G"),
    (121, "222.3G"), (122, "266.7G"), (123, "320.0G"), (124, "384.0G"), (125, "460.8G"),
    (126, "553.0G"), (127, "663.6G"), (128, "796.3G"), (129, "955.5G"), (130, "1.14T"),
    (131, "1.37T"), (132, "1.65T"), (133, "1.98T"), (134, "2.38T"), (135, "2.85T"),
    (136, "3.42T"), (137, "4.11T"), (138, "4.93T"), (139, "5.92T"), (140, "7.10T"),
    (141, "8.52T"), (142, "10.2T"), (143, "12.24T"), (144, "14.7T"), (145, "17.62T"),
    (146, "21.15T"), (147, "25.38T"), (148, "30.45T"), (149, "36.55T"), (150, "43.86T"),
    (151, "52.63T"), (152, "63.3T"), (153, "75.9T"), (154, "91.1T"), (155, "109.2T"),
    (156, "131T"), (157, "157.2T"), (158, "189T"), (159, "226.4T"), (160, "271.7T"),
    (161, "326.1T"), (162, "391.3T"), (163, "469.6T"), (164, "563.5T"), (165, "677T"),
    (166, "811.4T"), (167, "975T"), (168, "1.17P"), (169, "1.40P"), (170, "1.68P"),
    (171, "2.02P"), (172, "2.42P"), (173, "2.90P"), (174, "3.48P"), (175, "4.18P"),
    (176, "5.03P"), (177, "6.02P"), (178, "7.24P"), (179, "8.68P"), (180, "10.41P"),
    (181, "12.5P"), (182, "15P"), (183, "18P"), (184, "21.92P"), (185, "25.9P"),
    (186, "31.10P"), (187, "37.32P"), (188, "44.79P"), (189, "53.75P"), (190, "64.50P"),
    (191, "77.39P"), (192, "92.87P"), (193, "111.45P"), (194, "133.74P"), (195, "160.48P"),
    (196, "192.58P"), (197, "231.10P"), (198, "277.32P"), (199, "332.78P"), (200, "399.34P"),
]

LEVEL_TO_RAW = {lv: raw for lv, raw in LEVEL_ATTACK_RAW}

UNIT_MAP = {
    "M": 1e6,   "ㅡ": 1e6,
    "G": 1e9,   "ㅎ": 1e9,
    "T": 1e12,  "ㅅ": 1e12,
    "P": 1e15,  "ㅔ": 1e15,
}

def parse_power_str(s: str) -> float:
    s = (s or "").strip().replace(",", "")
    if not s:
        raise ValueError("입력값이 비어 있습니다.")
    unit = s[-1]

    if unit.upper() in UNIT_MAP:
        unit = unit.upper()

    if unit not in UNIT_MAP:
        raise ValueError("지원하지 않는 단위입니다. (M/G/T/P 또는 ㅡ/ㅎ/ㅅ/ㅔ)")

    val = float(s[:-1])
    result = val * UNIT_MAP[unit]
    if not math.isfinite(result) or result <= 0:
        raise ValueError("유한한 양수만 사용할 수 있습니다.")
    return result


# =========================
# ✅ 추가 기능: 공격% 파싱 + (+100%) 적용 후 최종 공격력 계산
# =========================
def parse_percent_str(s: str) -> float:
    s = (s or "").strip().replace(" ", "")
    if not s:
        raise ValueError("공격력% 입력값이 비어 있습니다.")
    if not s.endswith("%"):
        raise ValueError("공격력% 형식이 올바르지 않습니다. (예: 535.75%)")
    try:
        val = float(s[:-1])
    except ValueError:
        raise ValueError("공격력% 형식이 올바르지 않습니다. (예: 535.75%)")
    if not math.isfinite(val) or val < 0:
        raise ValueError("공격력%는 유한한 0 이상의 수여야 합니다.")
    return val

def apply_attack_percent(base_power: float, input_percent: float) -> float:
    """
    ✅ 규칙:
    - 입력 공격력% = '추가 공격력%'
    - 내부 계산 시 +100% 자동 적용
    - 최종 공격력 = 병력 × ( (입력% + 100) / 100 )
    """
    applied = input_percent + 100.0
    result = base_power * (applied / 100.0)
    if not math.isfinite(result) or result <= 0:
        raise ValueError("계산 가능한 공격력 범위를 벗어났습니다.")
    return result


LEVEL_ATTACK = [(lv, parse_power_str(p)) for lv, p in LEVEL_ATTACK_RAW]

def find_max_level(user_power: float):
    best = None
    for lv, req in LEVEL_ATTACK:
        if user_power >= req:
            best = lv
        else:
            break
    return best

def calculate_city_wall(level: int) -> float:
    """성벽 방어력 공식: 70 * 1.2^(level-1)"""
    return 70 * (1.2 ** (level - 1))

def format_power_unit(val: float) -> str:
    """숫자를 P, T, G, M 단위로 보기 좋게 변환"""
    if val >= 1e15: return f"{val/1e15:.2f}P"
    if val >= 1e12: return f"{val/1e12:.2f}T"
    if val >= 1e9: return f"{val/1e9:.2f}G"
    if val >= 1e6: return f"{val/1e6:.2f}M"
    return f"{val:,.0f}"

def clamp_level(lv: int) -> int:
    return max(61, min(200, lv))

# ── 출력 메시지 ──────────────────────────────────────────────────────────────
def make_single_line_result(final_level: int, author: discord.abc.User) -> str:
    wall_str = LEVEL_TO_RAW.get(final_level, "")
    wall_part = f" [ 🏰 성 방벽 : {wall_str} ]" if wall_str else ""
    return f"[ ⚔️ 최종 목표 레벨 : {final_level} ]{wall_part} <@&{Builder}> {author.mention}"


# =========================
# !공격력 (허용 채널 체크 + 채팅/파일/채널 로그)
# ✅ 확장: (병력, 공격%) 입력 지원 + 공격%에 +100 자동 적용
# =========================
@bot.command(name="공격력")
async def 공격력_cmd(ctx, *args):

    if ctx.channel.id not in building_chat_ID:
        warn = f"{ctx.author.mention} [ 빌더방에서 명령어를 입력해주세요 ]"
        await ctx.send(warn, delete_after=30)
        log_command("!", ctx.author, "!공격력 " + " ".join(args), warn, ctx.channel)
        return

    if not args:
        msg = f"{ctx.author.mention}\n❌ 사용법: `!공격력 <병력> [오프셋]` 또는 `!공격력 <병력> <공격력%> [오프셋]` ❌"
        await ctx.send(msg, delete_after=10)
        log_command("!", ctx.author, "!공격력", "인자 없음", ctx.channel)
        return

    power_text = None
    pct_text = None
    offset = 0

    try:
        if len(args) == 1:

            power_text = args[0]

        elif len(args) == 2:

            power_text = args[0]
            if str(args[1]).strip().endswith("%"):
                pct_text = args[1]
            else:
                offset = int(args[1])

        else:
            power_text = args[0]
            pct_text = args[1]
            offset = int(args[2])

        base_power = parse_power_str(power_text)

        if pct_text is not None:
            input_pct = parse_percent_str(pct_text)
            user_power = apply_attack_percent(base_power, input_pct)
        else:
            user_power = base_power

    except ValueError:
        msg = f"{ctx.author.mention}\n❌ 입력값이 잘못되었습니다 입력값을 확인해주세요 ❌"
        await ctx.send(msg, delete_after=10)
        log_command("!", ctx.author, "!공격력 " + " ".join(args), "입력값 오류", ctx.channel)
        return
    except Exception:
        msg = f"{ctx.author.mention}\n❌ 입력값이 잘못되었습니다 입력값을 확인해주세요 ❌"
        await ctx.send(msg, delete_after=10)
        log_command("!", ctx.author, "!공격력 " + " ".join(args), "입력값 오류", ctx.channel)
        return

    base_level = find_max_level(user_power)
    if base_level is None:
        result = f"<@&{Builder}> {ctx.author.mention}\n⚠️ 해당 공격력으로 공격 가능한 레벨이 없습니다."
        await ctx.send(result, delete_after=10)
        log_command("!", ctx.author, "!공격력 " + " ".join(args), "공격 가능 레벨 없음", ctx.channel)
        return

    final_lv = clamp_level(base_level + offset)
    out = make_single_line_result(final_lv, ctx.author)
    await ctx.send(out)
    log_command("!", ctx.author, "!공격력 " + " ".join(args), out, ctx.channel)


# =========================
# 자유형 입력 (!24.42M -1 등) — 허용 채널 체크 + 로그
# ✅ 확장: !479M 535.75% -1 도 지원 (+100 자동)
# =========================
POWER_LINE = re.compile(
    r"^!(\d+(?:\.\d+)?)\s*([mMgGtTpPㅡㅎㅅㅔ])(?:\s+(\d+(?:\.\d+)?)\s*%)?(?:\s*(-?\d+))?\s*$"
)

MISSING_UNIT_LINE = re.compile(r"^!\s*(\d+(?:\.\d+)?)\s*(?:-?\d+)?\s*$")
MISSING_UNIT_WITH_PCT_LINE = re.compile(r"^!\s*(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)\s*%(?:\s*-?\d+)?\s*$")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    content = message.content.strip()
    m = POWER_LINE.match(content)


    if MISSING_UNIT_LINE.match(content) or MISSING_UNIT_WITH_PCT_LINE.match(content):
        warn = f"{message.author.mention}\n❌ 입력값이 잘못되었습니다 입력값을 확인해주세요 ❌"
        await message.channel.send(warn, delete_after=10)
        log_command("!", message.author, content, "입력값 오류(단위 누락)", message.channel)
        return

    if m:

        if message.channel.id not in building_chat_ID:
            warn = f"{message.author.mention} [ 빌더방에서 명령어를 입력해주세요 ]"
            await message.channel.send(warn, delete_after=30)
            log_command("!", message.author, content, warn, message.channel)
            return


        num_str, unit, pct_num_str, offset_str = m.groups()
        power_text = f"{num_str}{unit.upper()}"

        try:
            base_power = parse_power_str(power_text)

            if pct_num_str is not None:
                input_pct = float(pct_num_str)
                user_power = apply_attack_percent(base_power, input_pct)
            else:
                user_power = base_power

        except ValueError:
            msg = f"{message.author.mention}\n❌ 입력값이 잘못되었습니다 입력값을 확인해주세요 ❌"
            await message.channel.send(msg, delete_after=10)
            log_command("!", message.author, content, "입력값 오류", message.channel)
            return

        base_level = find_max_level(user_power)
        if base_level is None:
            result = f"<@&{Builder}> {message.author.mention}\n⚠️ 해당 공격력으로 공격 가능한 레벨이 없습니다."
            await message.channel.send(result, delete_after=10)
            log_command("!", message.author, content, "공격 가능 레벨 없음", message.channel)
        else:
            offset_val = int(offset_str) if offset_str else 0
            final_lv = clamp_level(base_level + offset_val)
            out = make_single_line_result(final_lv, message.author)
            await _send_long_message(message.channel, out)
            log_command("!", message.author, content, out, message.channel)

        return


    handled = await handle_team_question_command(message)
    if handled:
        return

    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    original = getattr(error, "original", error)
    logger.error("접두사 명령 오류", exc_info=(type(original), original, original.__traceback__))
    await ctx.send("❌ 명령 처리 중 오류가 발생했습니다. 관리자에게 알려주세요.", delete_after=15)


# =========================
# /cc (슬래시) — 이름은 영문, 한국어 표시명 로컬라이즈로 "ㅊㅊ"
# =========================

@tree.command(

    name=app_commands.locale_str("cc", **{"ko": "ㅊㅊ"}),
    description=app_commands.locale_str(
        "Clean this channel",
        **{"ko": "현재 채팅방의 모든 메시지를 청소합니다."}
    ),
    guild=GUILD
)
async def clean_chat(interaction: discord.Interaction):
    await handle_clean_command(interaction, label="/cc", input_literal="/cc")


def _can_manage_teams(user) -> bool:
    return user.id == Admin or any(
        role.id in {Builder, Captain} for role in getattr(user, "roles", ())
    )


async def handle_clean_command(interaction: discord.Interaction, label: str, input_literal: str):

    if not _can_manage_teams(interaction.user):
        await interaction.response.send_message("🚫 권한이 없습니다!", ephemeral=True)
        return


    if interaction.channel_id not in Chat_Cleaning_Room_ID:
        await interaction.response.send_message("⚠️ 이 명령어는 지정된 채널에서만 사용할 수 있습니다.", ephemeral=True)
        return


    await interaction.response.send_message("🧹 채팅 청소중 🧹", ephemeral=True)
    try:
        status_msg = await interaction.original_response()
    except Exception:
        status_msg = None


    try:
        deleted_count = 0
        async for msg in interaction.channel.history(limit=None):
            await msg.delete()
            deleted_count += 1


        log_command(label, interaction.user, input_literal,
                    f"🧹채팅창을 청소했습니다!🧹 (deleted={deleted_count})", interaction.channel)


        if status_msg:
            try:
                await status_msg.edit(content="🧹채팅창을 청소했습니다!🧹")
            except Exception:
                pass
    except Exception as e:
        if status_msg:
            try:
                await status_msg.edit(content=f"❌ 채팅 삭제 중 오류 발생: {e}")
            except Exception:
                pass
        log_command(label, interaction.user, input_literal, f"삭제 오류: {e}", interaction.channel)
        return


    try:
        await asyncio.sleep(180)
        if status_msg:
            await status_msg.delete()
    except Exception:
        pass

# =========================
# /lb 및 /lbl (전체 로그 출력 및 자동 분할 전송)
# =========================

async def run_simulation(interaction: discord.Interaction, troops: str, striker: float, multiplier: float, xp_required: str, loss_rate_pct: float):
    await interaction.response.defer(thinking=True)

    try:
        current_troops = parse_power_str(troops)
        target_xp_val = parse_power_str(xp_required)
        if not math.isfinite(striker) or striker < 0:
            raise ValueError("공격력%는 유한한 0 이상의 수여야 합니다.")
        if not math.isfinite(multiplier) or multiplier <= 0:
            raise ValueError("배율%는 유한한 양수여야 합니다.")
        if not 0 < loss_rate_pct < 1:
            raise ValueError("병력 잔존 비율은 0과 1 사이여야 합니다.")

        total_xp = 0
        city_count = 0
        all_logs = []

        while total_xp < target_xp_val:
            if city_count >= 10000:
                raise ValueError("계산 횟수 한도를 초과했습니다. 입력값을 줄여주세요.")
            if city_count % 100 == 0:
                await asyncio.sleep(0)
            user_attack_power = apply_attack_percent(current_troops, striker)
            best_level = find_max_level(user_attack_power)

            if best_level is None or best_level < 61:
                break

            city_wall = calculate_city_wall(best_level)
            xp_gain = city_wall * (multiplier / 100) * 1.282

            if not math.isfinite(xp_gain) or not math.isfinite(total_xp + xp_gain):
                raise ValueError("경험치 계산 범위를 벗어났습니다.")
            total_xp += xp_gain
            city_count += 1

            all_logs.append(
                f"**#{city_count}** | Lvl {best_level} | 병력: {format_power_unit(current_troops)} | XP: +{format_power_unit(xp_gain)}"
            )


            current_troops *= loss_rate_pct


        cmd_name = interaction.command.name
        color = 0xf86767 if cmd_name == "lb" else 0xffa500
        loss_label = "10%" if cmd_name == "lb" else "25%"

        embed = discord.Embed(title=f"🏰 /{cmd_name} 시뮬레이션 (병력 {loss_label} 감소)", color=color)
        embed.add_field(
            name="📥 시작 조건",
            value=f"병력: `{troops.upper()}` | 공격력: `{striker}%` | 목표XP: `{xp_required.upper()}`",
            inline=False
        )

        summary = (
            f"✅ **총 공격 횟수:** `{city_count}개` 성\n"
            f"📊 **최종 획득 경험치:** `{format_power_unit(total_xp)}`\n"
            f"📉 **최종 남은 병력:** `{format_power_unit(current_troops)}`"
        )
        if total_xp < target_xp_val:
            summary += "\n⚠️ 병력이 부족해 목표 경험치에 도달하지 못했습니다."
        embed.add_field(name="📊 최종 요약", value=summary, inline=False)

        await interaction.followup.send(embed=embed)


        if not all_logs:
            await interaction.channel.send("⚠️ 공격 가능한 성이 없습니다.")
            return

        header = f"📝 **/{cmd_name} 상세 빌드 로그 (전체 과정)**\n"
        current_chunk = header

        for line in all_logs:
            formatted_line = line + "\n"

            if len(current_chunk) + len(formatted_line) > 1900:
                await interaction.channel.send(current_chunk)
                current_chunk = ""
            current_chunk += formatted_line

        if current_chunk:
            await interaction.channel.send(current_chunk)

        log_command(f"/{cmd_name}", interaction.user, f"{troops}/{xp_required}", f"{city_count}개 전체출력", interaction.channel)

    except ValueError as e:
        await interaction.followup.send(f"❌ 입력값 오류: {e}")
    except Exception:
        logger.exception("시뮬레이션 처리 오류")
        await interaction.followup.send("❌ 처리 중 오류가 발생했습니다. 관리자에게 알려주세요.")


@tree.command(name="lb", description="병력 10% 감소를 반영한 전체 빌드를 출력합니다.", guild=GUILD)
@app_commands.describe(troops="병력", striker="공격력%", multiplier="배율%", xp_required="목표XP")
async def lb_command(interaction: discord.Interaction, troops: str, striker: float, multiplier: float, xp_required: str):
    await run_simulation(interaction, troops, striker, multiplier, xp_required, 0.9)

@tree.command(name="lbl", description="병력 25% 감소를 반영한 전체 빌드를 출력합니다.", guild=GUILD)
@app_commands.describe(troops="병력", striker="공격력%", multiplier="배율%", xp_required="목표XP")
async def lbl_command(interaction: discord.Interaction, troops: str, striker: float, multiplier: float, xp_required: str):
    await run_simulation(interaction, troops, striker, multiplier, xp_required, 0.75)
# =========================
#  팀/닉네임 외부 파일 경로/캐시 + 엑셀 I/O 유틸 (수정본: 원자저장/락/중복방지/검색강화)
# =========================


# =========================
#  TEAMS (CSV 1파일 + "시트(팀) 구분" 보존) + ?팀 명령어 (통째로 교체용)
#  - Data/teams.csv 사용
#  - CSV에 team,nickname 형태로 저장해서 "팀별 구분" 유지 (합쳐지는게 정상 아님)
#  - 파일 없으면 자동 생성
#  - 동시 접근 락 적용
# =========================


DATA_DIR = LOG_DIR.parent / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TEAM_FILE = DATA_DIR / "teams.csv"

_TEAM_LOCK = asyncio.Lock()
_team_file_lock = threading.RLock()
_team_cache: dict[str, object] = {"mtime": None, "data": {}}

# ----- 정규화/키 -----
def _normalize_team_name(name) -> str:
    return "" if name is None else str(name).strip()

def _normalize_nick(nick) -> str:
    return "" if nick is None else str(nick).strip()

def _canonical_team_key(name) -> str:
    s = "" if name is None else str(name).strip().lower()
    return re.sub(r"[\[\]\s]+", "", s)

# =========================
#  CSV 파일 보장/안전 읽기
# =========================
def _ensure_teams_csv_exists() -> None:
    TEAM_FILE.parent.mkdir(parents=True, exist_ok=True)
    if TEAM_FILE.exists() and TEAM_FILE.stat().st_size > 0:
        return
    with open(TEAM_FILE, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["team", "nickname"])

def _read_team_csv_rows(path: Path) -> list[list[str]]:
    _ensure_teams_csv_exists()

    raw = path.read_bytes()


    if raw[:2] == b"PK":
        raise ValueError("teams.csv가 CSV가 아니라 XLSX(압축파일)처럼 보입니다. 엑셀에서 'CSV UTF-8'로 다시 저장해주세요.")


    nul_ratio = (raw.count(b"\x00") / max(1, len(raw)))
    likely_utf16 = nul_ratio > 0.05

    candidates: list[str] = []
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        candidates = ["utf-16"]
    elif raw.startswith(b"\xef\xbb\xbf"):
        candidates = ["utf-8-sig"]
    else:
        if likely_utf16:
            candidates += ["utf-16-le", "utf-16-be", "utf-16"]
        candidates += ["utf-8-sig", "cp949", "euc-kr", "latin-1"]

    last_err: Exception | None = None
    best_rows: list[list[str]] = []
    best_score = -1

    def _parse(text: str) -> tuple[list[list[str]], str]:
        text = text.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
        sample = text[:4096]

        delim = ","
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
            delim = dialect.delimiter
        except Exception:
            if sample.count("\t") > sample.count(",") and sample.count("\t") > 0:
                delim = "\t"
            elif sample.count(";") > sample.count(",") and sample.count(";") > 0:
                delim = ";"
            else:
                delim = ","

        rows = list(csv.reader(io.StringIO(text), delimiter=delim))
        fixed: list[list[str]] = []
        for r in rows:
            if not r:
                continue

            if len(r) >= 2:
                fixed.append([r[0], r[1]])
                continue

            cell = (r[0] or "").strip()
            if not cell:
                continue


            if "\t" in cell:
                parts = cell.split("\t", 1)
            elif ";" in cell:
                parts = cell.split(";", 1)
            elif "," in cell:
                parts = cell.split(",", 1)
            else:
                parts = re.split(r"\s{2,}", cell, maxsplit=1)

            if len(parts) >= 2:
                fixed.append([parts[0], parts[1]])
            else:
                fixed.append([cell, ""])
        return fixed, delim

    for enc in candidates:
        try:
            text = raw.decode(enc)
            rows, _ = _parse(text)


            score = 0
            start_idx = 0
            if rows and len(rows[0]) >= 2:
                h0 = (rows[0][0] or "").strip().lower()
                h1 = (rows[0][1] or "").strip().lower()
                if h0 in ("team", "팀") and h1 in ("nickname", "nick", "닉네임"):
                    start_idx = 1

            for r in rows[start_idx:start_idx + 120]:
                if len(r) < 2:
                    continue
                team = _normalize_team_name(r[0])
                if team:
                    score += 1

            if rows and score > best_score:
                best_score = score
                best_rows = rows

            if score >= 10:
                break

        except Exception as e:
            last_err = e
            continue

    if not best_rows:
        raise last_err if last_err else ValueError("teams.csv 디코딩/파싱 실패")

    return best_rows

# =========================
#  CSV 로드/세이브 (팀 구분 유지)
# =========================
def _load_teams_from_csv() -> dict[str, set[str]]:
    data: dict[str, set[str]] = {}
    _ensure_teams_csv_exists()

    rows = _read_team_csv_rows(TEAM_FILE)
    if not rows:
        return data

    # 헤더 감지
    start_idx = 0
    if len(rows[0]) >= 2:
        h0 = (rows[0][0] or "").strip().lower()
        h1 = (rows[0][1] or "").strip().lower()
        if h0 in ("team", "팀") and h1 in ("nickname", "nick", "닉네임"):
            start_idx = 1


    key_to_real: dict[str, str] = {}

    for row in rows[start_idx:]:
        if not row or len(row) < 1:
            continue

        tcell = row[0] if len(row) >= 1 else ""
        ncell = row[1] if len(row) >= 2 else ""

        team_raw = _normalize_team_name(tcell)
        nick = _normalize_nick(ncell)

        if not team_raw:
            continue

        key = _canonical_team_key(team_raw)
        real_team = key_to_real.get(key)
        if real_team is None:
            key_to_real[key] = team_raw
            real_team = team_raw

        bucket = data.setdefault(real_team, set())
        if nick:
            bucket.add(nick)
        else:
            data.setdefault(real_team, set())

    return data

def _save_teams_to_csv(data: dict[str, set[str]]) -> None:
    TEAM_FILE.parent.mkdir(parents=True, exist_ok=True)

    tmp_dir = str(TEAM_FILE.parent)
    fd, tmp_name = tempfile.mkstemp(prefix="teams_", suffix=".csv", dir=tmp_dir)
    os.close(fd)

    try:
        with open(tmp_name, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["team", "nickname"])

            for team in sorted(data.keys(), key=lambda x: x.lower()):
                members = sorted(list(data[team]), key=lambda x: x.lower())
                if members:
                    for nick in members:
                        w.writerow([team, nick])
                else:
                    w.writerow([team, ""])

        os.replace(tmp_name, TEAM_FILE)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except Exception:
            pass

# =========================
#  캐시
# =========================
def _get_teams(force_reload: bool = False) -> dict[str, set[str]]:
    with _team_file_lock:
        return _get_teams_unlocked(force_reload)


def _get_teams_unlocked(force_reload: bool = False) -> dict[str, set[str]]:
    _ensure_teams_csv_exists()
    stat = TEAM_FILE.stat()
    mtime = (stat.st_mtime_ns, stat.st_size)

    cached_mtime = _team_cache["mtime"]
    cached_data = _team_cache["data"]

    if (not force_reload) and cached_mtime == mtime:
        return cached_data

    data = _load_teams_from_csv()
    _team_cache["mtime"] = mtime
    _team_cache["data"] = data
    return data

def _find_team_name_case_insensitive(data: dict[str, set[str]], name: str) -> str | None:
    key = _canonical_team_key(name)
    for t in data.keys():
        if _canonical_team_key(t) == key:
            return t
    return None

def _add_member(nick: str, team: str) -> tuple[bool, str]:
    nick, team = _normalize_nick(nick), _normalize_team_name(team)
    if not nick or not _canonical_team_key(team):
        return False, "닉네임과 팀명을 입력해주세요."
    with _team_file_lock:
        data = {t: set(members) for t, members in _get_teams().items()}
        real_team = _find_team_name_case_insensitive(data, team) or team
        members = data.setdefault(real_team, set())
        if any(member.casefold() == nick.casefold() for member in members):
            return False, f"[{real_team}] {nick}은 이미 등록되어 있습니다."
        members.add(nick)
        _save_teams_to_csv(data)
        _team_cache["mtime"] = None
    return True, f"✅ [{real_team}] {nick} 추가 완료"


def _delete_member(nickname: str, team: str | None = None) -> tuple[bool, str]:
    nickname = _normalize_nick(nickname)
    if not nickname:
        return False, "삭제할 닉네임을 입력해주세요."
    with _team_file_lock:
        data = {t: set(members) for t, members in _get_teams().items()}
        real_team = _find_team_name_case_insensitive(data, team) if team else None
        if team and real_team is None:
            return False, f"[{team}] 팀을 찾지 못했습니다."
        targets = [real_team] if real_team is not None else list(data)
        removed = 0
        for target in targets:
            matches = {nick for nick in data[target] if nick.casefold() == nickname.casefold()}
            data[target].difference_update(matches)
            removed += len(matches)
        if not removed:
            return False, f"{nickname}을 찾지 못했습니다."
        _save_teams_to_csv(data)
        _team_cache["mtime"] = None
    return True, f"✅ {nickname} 삭제 완료 ({removed}건)"


@tree.command(
    name=app_commands.locale_str("_add_member", **{"ko": "팀원추가"}),
    description=app_commands.locale_str(
        "Add a member to team",
        **{"ko": "팀에 팀원을 추가합니다."}
    ),
    guild=GUILD
)
@app_commands.describe(
    nickname=app_commands.locale_str("Nickname", **{"ko": "닉네임"}),
    team=app_commands.locale_str("Team name", **{"ko": "팀명"})
)
@app_commands.rename(nickname="닉네임", team="팀명")
async def add_member_slash(interaction: discord.Interaction, nickname: str, team: str):
    if not _can_manage_teams(interaction.user):
        await interaction.response.send_message("🚫 권한이 없습니다!", ephemeral=True)
        return


    try:
        await interaction.response.send_message("🔎 처리 중...", ephemeral=True)
    except discord.InteractionResponded:
        pass

    try:
        ok, msg = await add_member_async(nickname, team)
        await interaction.followup.send(msg)
        log_command("/팀원추가", interaction.user, f"/팀원추가 {nickname} {team}", msg, interaction.channel)
    except Exception as e:
        out = f"❌ 팀원추가 처리 중 오류: {e}"
        await interaction.followup.send(out)
        log_command("/팀원추가", interaction.user, f"/팀원추가 {nickname} {team}", out, interaction.channel)


@tree.command(
    name=app_commands.locale_str("_delete_team", **{"ko": "팀원삭제"}),
    description=app_commands.locale_str(
        "Remove a member from a team (team optional)",
        **{"ko": "팀원 삭제 (팀명 생략 시 모든 팀에서 닉네임 삭제)"}
    ),
    guild=GUILD
)
@app_commands.describe(
    nickname=app_commands.locale_str("Nickname", **{"ko": "닉네임"}),
    team=app_commands.locale_str("Team name (optional)", **{"ko": "팀명 (선택)"})
)
@app_commands.rename(nickname="닉네임", team="팀명")
async def delete_member_slash(interaction: discord.Interaction, nickname: str, team: Optional[str] = None):
    if not _can_manage_teams(interaction.user):
        await interaction.response.send_message("🚫 권한이 없습니다!", ephemeral=True)
        return

    try:
        await interaction.response.send_message("🔎 처리 중...", ephemeral=True)
    except discord.InteractionResponded:
        pass

    try:
        ok, msg = await delete_member_async(nickname, team)
        await interaction.followup.send(msg)
        input_literal = f"/팀원삭제 {nickname} {team}".strip() if team else f"/팀원삭제 {nickname}"
        log_command("/팀원삭제", interaction.user, input_literal, msg, interaction.channel)
    except Exception as e:
        out = f"❌ 팀원삭제 처리 중 오류: {e}"
        await interaction.followup.send(out)
        input_literal = f"/팀원삭제 {nickname} {team}".strip() if team else f"/팀원삭제 {nickname}"
        log_command("/팀원삭제", interaction.user, input_literal, out, interaction.channel)

# ----- 비동기(락 적용) 래퍼 -----
async def add_member_async(nick: str, team: str) -> tuple[bool, str]:
    async with _TEAM_LOCK:
        return await asyncio.to_thread(_add_member, nick, team)

async def delete_member_async(nickname: str, team: str | None = None) -> tuple[bool, str]:
    async with _TEAM_LOCK:
        return await asyncio.to_thread(_delete_member, nickname, team)

# =========================
#  ?팀 계열 텍스트 명령 처리기
#  - on_message에서: handled = await handle_team_question_command(message)
# =========================
def _lcs_substr_len(a: str, b: str) -> int:
    a = a.lower()
    b = b.lower()
    if not a or not b:
        return 0
    prev = [0] * (len(b) + 1)
    best = 0
    for i in range(1, len(a) + 1):
        cur = [0]
        ai = a[i - 1]
        for j in range(1, len(b) + 1):
            if ai == b[j - 1]:
                v = prev[j - 1] + 1
                cur.append(v)
                if v > best:
                    best = v
            else:
                cur.append(0)
        prev = cur
    return best

def _rank_nicknames(data, query):
    q = query.strip().lower()
    hits = []
    for team, members in data.items():
        for nick in members:
            c = nick.strip().lower()
            if q == c:
                hits.append((1.0, team, nick))
                continue
            lcs = _lcs_substr_len(q, c)
            if lcs >= 3:
                hits.append((lcs / max(len(q), len(c)), team, nick))
    hits.sort(key=lambda x: (-x[0], x[1].lower(), x[2].lower()))
    return [(team, nick) for _, team, nick in hits]


def _format_search_output(ranked_hits: list[tuple[str, str]]) -> str:
    if not ranked_hits:
        return "🔍검색결과 : 🚫찾지못함🚫"
    lines = ["🔍 검색결과 🔍 :"]
    for team, nick in ranked_hits:
        lines.append(f"[{team} 팀] {nick}")
    return "\n".join(lines)

def _format_team_members_output(team: str, members: list[str]) -> str:
    lines = ["🔍 검색결과 🔍 :", f"[{team} 팀에 대한 검색결과]"]
    if members:
        lines += [f"[{team}] {m}" for m in members]
    else:
        lines.append("(등록된 팀원이 없습니다)")
    return "\n".join(lines)

async def _run_nickname_search(message: "discord.Message", query_raw: str) -> bool:
    query = (query_raw or "").strip()
    if not query:
        out = "사용법: `?팀`, `?팀명 <팀>`, `?닉네임 <검색어...>` 또는 `?검색어`"
        await message.channel.send(out, delete_after=15)
        log_command("?", message.author, f"?{query_raw}", out, message.channel)
        return True

    async with _TEAM_LOCK:
        data = await asyncio.to_thread(_get_teams)

    qkey = _canonical_team_key(query)
    matched_team: str | None = None
    for team in data.keys():
        if _canonical_team_key(team) == qkey:
            matched_team = team
            break

    if matched_team is not None:
        members = sorted(list(data.get(matched_team, set())), key=str.lower)
        out = _format_team_members_output(matched_team, members)
        await _send_long_message(message.channel, out)
        log_command("?", message.author, f"?{query_raw}", out, message.channel)
        return True

    ranked = await asyncio.to_thread(_rank_nicknames, data, query)
    out = _format_search_output(ranked)

    await _send_long_message(message.channel, out)
    log_command("?", message.author, f"?{query_raw}", out, message.channel)
    return True

def _message_chunks(text: str, limit: int = 1900):
    while text:
        if len(text) <= limit:
            yield text
            return
        split_at = text.rfind("\n", 0, limit + 1)
        if split_at <= 0:
            split_at = limit
        yield text[:split_at]
        text = text[split_at:]
        if text.startswith("\n"):
            text = text[1:]


async def _send_long_message(channel, text, **kwargs):
    for chunk in _message_chunks(text):
        await channel.send(chunk, **kwargs)


async def handle_team_question_command(message: "discord.Message") -> bool:
    content = (message.content or "").strip()
    if not content.startswith("?"):
        return False

    raw_after_q = content[1:].strip()
    parts = raw_after_q.split()
    if not parts:
        out = "❔ 사용법: `?팀` | `?팀명 <팀>` | `?닉네임 <검색어...>` | `?검색어`"
        await message.channel.send(out, delete_after=20)
        log_command("?", message.author, content, "사용법 안내", message.channel)
        return True

    cmd = parts[0]
    args = parts[1:]

    if cmd in ["팀", "teams", "list"]:
        async with _TEAM_LOCK:
            data = await asyncio.to_thread(_get_teams)
        if not data:
            out = "아직 등록된 팀이 없습니다."
            await _send_long_message(message.channel, out)
            log_command("?", message.author, content, out, message.channel)
            return True

        lines = [f"[{team} 팀] 인원 - {len(data[team])}명"
                 for team in sorted(data.keys(), key=str.lower)]
        out = "\n".join(lines)
        await _send_long_message(message.channel, out)
        log_command("?", message.author, content, out, message.channel)
        return True

    if cmd in ["팀명", "team", "members"]:
        if not args:
            out = "사용법: `?팀명 <팀이름>`"
            await message.channel.send(out, delete_after=15)
            log_command("?", message.author, content, out, message.channel)
            return True

        team_input = " ".join(args).strip()
        async with _TEAM_LOCK:
            data = await asyncio.to_thread(_get_teams)

        real_team = _find_team_name_case_insensitive(data, team_input)
        if real_team is None:
            out = f"[{team_input} 팀 리스트]\n(등록된 팀이 없습니다)"
            await _send_long_message(message.channel, out)
            log_command("?", message.author, content, out, message.channel)
            return True

        members = sorted(list(data.get(real_team, set())), key=str.lower)
        if not members:
            out = f"[{real_team} 팀 리스트]\n(등록된 팀원이 없습니다)"
            await _send_long_message(message.channel, out)
            log_command("?", message.author, content, out, message.channel)
            return True

        lines = [f"[{real_team} 팀 리스트]"] + [f"[{real_team}] {nick}" for nick in members]
        out = "\n".join(lines)
        await _send_long_message(message.channel, out)
        log_command("?", message.author, content, out, message.channel)
        return True

    if cmd in ["닉네임", "nick", "검색", "find"]:
        if not args:
            out = "사용법: `?닉네임 <검색어...>` (예: `?닉네임 KOR Raven`)"
            await message.channel.send(out, delete_after=15)
            log_command("?", message.author, content, out, message.channel)
            return True
        query_raw = " ".join(args)
        return await _run_nickname_search(message, query_raw)

    return await _run_nickname_search(message, raw_after_q)


# =========================
# /보석 명령어
# =========================


# ================== CONSTANT ==================
DATE_RX = re.compile(r'(\d{4})\D(\d{1,2})\D(\d{1,2})')
ALLOWED_JEWELS = ["공격", "이속", "청소부", "경계", "모집", "인양", "골드", "방어", "대담", "용기"]

JEWEL_FILE = DATA_DIR / "jewel.csv"

# ================== UTIL ==================
def _today_kst_str() -> str:
    return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")


def _parse_ymd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _canon_date_str_ymd(dt: Any) -> Optional[str]:
    if dt is None:
        return None

    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    if isinstance(dt, date):
        return dt.strftime("%Y-%m-%d")

    s = str(dt).strip()
    if not s:
        return None

    s = (
        s.replace("\ufeff", "")
         .replace("\x00", "")
         .replace("\u200b", "")
         .replace("\u200c", "")
         .replace("\u200d", "")
         .replace("년", "-")
         .replace("월", "-")
         .replace("일", "")
         .replace("/", "-")
         .replace(".", "-")
    )

    m = DATE_RX.search(s)
    if not m:
        return None

    y, mth, d = m.groups()
    try:
        return date(int(y), int(mth), int(d)).isoformat()
    except Exception:
        return None


def _ensure_jewel_csv_exists() -> None:
    JEWEL_FILE.parent.mkdir(parents=True, exist_ok=True)
    if JEWEL_FILE.exists():
        return

    with open(JEWEL_FILE, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "type"])


def _read_jewel_csv_rows(path: Path) -> list[list[str]]:
    _ensure_jewel_csv_exists()
    raw = path.read_bytes()

    if raw[:2] == b"PK":
        raise ValueError("jewel.csv가 CSV가 아니라 XLSX입니다. CSV UTF-8로 다시 저장하세요.")

    nul_ratio = raw.count(b"\x00") / max(1, len(raw))
    likely_utf16 = nul_ratio > 0.05

    encodings = []
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        encodings = ["utf-16"]
    elif raw.startswith(b"\xef\xbb\xbf"):
        encodings = ["utf-8-sig"]
    else:
        if likely_utf16:
            encodings += ["utf-16-le", "utf-16-be", "utf-16"]
        encodings += ["utf-8-sig", "cp949", "euc-kr", "latin-1"]

    def _parse(text: str) -> list[list[str]]:
        text = text.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
        sample = text[:4096]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
            delim = dialect.delimiter
        except Exception:
            delim = "\t" if sample.count("\t") > sample.count(",") else ","

        rows = list(csv.reader(io.StringIO(text), delimiter=delim))
        fixed = []

        for r in rows:
            if not r:
                continue
            if len(r) >= 2:
                fixed.append(r)
                continue

            cell = (r[0] or "").strip()
            if not cell:
                continue

            if "\t" in cell:
                parts = cell.split("\t", 1)
            elif ";" in cell:
                parts = cell.split(";", 1)
            elif "," in cell:
                parts = cell.split(",", 1)
            else:
                parts = re.split(r"\s{2,}", cell, maxsplit=1)

            fixed.append(parts if len(parts) >= 2 else [cell])

        return fixed

    last_err = None
    for enc in encodings:
        try:
            rows = _parse(raw.decode(enc))
            if rows:
                return rows
        except Exception as e:
            last_err = e

    raise last_err if last_err else ValueError("CSV 디코딩 실패")


def _lookup_jewel_value_by_date(target_ymd: str) -> Tuple[bool, str]:
    rows = _read_jewel_csv_rows(JEWEL_FILE)
    if not rows:
        return False, "CSV가 비어 있습니다."

    start_idx = 1 if rows[0][0].lower() in ("date", "날짜") else 0

    for r in rows[start_idx:]:
        if len(r) < 2:
            continue
        if _canon_date_str_ymd(r[0]) == target_ymd:
            return True, r[1].strip()

    return False, f"[{target_ymd}] 날짜를 찾지 못했습니다."


def _find_next_n_dates_by_type(jewel_type: str, start_ymd: str, n: int = 4):
    start_d = _parse_ymd(start_ymd)
    if not start_d:
        return False, "시작 날짜 오류"

    rows = _read_jewel_csv_rows(JEWEL_FILE)
    start_idx = 1 if rows and rows[0][0].lower() in ("date", "날짜") else 0

    out = []
    for r in rows[start_idx:]:
        if len(r) < 2:
            continue
        if r[1].strip() != jewel_type:
            continue

        ymd = _canon_date_str_ymd(r[0])
        if not ymd:
            continue

        d = _parse_ymd(ymd)
        if d and d >= start_d:
            out.append(ymd)

    uniq = []
    for d in sorted(out):
        if d not in uniq:
            uniq.append(d)
        if len(uniq) >= n:
            break

    return (True, uniq) if uniq else (False, f"'{jewel_type}' 일정 없음")

# ================== COMMAND ==================
@tree.command(
    name=app_commands.locale_str("jewel", **{"ko": "보석"}),
    description=app_commands.locale_str(
        "Lookup by date or type",
        **{"ko": "날짜 또는 종류 조회"}
    ),
    guild=GUILD
)
@app_commands.describe(
    arg=app_commands.locale_str(
        "Date or type",
        **{"ko": "YYYY-MM-DD 또는 종류"}
    )
)
@app_commands.rename(arg="입력값")
async def jewel_lookup(interaction: discord.Interaction, arg: Optional[str] = None):
    await interaction.response.send_message("🔎 조회 중...", ephemeral=True)

    if arg is None:
        target = _today_kst_str()
        ok, res = await asyncio.to_thread(_lookup_jewel_value_by_date, target)
        await interaction.followup.send(f"📅 {target} → {res}" if ok else f"❌ {res}")
        return

    arg = arg.strip()

    if _parse_ymd(arg):
        ok, res = await asyncio.to_thread(_lookup_jewel_value_by_date, arg)
        await interaction.followup.send(f"📅 {arg} → {res}" if ok else f"❌ {res}")
        return

    if arg in ALLOWED_JEWELS:
        ok, res = await asyncio.to_thread(_find_next_n_dates_by_type, arg, _today_kst_str(), 4)
        if ok:
            await interaction.followup.send("\n".join(f"📅 [ {d} ] → {arg} 💎" for d in res))
        else:
            await interaction.followup.send(f"❌ {res}")
        return

    await interaction.followup.send(
        "❌ 입력값 오류\n- 날짜: YYYY-MM-DD\n- 종류: " + ", ".join(ALLOWED_JEWELS)
    )


# =========================
# /명령어 — 도움말 (3분 후 자동삭제)
# =========================
@tree.command(
    name=app_commands.locale_str("help", **{"ko": "명령어"}),
    description=app_commands.locale_str(
        "Show all bot commands and examples",
        **{"ko": "봇 명령어 목록과 예시를 보여줍니다."}
    ),
    guild=GUILD
)
async def show_help(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)

    help_text = (
        "📖 **밀리언로드 봇 명령어 안내**\n"
        "*(이 메시지는 3분 뒤 자동으로 삭제됩니다)*\n"
        "\n"
        "### ⚔️ 공격력 계산\n"
        "- `!공격력 <수치>` (예: `!공격력 42.2T`)\n"
        "- 자유형: `!<수치>[단위] [오프셋]` (예: `!42.2T -1`)\n"
        "- 잘못된 입력(단위 누락 등): `❌ 입력값이 잘못되었습니다 입력값을 확인해주세요 ❌`\n"
        "\n"
        "**예시 입력**: `!42.2T`\n"
        "**예시 출력**: `[ ⚔️ 최종 목표 레벨 : 149 ] [ 🏰 성 방벽 : 36.55T ] @빌더 @입력자`\n"
        "\n"
        "### 🔎 팀/닉네임 조회 (`?` 계열)\n"
        "- `?팀` : 등록된 팀 목록 + 인원수\n"
        "- `?팀명 <팀>` : 해당 팀의 팀원 전체\n"
        "- `?닉네임 <검색어>` 또는 `?<검색어>` : 닉네임 유사 검색(최장공통부분문자열 기반, 완전일치 우선)\n"
        "\n"
        "**예시**\n"
        "- 입력: `?팀`\n"
        "  출력: `[KOR 팀] 인원 - 10명` …\n"
        "- 입력: `?팀명 KOR`\n"
        "  출력:\n"
        "  `[KOR 팀 리스트]`\\n`[KOR] Player1`\\n`[KOR] Player2`\\n`[KOR] Player3`\n"
        "- 입력: `?Play`\n"
        "  출력:\n"
        "  `🔍 검색결과 🔍 :`\\n`[KOR 팀] Play1`\\n`[MD 팀] Play_31` …\n"
        "\n"
        "### 💎 보석 일정 /(`/보석`)\n"
        "- 날짜 모드: `/보석 2025-10-29` \n"
        "- 종류 모드: `/보석 공격` (또는 `이속/청소부/경계/모집/인양/골드/방어/대담/용기`)\n"
        "  → **오늘 이후** 해당 종류가 등장하는 날짜 **4회** 출력\n"
        "\n"
        "**예시 (종류 모드)**\n"
        "```\n"
        "📅 [ 2025-10-29 ] → 이속 💎\n"
        "📅 [ 2025-11-08 ] → 이속 💎\n"
        "📅 [ 2025-11-18 ] → 이속 💎\n"
        "📅 [ 2025-11-28 ] → 이속 💎\n"
        "```\n"
        "\n"
        "### 👥 팀 관리(엑셀 연동)\n"
        "- `/팀원추가 \"닉네임\" \"소속팀\"`  (예: `/팀원추가 \"KOR name\" \"KOR\"`)\n"
        "- `/팀원삭제 \"닉네임\"` (팀명 생략 시 모든 팀에서 해당 닉네임 삭제)\n"
        "\n"
        "\n"
        "# 🔧 관리자 전용\n"
        "### 🧹 채팅 청소\n"
        "- `/ㅊㅊ` 또는 `/cc` : 현재 채널 메시지 삭제 (지정 채널/권한 전용)\n"
    )

    await _send_long_message(interaction.channel, help_text, delete_after=180)
    await interaction.edit_original_response(content="📖 도움말을 표시했습니다.")

    log_command("/명령어", interaction.user, "/명령어", "도움말 표시(3분 후 자동 삭제)", interaction.channel)


# =========================
# /purge_global 삭제한 명령어 기록 지우기
# =========================
@tree.command(name="purge_global", description="(관리자) 글로벌 슬래시 명령 전체 삭제", guild=GUILD)
async def purge_global(inter: discord.Interaction):
    if inter.user.id != Admin:
        await inter.response.send_message("🚫 권한 없음", ephemeral=True)
        return
    await inter.response.defer(ephemeral=True, thinking=True)
    tree.clear_commands(guild=None)
    await tree.sync(guild=None)
    await tree.sync(guild=GUILD)
    await inter.edit_original_response(content="🧹 글로벌 커맨드 정리 완료!")


# =========================
# /botstop (관리자 전용)
# =========================
@tree.command(name="botstop", description="봇을 종료합니다. (관리자 전용)",guild=GUILD)
async def stop_bot(interaction: discord.Interaction):
    if interaction.user.id != Admin:
        await interaction.response.send_message("🚫 권한이 없습니다!", ephemeral=True)
        log_command("/botstop", interaction.user, "/botstop", "권한 없음", interaction.channel)
        return

    try:
        await interaction.response.defer(ephemeral=True, thinking=True)
    except discord.InteractionResponded:
        pass

    result_text = "🛑 봇을 종료합니다..."
    try:
        await interaction.edit_original_response(content=result_text)
    except Exception:
        pass

    log_command("/botstop", interaction.user, "/botstop", result_text, interaction.channel)

    await bot.close()

# =========================
# /sync (관리자 전용)
# =========================
@tree.command(name="sync", description="슬래시 명령어를 이 서버에 다시 동기화합니다.", guild=discord.Object(id=GUILD_ID))
async def sync_cmd(inter: discord.Interaction):
    if inter.user.id != Admin:
        await inter.response.send_message("🚫 권한이 없습니다!", ephemeral=True)
        log_command("/sync", inter.user, "/sync", "권한 없음", inter.channel)
        return

    try:
        await inter.response.defer(ephemeral=True, thinking=True)
    except discord.InteractionResponded:
        pass

    cmds = await tree.sync(guild=discord.Object(id=GUILD_ID))
    names = ", ".join(c.name for c in cmds) if cmds else "(none)"
    result_text = f"✅ 동기화 완료: {len(cmds)}개 → {names}"

    try:
        await inter.edit_original_response(content=result_text)
    except Exception:
        pass

    log_command("/sync", inter.user, "/sync", result_text, inter.channel)

# 초기화는 setup_hook에서 한 번만 실행합니다. on_ready는 재접속에도 호출됩니다.
@bot.event
async def on_ready():
    logger.info("봇 연결 완료: %s (%s)", bot.user, bot.user.id)


@bot.event
async def on_disconnect():
    logger.warning("Discord 연결 끊김: 자동 재접속 대기")


@bot.event
async def on_resumed():
    logger.info("Discord 세션 재개 완료")


@bot.event
async def on_error(event, *args, **kwargs):
    logger.exception("Discord 이벤트 처리 오류: %s", event)


@tree.error
async def on_app_command_error(interaction, error):
    original = getattr(error, "original", error)
    logger.error("슬래시 명령 오류: %s", getattr(interaction.command, "name", None),
                 exc_info=(type(original), original, original.__traceback__))
    try:
        message = "❌ 명령 처리 중 오류가 발생했습니다. 관리자에게 알려주세요."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        logger.exception("오류 안내 메시지 전송 실패")


def _configure_logging():
    runtime_handler = RotatingFileHandler(
        LOG_DIR / "runtime.log", maxBytes=5 * 1024 * 1024,
        backupCount=5, encoding="utf-8",
    )
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    for handler in (runtime_handler, console_handler):
        handler.setFormatter(formatter)
    log_queue = queue.Queue(maxsize=1000)
    queue_handler = QueueHandler(log_queue)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(queue_handler)
    listener = QueueListener(log_queue, runtime_handler, console_handler)
    listener.start()
    return listener, queue_handler, runtime_handler, console_handler


def _load_token() -> str:
    token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    token_file = BASE_DIR / "bot_token.txt"
    if not token and token_file.exists():
        token = token_file.read_text(encoding="utf-8-sig").strip()
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN 환경변수 또는 bot_token.txt에 봇 토큰을 설정해주세요.")
    return token


def main():
    listener, queue_handler, runtime_handler, console_handler = _configure_logging()
    try:
        token = _load_token()
        logger.info("봇 시작: Python %s / discord.py %s", sys.version.split()[0], discord.__version__)
        bot.run(token, reconnect=True, log_handler=None)
    except Exception:
        logger.exception("봇 실행 중 치명적 오류로 종료")
        return 1
    finally:
        logger.info("봇 프로세스 종료")
        logging.getLogger().removeHandler(queue_handler)
        # QueueListener.stop()의 종료 표시가 들어갈 공간을 확보합니다.
        listener.queue.join()
        listener.stop()
        runtime_handler.close()
        console_handler.close()
        _command_log_handler.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
