# MillionLords_bot

[한국어](#한국어) · [English](#english)

## 한국어

MillionLords 길드를 위한 Discord 봇입니다. 공격 가능한 도시 레벨 계산, 병력 감소를 반영한 레벨 빌드 시뮬레이션, 팀·닉네임 조회와 관리, 보석 일정 조회를 지원합니다.

### 주요 기능

- `!공격력 <병력> [공격력%] [오프셋]` 및 `!42.2T -1` 같은 자유형 공격력 입력
- `/lb`, `/lbl`: 병력 10% 또는 25% 감소를 반영한 전체 빌드 계산
- `?팀`, `?팀명 <팀>`, `?닉네임 <검색어>`: CSV 기반 팀·닉네임 조회
- `/팀원추가`, `/팀원삭제`: 권한을 가진 사용자의 팀원 관리
- `/보석`: 날짜 또는 종류로 보석 일정 조회
- 채널 청소, 도움말, 관리자 동기화·정상 종료 명령

한국어 클라이언트에는 일부 슬래시 명령 이름이 번역되어 표시됩니다.

### 설치와 실행

Python 3.11 이상이 필요합니다. Python 3.14.2 / discord.py 2.6.4에서 검증했습니다.

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

1. `MillionLords_bot.py` 상단의 서버·사용자·역할·채널 ID를 자신의 Discord 서버에 맞게 설정합니다. 저장소 코드는 제작자의 기존 설정을 유지합니다.
2. Discord Developer Portal에서 Server Members Intent와 Message Content Intent를 활성화하고 봇에 필요한 채널·역할·메시지 관리 권한을 부여합니다.
3. `bot_token.example.txt`를 `bot_token.txt`로 복사한 뒤 자신의 봇 토큰을 입력합니다. 또는 `DISCORD_BOT_TOKEN` 환경변수를 사용합니다. 환경변수가 우선합니다.
4. `Data/teams.example.csv`, `Data/jewel.example.csv`를 각각 `teams.csv`, `jewel.csv`로 복사하고 실제 팀원·보석 일정을 입력합니다. 예제는 실제 사용자 데이터가 아닙니다.
5. 다음 명령으로 실행합니다.

```powershell
.venv\Scripts\python.exe MillionLords_bot.py
```

CSV와 로그는 봇 파일이 있는 폴더를 기준으로 저장됩니다. 토큰, 실제 팀·보석 데이터, 로그, 원본 백업은 Git에서 제외됩니다. 코드에 들어 있던 토큰은 재발급한 값을 사용하는 것이 좋습니다.

### 이번 안정성 수정

- 실패 로그의 무제한 적재와 로그마다 새 Task 생성: 파일·채널 작업을 각각 하나로 유지하며 각 큐를 500건, 항목 길이를 12,000자로 제한했습니다.
- 재접속마다 생성되던 주기적 GC 작업을 제거하고 초기화를 `setup_hook`으로 이동했습니다.
- 파일 입출력과 닉네임 검색을 별도 스레드에서 처리하도록 변경했습니다.
- 무한대·NaN·계산 범위 초과 입력을 거르고 시뮬레이션 반복 횟수에 상한을 두었습니다.
- 누락된 팀원 추가·삭제 함수, 사용자 ID와 역할 ID를 혼동한 권한 검사, 중복 CSV 함수 이름을 수정했습니다.
- 팀 데이터 저장 실패 시 캐시가 잘못 바뀌지 않도록 수정하고 동시 갱신을 보호했습니다.
- 긴 메시지와 도움말의 API 인자 오류, 짧은 닉네임 정확 일치 검색, 한국 날짜 처리를 수정했습니다.
- 목표 XP 미달 안내와 정상 종료 처리, 연결·오류 로그를 추가했습니다.
- 사용하지 않는 openpyxl과 중복 import를 제거했습니다.

### 검증과 로그

```powershell
.venv\Scripts\python.exe -X utf8 test_bot.py
```

오프라인 테스트 16개와 정적 검사를 통과했습니다. 테스트는 Discord에 접속하지 않으며 임시 데이터로 로그 대기열, 재접속, 파일 실패, 동시 팀 관리, 계산과 메시지 처리를 검증합니다.

`Log/runtime.log`에 연결·시작·종료·예외를 기록합니다. `runtime.log`와 `bot_log.txt`는 각각 5 MiB 단위, 백업 5개로 순환합니다. 월별 명령 CSV는 보존됩니다. 큐가 가득 차면 가장 오래된 대기 로그를 버리고 경고를 기록합니다.

과거 자동 종료의 원인은 종료 당시 예외 로그가 없어 확정하지 못했습니다. 실제 Discord에서 장시간 운영을 재현한 결과는 아닙니다. PC 절전·재부팅·터미널 종료를 복구하는 서비스 기능은 포함하지 않습니다.

## English

A Discord bot for a MillionLords guild, featuring attack-level calculations, troop-loss build simulations, team and nickname management, and jewel schedule lookup.

### Features

- Attack calculations through `!공격력` or shorthand such as `!42.2T -1`
- `/lb` and `/lbl` build simulations with 10% or 25% troop loss
- CSV-backed team lists, member lookup, fuzzy nickname search, and authorized member management
- Jewel lookup by date or type
- Channel cleanup, help, command synchronization, and graceful administrator shutdown

Some slash command names are localized for Korean Discord clients.

### Setup

Requires Python 3.11 or later. Verified with Python 3.14.2 and discord.py 2.6.4.

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

1. Configure the guild, user, role, and channel IDs near the top of `MillionLords_bot.py` for your server. The source retains the author's existing configuration.
2. Enable Server Members Intent and Message Content Intent in the Discord Developer Portal. Grant the permissions required for channels, roles, and message management.
3. Copy `bot_token.example.txt` to `bot_token.txt` and enter your bot token, or set `DISCORD_BOT_TOKEN`. The environment variable takes precedence.
4. Copy `Data/teams.example.csv` and `Data/jewel.example.csv` to `teams.csv` and `jewel.csv`, then supply your own data. The examples contain no real user data.
5. Run the bot:

```powershell
.venv\Scripts\python.exe MillionLords_bot.py
```

Data and logs are stored relative to the bot file. Tokens, live CSV data, logs, and original backups are excluded from Git. Use a rotated token if your previous token was embedded in source code.

### Stability and bug fixes

- Replaced unbounded failed-log buffering and per-log Task creation with one file worker and one channel worker; each queue holds at most 500 entries, with a 12,000-character entry limit.
- Removed repeatedly created manual-GC tasks and moved one-time initialization into `setup_hook`.
- Moved file operations and nickname scoring off the Discord event loop.
- Rejected non-finite and overflowing values and bounded simulation iterations.
- Implemented missing team mutation helpers, corrected role checks, and separated conflicting CSV parser names.
- Protected concurrent updates and kept the cache unchanged when saving fails.
- Fixed long-message splitting, unsupported help API arguments, short exact nickname searches, and Korean date handling.
- Added XP shortfall reporting, graceful shutdown, and connection/error logs.
- Removed unused openpyxl dependencies and duplicate imports.

### Validation and logs

```powershell
.venv\Scripts\python.exe -X utf8 test_bot.py
```

All 16 offline tests and static checks passed. Tests use temporary data and mocked Discord APIs to cover log bounds, reconnect handling, file failures, concurrent team updates, calculations, and message handling.

Connection and lifecycle events and exceptions are recorded in `Log/runtime.log`. Both `runtime.log` and `bot_log.txt` rotate at 5 MiB with five backups; monthly command CSV files are retained. When a queue fills, its oldest pending entry is dropped and a warning is recorded.

The historical shutdown cause could not be confirmed without exception logs from the incident. Long-running live Discord operation has not been reproduced. This project does not include a service that recovers from PC sleep, reboot, or closing the terminal.
