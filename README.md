# v2 매매 프로그램

제안사항이 반영된 버전입니다. **v2 폴더만으로 독립 실행** 가능합니다 (v1 폴더 불필요).

## 반영된 제안

1. **설정 외부화** — `config.yaml`에서 주기, 마진%, 청산 조건 on/off, 리스크 한도 등 설정
2. **전략 레이어 분리** — `strategy.py`: 진입/청산 규칙만 순수 함수 (MT5/DB 무의존), 백테스트·실거래 공통
3. **일일/주간 리스크 상한** — `risk_limits.py`: 일일 최대 손실%, 일일 최대 진입 횟수, 심볼당 최대 포지션
4. **구조화 로그** — `structured_logger.py`: JSON 한 줄 로그, 청산 사유·진입 조건 기록
5. **실행 재시도·알림** — `execution.py`: MT5/주문 실패 시 재시도, 실패 시 텔레그램 알림
6. **성과 리포트** — `performance_report.py`: 진입조건별·TF별·세션별 승률·수익·거래 수
7. **포지션/예약 모니터** — config·리스크·로그를 쓰는 래퍼 (`position_monitor.py`, `reservation_monitor.py`)

## 디렉터리 구조 (v2 독립)

v2 폴더 안에 실행에 필요한 모든 모듈이 포함되어 있습니다. v1을 참조하지 않습니다.

```
v2/
  config.yaml
  strategy.py
  risk_limits.py
  structured_logger.py
  execution.py
  performance_report.py
  simulator_with_exits.py
  position_monitor.py
  reservation_monitor.py
  launcher.py
  db_config.py
  mt5_time_utils.py
  mt5_trade_utils.py
  telegram_sender_utils.py
  ktr_db_utils.py
  trade_performance_db.py
  position_monitor_db.py
  ktr_sltp_utils.py
  ktr_sltp_updater.py
  ktr_lots.py
  position_monitoring_closing.py
  ktr_order_reservation_gui.py
  position_monitor_launcher.py   # 포지션 모니터 GUI (v1과 동일)
  position_monitor_bb_refresh.py # BB 새로고침 (GUI에서 호출)
  logs/
  reports/
```

## 데이터베이스 (v2/scheduler.db)

- **경로**: v2 폴더의 `scheduler.db` 한 파일에 KTR 기록(ktr_records), 봉 데이터(bars), 매매 기록(trade_records) 통합.
- **경로 설정**: `path_config.yaml`에서 `db_path`로 지정하거나, 비우면 v2 폴더의 `scheduler.db` 사용. 환경변수 `WINDOWS_SCHEDULER_DB`로도 지정 가능.
- **최초 1회**:
  - v1을 쓰던 경우: v1의 `scheduler.db`가 있으면 `python init_db.py` 실행 시 v2로 복사됩니다.
  - 없으면: `python init_db.py` 실행 시 v2에 빈 `scheduler.db`를 만들고 필요한 테이블을 생성합니다.
  ```bash
  cd c:\py_project\v2
  python init_db.py
  ```

## 다른 PC에서 실행 시 (경로 환경 파일)

**다른 컴퓨터에서 실행할 때는 `path_config.yaml`만 수정하면 됩니다.** 프로그램 전체의 경로를 바꿀 필요가 없습니다.

- **path_config.yaml** (v2 폴더에 있음)
  - `mt5_path`: MetaTrader 5 `terminal64.exe` 전체 경로 (해당 PC의 MT5 설치 경로로 수정)
  - `mt5_terminal_data_folder`: (선택) MT5 터미널 데이터 폴더
  - `db_path`: (선택) 비우면 v2/scheduler.db 사용
- 없으면 `path_config.example.yaml`을 복사해 `path_config.yaml`로 저장한 뒤 위 항목만 수정하면 됩니다.
- 환경변수 `MT5_PATH`, `WINDOWS_SCHEDULER_DB`로도 덮어쓸 수 있습니다.

## 사용법

- **v2 폴더에서 실행** (독립 실행):
  ```bash
  cd c:\py_project\v2
  python launcher.py
  python launcher.py position
  python launcher.py reservation
  python launcher.py report
  python launcher.py position loop
  python launcher.py reservation loop
  ```
- 또는 **상위 폴더에서 v2 지정**:
  ```bash
  cd c:\py_project
  python v2/launcher.py
  ```
- **성과 리포트만**: `python performance_report.py` (v2 폴더에서)
- **포지션 모니터 GUI** (v1과 동일한 창): `python position_monitor_launcher.py` (v2 폴더에서)
- **예약 오더 GUI**: `python ktr_order_reservation_gui.py` (v2 폴더에서)
## 설정 (config.yaml)

- `position_monitor.check_interval_sec`: 포지션 점검 주기(초)
- `reservation_monitor.poll_interval_sec`: 예약 루프 주기(초), 봉 마감 시점에만 조건 점검
- `risk_limits.daily_max_loss_pct`: 일일 손실 이 비율 초과 시 신규 진입 중단
- `risk_limits.daily_max_entries`: 일일 최대 진입 횟수
- `risk_limits.symbol_max_positions`: 심볼당 동시 포지션 최대
- `execution.max_retries`, `alert_on_failure`: 재시도 횟수, 실패 시 텔레그램

v2는 **v1 없이** 독립 실행됩니다. 필요한 모듈(포지션 모니터, 예약 GUI, DB, MT5 유틸 등)은 모두 v2 폴더에 포함되어 있습니다.

---

## v1과 동일 동작 확인 방법

v2가 v1과 같은 로직으로 동작하는지 확인할 때 아래 순서로 점검하면 됩니다.

### 1. 기능 매핑 (v1 → v2)

| v1 실행 방식 | v2 실행 방식 | 비고 |
|-------------|-------------|------|
| `position_monitor_launcher.py` → subprocess `position_monitoring_closing.py` (무한 루프) | `python launcher.py position loop` | 포지션 모니터 주기 점검 |
| `position_monitoring_closing.py` 1회 실행 | `python launcher.py position` | 포지션 1회 점검(청산 조건·SL/TP 등) |
| 예약 오더 점검 (v1에서 별도 스크립트/GUI) | `python launcher.py reservation` 또는 `reservation loop` | 예약 조건 점검·주문 실행 |
| (v1에 성과 리포트가 있다면) | `python launcher.py report` | 성과 리포트 생성 |

- **포지션 모니터**: v1은 GUI 런처가 `position_monitoring_closing.py`를 주기적으로 실행. v2는 `position_monitor.py`가 같은 `position_monitoring_closing.run_one_check()`를 호출하므로 **청산·SL/TP 로직은 동일**합니다.
- **예약 모니터**: v2는 v1에서 복사한 `ktr_order_reservation_gui` 모듈의 `check_entry_condition_with_detail`, 봉 마감 판단 등을 사용하므로 **진입 조건·타임프레임 기준은 동일**합니다.

### 2. 확인 체크리스트

1. **DB 경로**
   - v2 실행 시 `db_config.UNIFIED_DB_PATH`가 v2 폴더의 `scheduler.db`를 가리키는지 확인 (로그에 `[DB]` 또는 초기화 메시지로 확인).
   - v1 데이터를 쓰려면 `python init_db.py`로 v1 DB를 v2로 복사한 뒤 v2만 실행.

2. **포지션 모니터 1회**
   - v2 폴더에서: `python launcher.py position`
   - MT5 연결·포지션 조회·청산 조건·SL/TP 갱신이 v1과 같이 동작하는지 로그(`logs/` 또는 콘솔)로 확인.

3. **예약 모니터 1회**
   - v2 폴더에서: `python launcher.py reservation`
   - 예약 목록 로드·봉 마감 시점 조건 점검·진입 시 주문 실행이 v1과 같이 나오는지 로그로 확인.

4. **동일 입력으로 비교 (선택)**
   - 같은 MT5 계정·같은 예약·같은 시간대에 v1에서 1회 실행한 결과(로그·주문 여부)와 v2에서 1회 실행한 결과를 비교.
   - v1: v1 폴더에서 `python position_monitoring_closing.py` 1회 (또는 GUI로 시작 후 한 사이클 로그 캡처).
   - v2: v2 폴더에서 `python launcher.py` (기본: 포지션 1회 + 예약 1회) 또는 `position` / `reservation` 각각 1회.

5. **환경변수 (v1과 맞추고 싶을 때)**
   - v1 포지션 모니터는 `POSITION_MONITOR_STOP_LOSS_PCT`, `POSITION_MONITOR_NIGHT_7PCT` 등을 사용. v2는 `config.yaml`의 `position_monitor` 등으로 제어. 동일 동작을 보려면 config.yaml의 청산 관련 값을 v1과 맞춰 두면 됩니다.

### 3. 요약

- **실행 경로**: 반드시 v2 폴더에서 실행 (`cd ...\v2` 후 `python launcher.py ...`).
- **동일성**: 포지션 청산·예약 진입 로직은 v1에서 가져온 모듈(`position_monitoring_closing`, `ktr_order_reservation_gui`)을 그대로 사용하므로, **같은 DB·같은 config면 결과가 v1과 동일**해야 합니다.
- **차이점**: v2는 설정(config.yaml), 로그(구조화 로그), 리스크 상한·재시도·알림 등이 추가되어 있으나, 핵심 매매 로직은 v1과 같습니다.
