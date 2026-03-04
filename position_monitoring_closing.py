# -*- coding: utf-8 -*-
"""
오픈 포지션을 모니터링하여, 진입 타임프레임(comment의 1H/5M/10M 등) 봉 기준으로 청산 조건 점검 후 텔레그램 알림.

청산 조건 (진입 TF 봉 기준, 해당 TF 포지션만 청산):
- 매수(Long/기본더블비): (1) 이번 봉이 4B(4,4) 상단에 닿기만 하고(돌파 아님) 4B 아래에서 마감 시 청산. (2) 역배열(20이평 < 120이평) 구간에서는 이번 봉이 20이평에 닿으면(Low ≤ 20이평) 청산.
- 매도(Short): 직전 봉 20B 미터치·이번 봉만 20B(20,2) 하단 터치 시 청산.

Long 포지션 T/P: 현재가가 20이평 아래면 T/P 없음(제거). 20이평 상향 돌파 시 T/P = 20B상단 - 오프셋%(심볼별 ktr_bb_offset.json). 타임프레임별 봉이 갱신될 때마다 재계산·갱신(고정 아님).

월~금 23:25(KST): 전 포지션 강제 청산 (하루 1회).

토 07:00(KST) ~ 월 07:30(KST): 포지션 모니터링 미실행 (주말 휴장).
"""
import atexit
import io
import sqlite3
import sys
import os
import time
import json
import math
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

import pytz
import MetaTrader5 as _mt5  # type: ignore[reportMissingImports]
mt5: Any = _mt5
import mt5_trade_utils as tr
from dotenv import load_dotenv

from telegram_sender_utils import send_telegram_msg
from ktr_sltp_updater import _parse_comment
from ktr_sltp_utils import get_tp_level, get_ktr_from_db_auto
import position_monitor_db as pm_db

load_dotenv()


def _remove_reservations_for_symbol(symbol: str) -> int:
    """청산된 심볼에 해당하는 예약 오더 삭제는 하지 않음. (예약은 GUI에서만 추가/삭제, ktr_reservations.json 유지)"""
    return 0

# 출력 인코딩
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# 설정
# MT5 경로: 환경변수 MT5_PATH 또는 mt5_trade_utils (현재 폴더 기준)
MT5_PATH = os.environ.get("MT5_PATH", getattr(tr, "MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe"))
CHECK_INTERVAL_SEC = 60  # 전체 점검(포지션·청산·20B T/P 갱신) 주기: 1분 (캔들 변동에 맞춰 T/P 계속 업데이트)
DB_UPDATE_INTERVAL_SEC = 30  # DB 캔들 갱신 시도 주기: 30초

# 1H 봉 지표 파라미터
SMA_PERIOD = 20
SMA_PERIOD_120 = 120
BB_PERIOD_4, BB_STD_4 = 4, 4
BB_PERIOD_20, BB_STD_20 = 20, 2
SMA4_PERIOD = 4  # 4이평 (윗꼬리 터치 청산 패턴용)
RATES_COUNT = 150  # 1H 봉 (120이평 계산에 충분)
MIN_1H_BARS_FOR_LEVELS = 121  # 1H 레벨 점검(20/120이평, BB)에 필요한 최소 봉 수

# 포지션 오픈 후 이 시간이 지나야 청산 대상 (2시간 이내 포지션은 청산 제외)
MIN_AGE_HOURS = 2
MIN_AGE_SEC = MIN_AGE_HOURS * 3600

# balance(잔고) 대비 손실금액 손실률이 이 값 이하이면 전 포지션 청산 (예: 잔고 1000$, 손실 100$ → -10%)
# 런처에서 선택한 손실율(7/10/20/30/50)은 환경변수 POSITION_MONITOR_STOP_LOSS_PCT 로 전달됨 (기본 20 → -20%)
OVERALL_LOSS_RATE_STOP_THRESHOLD = -20.0
_stop_pct = os.getenv("POSITION_MONITOR_STOP_LOSS_PCT", "20").strip()
try:
    _pct = float(_stop_pct)
    if _pct > 0:
        OVERALL_LOSS_RATE_STOP_THRESHOLD = -_pct
except ValueError:
    pass

# 마진레벨(equity/증거금*100)이 이 값 이하이면 전 포지션 청산. 런처에서 100/200/300 선택.
MARGIN_LEVEL_CLOSE_PCT = 200.0
_margin_close = os.getenv("POSITION_MONITOR_MARGIN_LEVEL_CLOSE_PCT", "200").strip()
try:
    _m = float(_margin_close)
    if _m > 0:
        MARGIN_LEVEL_CLOSE_PCT = _m
except ValueError:
    pass

# 잔액(balance) 대비 마진이 이 비율(%)을 초과하면 가장 나중에 만들어진 오더 1건 청산 (예: 7% 초과 시 최신 포지션 1건 청산).
MARGIN_PCT_CLOSE_LAST_ORDER = 7.0
_margin_pct_last = os.getenv("POSITION_MONITOR_MARGIN_PCT_CLOSE_LAST", "7").strip()
try:
    _mp = float(_margin_pct_last)
    if _mp > 0:
        MARGIN_PCT_CLOSE_LAST_ORDER = _mp
except ValueError:
    pass

# 수익금이 잔액의 이 비율을 초과하면 전 포지션 청산 후 동일 규모로 재진입 (수익 실현).
PROFIT_TAKE_PCT = 50.0

# 손실율(청산 판단용) = 전체 포지션 (손익+스왑) 합계 / 계정 balance(잔고) * 100.

# KTR 예약 진입 매직. 이 매직이 아니면 수작업(MT5 직접) 오더로 간주.
MAGIC_KTR = 888001
# (삭제됨) 수작업 오더 진입 시간 제한 — 오더 만들 수 있는 시간 제한 없음.

# 세션별 기준 봉 + 구간 (KST). 기준 봉 High 초과 Close → Short 청산, Low 하회 Close → Long 청산
KST = pytz.timezone("Asia/Seoul")
from mt5_time_utils import MT5_SESSION_OFFSET_SEC, mt5_ts_to_kst
# 아시아: 8~9시 봉 기준, 9~17시 구간
ASIA_REF_HOUR, ASIA_WINDOW_START, ASIA_WINDOW_END = 8, 9, 17
# 유럽: 17~18시 봉 기준, 18~23시 구간
EUROPE_REF_HOUR, EUROPE_WINDOW_START, EUROPE_WINDOW_END = 17, 18, 23
# 미국: 23~24시 봉 기준, 0~7시 구간(다음날). ref는 전날 23시 봉
US_REF_HOUR, US_WINDOW_START, US_WINDOW_END = 23, 0, 7

# 월~금 23:25(KST)에 전 포지션 강제 청산 (하루 1회만 실행)
WEEKDAY_2325_CLOSE_HOUR = 23
WEEKDAY_2325_CLOSE_MIN_START = 25
WEEKDAY_2325_CLOSE_MIN_END = 29  # 5분 주기이므로 23:25~23:29 구간에서 1회만
_last_2325_close_date: Optional[str] = None  # 오늘 이미 실행했는지 ("YYYY-MM-DD")

# 토 07:00(KST) ~ 월 07:30(KST) 구간에서는 포지션 모니터링 미실행 (주말 휴장)
WEEKEND_OFF_START_WEEKDAY = 5  # Saturday
WEEKEND_OFF_START_HOUR, WEEKEND_OFF_START_MIN = 7, 0
WEEKEND_OFF_END_WEEKDAY = 0  # Monday
WEEKEND_OFF_END_HOUR, WEEKEND_OFF_END_MIN = 7, 30


def _is_weekend_off_window() -> bool:
    """토 07:00(KST) ~ 월 07:30(KST) 구간이면 True (모니터링 미실행)."""
    now = datetime.now(KST)
    wd = now.weekday()  # 0=Mon .. 6=Sun
    h, m = now.hour, now.minute
    if wd == WEEKEND_OFF_START_WEEKDAY:  # Saturday
        return h > WEEKEND_OFF_START_HOUR or (h == WEEKEND_OFF_START_HOUR and m >= WEEKEND_OFF_START_MIN)
    if wd == 6:  # Sunday
        return True
    if wd == WEEKEND_OFF_END_WEEKDAY:  # Monday
        return h < WEEKEND_OFF_END_HOUR or (h == WEEKEND_OFF_END_HOUR and m < WEEKEND_OFF_END_MIN)
    return False


# GUI 런처에서 선택한 BB 타임프레임 (M5 / M10 / H1). 파일에 저장되어 매 점검 시 읽음.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BB_TF_FILE = os.path.join(_SCRIPT_DIR, "position_monitor_bb_tf.txt")
# 청산 시작/정지: 런처가 "1"이면 청산 실행, "0"이면 청산만 중지(포지션/DB/BB 등은 계속 실행).
CLOSING_ENABLED_FILE = os.path.join(_SCRIPT_DIR, "position_monitor_closing_enabled.txt")
# 심볼별 BB 오프셋 % (T/P = 20B상단 - 오프셋). KTR 예약 GUI와 동일한 파일 사용.
BB_OFFSET_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "ktr_bb_offset.json"))
# 심볼별 기본 오프셋 %. 지수(나스닥)는 절대가격이 커서 오프셋 %를 더 크게 적용 (KTR GUI와 동일)
BB_OFFSET_SYMBOLS = ("XAUUSD+", "NAS100+")
DEFAULT_BB_OFFSET_PCT: dict = {"XAUUSD+": 0.5, "NAS100+": 2.5}
# 실시간 오더에서 T/P를 수동 업데이트한 티켓. 이 티켓에는 20B T/P를 넣지 않음.
REALTIME_TP_TICKETS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "position_monitor_realtime_tp_tickets.json"))
# 10분봉 4B/20B 자동오더 예약 주문 티켓 (가격 갱신용, ktr_order_reservation_gui와 동일 파일)
M10_BB_AUTO_ORDERS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "m10_bb_auto_orders.json"))
# 23:25 전량 청산 실행 여부 (프로세스 간 중복 방지: 날짜별 마커 파일)
def _path_2325_done_marker(date_str: str) -> str:
    return os.path.join(_SCRIPT_DIR, f".position_monitor_2325_done_{date_str}")
# 런처에서 선택한 손실율/마진레벨 (실행 중 라디오 변경 시에도 반영되도록 매 점검 시 파일에서 재로드)
STOP_LOSS_PCT_FILE = os.path.join(_SCRIPT_DIR, "position_monitor_stop_loss_pct.txt")
MARGIN_LEVEL_CLOSE_FILE = os.path.join(_SCRIPT_DIR, "position_monitor_margin_level_close.txt")
# 손실률 -5% 단위 알림: 이미 알림 보낸 구간 (0이면 리셋, -5/-10/... 이면 해당 %까지 알림 보냄)
LOSS_ALERT_SENT_FILE = os.path.join(_SCRIPT_DIR, "position_monitor_loss_alert_sent.txt")


def _reload_stop_params_from_files() -> None:
    """런처가 쓴 손실율/마진레벨 파일을 읽어 전역 기준값 갱신. (버튼 변경 시 재시작 없이 반영)"""
    global OVERALL_LOSS_RATE_STOP_THRESHOLD, MARGIN_LEVEL_CLOSE_PCT
    if os.path.isfile(STOP_LOSS_PCT_FILE):
        try:
            with open(STOP_LOSS_PCT_FILE, "r", encoding="utf-8") as f:
                s = f.read().strip()
            if s:
                pct = float(s)
                if pct > 0:
                    OVERALL_LOSS_RATE_STOP_THRESHOLD = -pct
        except Exception:
            pass
    if os.path.isfile(MARGIN_LEVEL_CLOSE_FILE):
        try:
            with open(MARGIN_LEVEL_CLOSE_FILE, "r", encoding="utf-8") as f:
                s = f.read().strip()
            if s:
                m = float(s)
                if m > 0:
                    MARGIN_LEVEL_CLOSE_PCT = m
        except Exception:
            pass


def _send_loss_rate_alert_if_stepped(positions: Any, balance: float, total_profit: float) -> None:
    """잔고 대비 손실률이 -5%, -10%, -15% ... 구간을 넘을 때마다 텔레그램 알림 1회. 손실률이 -5% 위로 회복되면 다음 하락 시 다시 -5%부터 알림."""
    if not positions or balance <= 0:
        return
    loss_rate_pct = total_profit / balance * 100
    # -5% 미만(회복)이면 알림 구간 리셋
    if loss_rate_pct > -5:
        try:
            if os.path.isfile(LOSS_ALERT_SENT_FILE):
                with open(LOSS_ALERT_SENT_FILE, "w", encoding="utf-8") as f:
                    f.write("0")
        except Exception:
            pass
        return
    # 현재 구간: -5 단위로 내림 (예: -7% → -10, -5% → -5)
    current_step = -5 * math.ceil(abs(loss_rate_pct) / 5)
    last_sent = 0
    try:
        if os.path.isfile(LOSS_ALERT_SENT_FILE):
            with open(LOSS_ALERT_SENT_FILE, "r", encoding="utf-8") as f:
                s = f.read().strip()
            if s:
                last_sent = int(float(s))
    except Exception:
        pass
    # 새로 도달한 구간이면 알림 전송 후 기록
    if current_step < last_sent:
        try:
            margin = 0.0
            margin_ratio_pct = 0.0
            acc = tr.get_account_info()
            if acc is not None:
                margin = float(acc.get("margin", 0) or 0)
                margin_ratio_pct = (margin / balance * 100.0) if balance > 0 else 0.0
            _send_telegram(
                f"📉 **손실률 {current_step}% 구간 도달**\n"
                f"• 현재 손실률: {loss_rate_pct:.2f}%\n"
                f"• 잔고: ${balance:,.0f} | 손익: ${total_profit:+,.2f}\n"
                f"• 마진: ${margin:,.0f} | 잔액 대비 마진: {margin_ratio_pct:.1f}%\n"
                f"• 시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST"
            )
            with open(LOSS_ALERT_SENT_FILE, "w", encoding="utf-8") as f:
                f.write(str(current_step))
        except Exception as e:
            print(f"  ⚠️ 손실률 알림 전송 실패: {e}", flush=True)


def _load_bb_offset_pct() -> dict:
    """심볼별 볼린저 밴드 오프셋 % 로드. 파일에 없으면 지수 규모에 따른 기본값 사용."""
    result: dict = {}
    if os.path.isfile(BB_OFFSET_PATH):
        try:
            with open(BB_OFFSET_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            result = {k: float(v) for k, v in (data or {}).items() if isinstance(v, (int, float))}
        except Exception:
            pass
    for sym in BB_OFFSET_SYMBOLS:
        if sym not in result:
            result[sym] = DEFAULT_BB_OFFSET_PCT.get(sym, 0.0)
    return result


def _apply_bb_offset_upper(value: float, offset_pct: float) -> float:
    """20B 상단에 오프셋 % 적용 → T/P는 반드시 20B 상단보다 낮은 값. offset_pct 0이면 value 그대로."""
    if not offset_pct:
        return value
    # 오프셋 %만큼 낮춤: value * (1 - pct/100). 음수 오프셋 방지로 상단 초과하지 않도록 min 적용.
    pct = max(0.0, float(offset_pct))
    result = value * (1.0 - pct / 100.0)
    return min(result, value)  # 20B 상단보다 높아지지 않도록 보장


def _apply_bb_offset_lower(value: float, offset_pct: float) -> float:
    """20B/4B 하단에 오프셋 % 적용 → 하단은 높아지게 *(1+offset%). 10M 4B/20B 자동오더 가격용."""
    if not offset_pct:
        return value
    pct = max(0.0, float(offset_pct))
    return value * (1.0 + pct / 100.0)


def _update_m10_bb_auto_order_prices() -> None:
    """m10_bb_auto_orders.json에 등록된 10분봉 20B/4B 하단 예약 주문의 가격을 현재 10M BB(오프셋) 값으로 갱신."""
    if not os.path.isfile(M10_BB_AUTO_ORDERS_PATH):
        return
    try:
        with open(M10_BB_AUTO_ORDERS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return
    if not isinstance(data, list):
        return
    if not tr.init_mt5():
        return
    bb_offset_map = _load_bb_offset_pct()
    for entry in data:
        if not isinstance(entry, dict):
            continue
        symbol = entry.get("symbol")
        ticket_20b = entry.get("ticket_20b")
        ticket_4b = entry.get("ticket_4b")
        if not symbol or (not ticket_20b and not ticket_4b):
            continue
        rates = get_rates_for_tf(symbol, "M10", count=25)
        if rates is None or len(rates) < 21:
            continue
        l20 = get_20b_lower_from_rates(rates)
        l4 = get_4b_lower_from_rates(rates)
        if l20 is None and l4 is None:
            continue
        offset_pct = bb_offset_map.get(symbol, 0) or 0
        l20_off = _apply_bb_offset_lower(float(l20), offset_pct) if l20 is not None else None
        l4_off = _apply_bb_offset_lower(float(l4), offset_pct) if l4 is not None else None
        if l20_off is None:
            l20_off = l4_off
        if l4_off is None:
            l4_off = l20_off
        if ticket_20b and l20_off is not None:
            ok, msg = tr.modify_pending_order_price(int(ticket_20b), l20_off)
            if ok:
                print(f"  [10M 4B/20B 자동오더] {symbol} 20B 하단 예약 가격 갱신: {l20_off:.5g}", flush=True)
            else:
                print(f"  [10M 4B/20B 자동오더] {symbol} 20B 하단 수정 실패: {msg}", flush=True)
        if ticket_4b and l4_off is not None:
            ok, msg = tr.modify_pending_order_price(int(ticket_4b), l4_off)
            if ok:
                print(f"  [10M 4B/20B 자동오더] {symbol} 4B 하단 예약 가격 갱신: {l4_off:.5g}", flush=True)
            else:
                print(f"  [10M 4B/20B 자동오더] {symbol} 4B 하단 수정 실패: {msg}", flush=True)


def _load_realtime_tp_tickets() -> set:
    """실시간 오더에서 T/P를 설정한 티켓 ID 집합. 이 티켓에는 20B T/P를 넣지 않음."""
    if not os.path.isfile(REALTIME_TP_TICKETS_PATH):
        return set()
    try:
        with open(REALTIME_TP_TICKETS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(int(x) for x in (data if isinstance(data, list) else []) if isinstance(x, (int, float)))
    except Exception:
        return set()


def _save_realtime_tp_tickets(tickets: set) -> None:
    """실시간 T/P 티켓 목록 저장 (청산된 티켓 제거 후)."""
    try:
        with open(REALTIME_TP_TICKETS_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(tickets), f)
    except Exception:
        pass


try:
    from db_config import UNIFIED_DB_PATH
    KTR_DB_PATH = UNIFIED_DB_PATH
    PM_DB_PATH = UNIFIED_DB_PATH
except ImportError:
    _FALLBACK_DB = os.path.join(_SCRIPT_DIR, "scheduler.db")
    KTR_DB_PATH = _FALLBACK_DB
    PM_DB_PATH = _FALLBACK_DB
BB_RATES_COUNT = 25  # 20/2 BB 계산에 21봉 필요


def _get_bb_tf_from_file() -> str:
    """런처가 저장한 BB 타임프레임 읽기. 기본 H1."""
    try:
        if os.path.isfile(BB_TF_FILE):
            with open(BB_TF_FILE, "r", encoding="utf-8") as f:
                t = f.read().strip().upper()
            if t in ("M5", "M10", "H1"):
                return t
    except Exception:
        pass
    return "H1"


def _is_closing_enabled() -> bool:
    """청산 기능 항상 활성화 (과거: 런처 파일 "1"/"0"으로 시작·정지)."""
    return True


def _mt5_tf_from_str(tf_str: str) -> int:
    if tf_str == "M5":
        return mt5.TIMEFRAME_M5
    if tf_str == "M10":
        return mt5.TIMEFRAME_M10
    if tf_str == "H2":
        return getattr(mt5, "TIMEFRAME_H2", 16386)
    if tf_str == "H4":
        return getattr(mt5, "TIMEFRAME_H4", 16388)
    return mt5.TIMEFRAME_H1


# DB에서 캔들을 읽을 수 있는 심볼·타임프레임 (포지션 모니터가 갱신하는 조합)
_DB_SYMBOLS = ("XAUUSD+", "NAS100+")
_DB_TIMEFRAMES = ("M5", "M10", "H1", "H2", "H4")


def get_rates_for_tf(symbol: str, tf_str: str, count: int = 150) -> Optional[Any]:
    """지정 타임프레임(5/10분봉, 1시간봉)의 봉 데이터 조회. DB 우선(해당 심볼/TF일 때), 없으면 MT5."""
    if symbol in _DB_SYMBOLS and tf_str in _DB_TIMEFRAMES:
        rates = pm_db.get_rates_from_db(symbol, tf_str, limit=count)
        if rates is not None and len(rates) >= min(21, count):
            return rates
    if not mt5.symbol_select(symbol, True):
        return None
    mt5_tf = _mt5_tf_from_str(tf_str)
    rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, count)
    if rates is None or len(rates) < 2:
        return None
    return rates


def _get_rates_from_mt5_only(symbol: str, tf_str: str, count: int = 150) -> Optional[Any]:
    """DB 갱신 전용: MT5에서만 봉 조회(DB 미사용). 타임프레임별로 독립 복사본 반환(MT5 버퍼 재사용 방지)."""
    if not mt5.symbol_select(symbol, True):
        return None
    mt5_tf = _mt5_tf_from_str(tf_str)
    rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, count)
    if rates is None or len(rates) < 2:
        return None
    # MT5가 동일 버퍼를 재사용하면 다음 호출 시 이전 타임프레임 데이터가 덮어써질 수 있음 → 복사본 반환
    try:
        return rates.copy()
    except AttributeError:
        return rates


def get_rates_for_bb(symbol: str) -> Optional[Any]:
    """BB 표시용: 선택된 타임프레임(5/10분봉, 1시간봉)의 봉 데이터 조회."""
    tf_str = _get_bb_tf_from_file()
    rates = get_rates_for_tf(symbol, tf_str, count=BB_RATES_COUNT)
    if rates is None or len(rates) < 21:
        return None
    return rates


def _index_of_last_closed_bar_kst(rates: Any, tf_str: str) -> Optional[int]:
    """
    KST 기준 '직전 봉'(방금 마감된 봉)의 rates 인덱스를 반환.
    MT5 copy_rates_from_pos의 [1]은 서버 시간 기준 직전 봉이라 차트(KST)와 다를 수 있음.
    mt5_ts_to_kst로 봉 시각을 보정한 뒤, 현재 시각 기준 직전에 마감된 KST 봉을 찾음.
    """
    if rates is None or len(rates) == 0:
        return None
    now_kst = datetime.now(KST)
    if tf_str == "H1":
        # 직전 마감 1시간봉: e.g. 14:35 KST → 13:00 시작 봉
        target = (now_kst - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    elif tf_str == "H2":
        t = (now_kst - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
        target = t.replace(hour=(t.hour // 2) * 2)
    elif tf_str == "H4":
        t = (now_kst - timedelta(hours=4)).replace(minute=0, second=0, microsecond=0)
        target = t.replace(hour=(t.hour // 4) * 4)
    elif tf_str == "M10":
        if now_kst.minute < 10:
            target = (now_kst - timedelta(hours=1)).replace(minute=50, second=0, microsecond=0)
        else:
            target = now_kst.replace(minute=(now_kst.minute // 10 - 1) * 10, second=0, microsecond=0)
    elif tf_str == "M5":
        if now_kst.minute < 5:
            target = (now_kst - timedelta(hours=1)).replace(minute=55, second=0, microsecond=0)
        else:
            target = now_kst.replace(minute=(now_kst.minute // 5 - 1) * 5, second=0, microsecond=0)
    else:
        target = (now_kst - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    # H1/H2/H4: rates[0]=최신(진행 중 봉)이므로, 직전 마감 봉을 찾으려면 과거→최신 순으로 검사해 target 봉을 선택해야 함.
    # 순방향이면 현재 봉(6시 등)을 직전 마감으로 잘못 선택해, 실제 직전 마감 봉(5시)이 DB에 안 들어가 4시 다음 6시만 나오는 현상 발생.
    indices = range(len(rates)) if tf_str in ("M5", "M10") else range(len(rates) - 1, -1, -1)
    for i in indices:
        bar_ts = int(rates["time"][i])
        bar_dt = mt5_ts_to_kst(bar_ts)
        if tf_str == "H1":
            # 직전 마감 봉: MT5가 봉 시작(11:00) 또는 봉 끝(12:00)으로 줄 수 있음 → 둘 다 같은 봉으로 인정
            if bar_dt.minute != 0:
                continue
            if bar_dt.hour != target.hour and bar_dt.hour != (target.hour + 1) % 24:
                continue
            if bar_dt.date() == target.date():
                return i
            if target.hour == 23 and (bar_dt.date() - target.date()).days == 1 and bar_dt.hour == 0:
                return i
        elif tf_str == "H2":
            # 같은 2시간 구간(10~12 등)이면 동일 봉으로 인정 (MT5 서버 시각에 따라 11시 등으로 올 수 있음)
            if bar_dt.date() == target.date() and bar_dt.minute == 0 and (bar_dt.hour // 2) * 2 == target.hour:
                return i
        elif tf_str == "H4":
            # 같은 4시간 구간(08~12 등)이면 동일 봉으로 인정 (MT5 서버 시각에 따라 9·10·11시로 올 수 있음)
            if bar_dt.date() == target.date() and bar_dt.minute == 0 and (bar_dt.hour // 4) * 4 == target.hour:
                return i
        elif tf_str == "M10":
            if bar_dt.date() == target.date() and bar_dt.hour == target.hour and bar_dt.minute == target.minute:
                return i
        elif tf_str == "M5":
            if bar_dt.date() == target.date() and bar_dt.hour == target.hour and bar_dt.minute == target.minute:
                return i
    return None


_mt5_shutdown_registered = False


def _shutdown_mt5_on_exit() -> None:
    """프로세스 종료 시 MT5 연결 해제 (atexit에서 1회만 호출)."""
    try:
        mt5.shutdown()
    except Exception:
        pass


def init_mt5() -> bool:
    global _mt5_shutdown_registered
    if not mt5.initialize(path=MT5_PATH):
        print(f"❌ MT5 초기화 실패: {mt5.last_error()}", flush=True)
        return False
    if not _mt5_shutdown_registered:
        atexit.register(_shutdown_mt5_on_exit)
        _mt5_shutdown_registered = True
    return True


def _send_telegram(text: str) -> None:
    send_telegram_msg(text)


def _log_closes_to_file(reason: str, closed_list: List[dict], detail: str = "") -> None:
    """청산 발생 시 파일에 기록. reason=청산 사유, closed_list=청산된 포지션 목록, detail=추가 설명. 텔레그램 실패해도 로그로 원인 파악 가능."""
    if not closed_list:
        return
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "position_close.log")
        now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "a", encoding="utf-8") as f:
            for c in closed_list:
                sym = c.get("symbol", "")
                ticket = c.get("ticket", "")
                side = c.get("type", "")
                profit = c.get("profit", 0)
                line = f"{now} | {reason} | {sym} #{ticket} {side} ${profit:+,.2f}"
                if detail:
                    line += f" | {detail}"
                f.write(line + "\n")
    except Exception as e:
        print(f"  [청산 로그 기록 실패] {e}", flush=True)


def sma_last(closes: List[float], period: int) -> Optional[float]:
    """가장 최근 period개 종가의 SMA (closes는 과거→최신 순이므로 마지막 period개 사용)"""
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _rma_series(values: List[float], length: int) -> Optional[List[float]]:
    """Wilder RMA. First = SMA of first length, then rma[i] = (rma[i-1]*(length-1)+values[i])/length."""
    if not values or length < 1 or len(values) < length:
        return None
    out: List[float] = []
    out.append(sum(values[:length]) / length)
    for i in range(length, len(values)):
        out.append((out[-1] * (length - 1) + values[i]) / length)
    return out


def _rsi_series(closes_chron: List[float], period: int = 14) -> Optional[List[float]]:
    """RSI(period) 시리즈. closes_chron = 과거→현재 순. 반환: 앞 period개 None, 이후 RSI 값."""
    if not closes_chron or period < 1 or len(closes_chron) < period + 1:
        return None
    n = len(closes_chron)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, n):
        ch = closes_chron[i] - closes_chron[i - 1]
        gains.append(ch if ch > 0 else 0.0)
        losses.append(-ch if ch < 0 else 0.0)
    rma_g = _rma_series(gains, period)
    rma_l = _rma_series(losses, period)
    if not rma_g or not rma_l or len(rma_g) != n - period + 1 or len(rma_l) != n - period + 1:
        return None
    result: List[Optional[float]] = [None] * (period - 1)
    for j in range(len(rma_g)):
        g, l = rma_g[j], rma_l[j]
        if l <= 0:
            result.append(100.0)
        else:
            result.append(100.0 - 100.0 / (1.0 + g / l))
    return result


def bollinger_upper(closes: List[float], period: int, num_std: float) -> Optional[float]:
    """볼린저 상단. TradingView Pine ta.stdev와 동일하게 표본 표준편차(n-1) 사용."""
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    variance = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = variance ** 0.5 if variance > 0 else 0.0
    return mid + num_std * std


def bollinger_lower(closes: List[float], period: int, num_std: float) -> Optional[float]:
    """볼린저 하단. 표본 표준편차(n-1) 사용."""
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    variance = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = variance ** 0.5 if variance > 0 else 0.0
    return mid - num_std * std


def bollinger_upper_and_std(
    closes: List[float], period: int, num_std: float
) -> Optional[Tuple[float, float]]:
    """볼린저 상단과 표준편차 반환. Pine ta.stdev 동일(표본 σ, n-1)."""
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    variance = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = variance ** 0.5 if variance > 0 else 0.0
    upper = mid + num_std * std
    return (upper, std)


def get_1h_rates(symbol: str) -> Optional[Any]:
    """해당 심볼 1H 봉 데이터 조회. DB 우선(XAUUSD+/NAS100+), 없거나 봉 수 부족하면 MT5."""
    if symbol in _DB_SYMBOLS:
        rates = pm_db.get_rates_from_db(symbol, "H1", limit=RATES_COUNT)
        # 1H 레벨 점검(120이평 등)을 위해 최소 봉 수 이상일 때만 DB 사용
        if rates is not None and len(rates) >= MIN_1H_BARS_FOR_LEVELS:
            return rates
    if not mt5.symbol_select(symbol, True):
        print(f"⚠️ 종목 선택 실패: {symbol}")
        return None
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_H1, 0, RATES_COUNT)
    if rates is None or len(rates) < 2:
        return None
    return rates


def should_close_on_levels(rates: Any) -> tuple[bool, str, List[str]]:
    """
    [현재 미사용] run_one_check()에서는 20B 상단/하단 터치만 사용. 레벨 되튐 청산은 호출되지 않음.
    High는 레벨에 닿았지만 Close는 레벨에 닿지 않은 경우만 청산.
    (High >= 레벨 and Close < 레벨) → 레벨에서 되튐 후 종가가 아래 → 청산.
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    current_high = float(rates["high"][-1])
    current_close = float(rates["close"][-1])
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]

    reasons = []
    detail_lines: List[str] = []

    def _check(name: str, level: Optional[float]) -> bool:
        if level is None:
            detail_lines.append(f"  • {name}: (계산 불가)")
            return False
        high_ok = current_high >= level
        close_ok = current_close < level
        hit = high_ok and close_ok
        if hit:
            reasons.append(f"{name}({level:.2f})")
        h_str = f"High {current_high:.2f}{'≥' if high_ok else '<'}{level:.2f}"
        c_str = f"Close {current_close:.2f}{'<' if close_ok else '≥'}{level:.2f}"
        result = "해당사항 있음 (1)" if hit else "해당사항 없음 (0)"
        detail_lines.append(f"  • {name}: 레벨 {level:.2f} | {h_str}, {c_str} → {result}")
        return hit

    sma20 = sma_last(closes, SMA_PERIOD)
    _check("20이평", sma20)

    sma120 = sma_last(closes, SMA_PERIOD_120)
    _check("120이평", sma120)

    bb4_result = bollinger_upper_and_std(closes, BB_PERIOD_4, BB_STD_4)
    if bb4_result is not None:
        bb4_up, _ = bb4_result
        _check("BB4/4상단", bb4_up)
    else:
        _check("BB4/4상단", None)

    bb20_result = bollinger_upper_and_std(closes, BB_PERIOD_20, BB_STD_20)
    if bb20_result is not None:
        bb20_up, _ = bb20_result
        _check("BB20/2상단", bb20_up)
    else:
        _check("BB20/2상단", None)

    trigger = len(reasons) > 0
    reason_str = ", ".join(reasons) if reasons else ""
    return trigger, reason_str, detail_lines


def should_close_on_sma4_wick_rejection(rates: Any) -> tuple[bool, str, List[str]]:
    """
    4이평 윗꼬리 터치 후 음봉·4이평 미돌파 청산 패턴.
    - 앞선 캔들(직전 마감 봉): High > 4이평, Close < 4이평 (윗꼬리만 4이평 터치)
    - 현재 캔들: 음봉(Close < Open) 이고 Close < 4이평 (4이평 돌파 실패)
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    if rates is None or len(rates) < 5:
        detail_lines.append("  • 4이평 윗꼬리 패턴: 봉 수 부족")
        return False, "", detail_lines

    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    sma4 = sma_last(closes, SMA4_PERIOD)
    if sma4 is None:
        detail_lines.append("  • 4이평 윗꼬리 패턴: 4이평 계산 불가")
        return False, "", detail_lines

    high_prev = float(rates["high"][-2])
    close_prev = float(rates["close"][-2])
    open_cur = float(rates["open"][-1])
    close_cur = float(rates["close"][-1])

    prev_upper_wick_touch = high_prev > sma4 and close_prev < sma4
    cur_bearish = close_cur < open_cur
    cur_below_sma4 = close_cur < sma4
    trigger = prev_upper_wick_touch and cur_bearish and cur_below_sma4

    detail_lines.append(
        f"  • 4이평 윗꼬리 패턴: 직전봉 High {high_prev:.2f}{'>' if high_prev > sma4 else '≤'}4이평 {sma4:.2f}, "
        f"Close {close_prev:.2f}{'<' if close_prev < sma4 else '≥'}4이평 | "
        f"현재봉 음봉={cur_bearish}, Close {close_cur:.2f}{'<' if cur_below_sma4 else '≥'}4이평 → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    reason = "4이평 윗꼬리 터치 후 음봉·4이평 미돌파" if trigger else ""
    return trigger, reason, detail_lines


def should_close_on_three_bars_resistance(rates: Any) -> tuple[bool, str, List[str]]:
    """
    20이평·4이평 위, 20B 상단 아래 구간에서 직전 3개 봉이 저항(고점)을 돌파하지 못한 경우 청산.
    - 조건: 직전 마감 봉 종가 > 20이평, > 4이평, < 20B상단
    - 저항: 직전 3봉보다 앞선 봉의 High를 고점(저항)으로 두고, 직전 3봉 모두 High가 그 고점 이하이면 충족
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    if rates is None or len(rates) < 7:
        detail_lines.append("  • 3봉 저항 청산: 봉 수 부족")
        return False, "", detail_lines

    # 직전 마감 봉(-2) 기준 지표. 현재 봉(-1) 제외한 종가로 계산
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    last_close = float(rates["close"][-2])
    sma20 = sma_last(closes, SMA_PERIOD)
    sma4 = sma_last(closes, SMA4_PERIOD)
    bb20_result = bollinger_upper_and_std(closes, BB_PERIOD_20, BB_STD_20)
    bb20_up = bb20_result[0] if bb20_result else None

    if sma20 is None or sma4 is None or bb20_up is None:
        detail_lines.append("  • 3봉 저항 청산: 20이평/4이평/20B상단 계산 불가")
        return False, "", detail_lines

    above_sma20 = last_close > sma20
    above_sma4 = last_close > sma4
    below_bb20 = last_close < bb20_up
    zone_ok = above_sma20 and above_sma4 and below_bb20

    # 저항 = 직전 3봉(-4,-3,-2) 바로 앞 봉(-5)의 High. 직전 3봉 모두 High <= 저항이면 고점 돌파 실패
    resistance_high = float(rates["high"][-5])
    h_4 = float(rates["high"][-4])
    h_3 = float(rates["high"][-3])
    h_2 = float(rates["high"][-2])
    no_break_4 = h_4 <= resistance_high
    no_break_3 = h_3 <= resistance_high
    no_break_2 = h_2 <= resistance_high
    three_fail = no_break_4 and no_break_3 and no_break_2

    trigger = zone_ok and three_fail
    reason = "20이평·4이평 위·20B아래 3봉 고점 미돌파" if trigger else ""

    detail_lines.append(
        f"  • 3봉 저항: 20이평 위={above_sma20}, 4이평 위={above_sma4}, 20B아래={below_bb20} | "
        f"저항고점 {resistance_high:.2f}, 직전3봉 High ({h_4:.2f},{h_3:.2f},{h_2:.2f}) 모두 이하={three_fail} → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    return trigger, reason, detail_lines


# 도지 판정: 몸통이 봉 전체 범위의 이 비율 이하면 도지로 봄
DOJI_BODY_RATIO_MAX = 0.2
# 윗꼬리가 봉 전체 범위의 이 비율 이상이면 "긴 윗꼬리"
LONG_UPPER_WICK_RATIO_MIN = 0.5
# 돌파더블비 매수 청산: 4이평 위 도지/긴윗꼬리 + RSI <= RSI이동평균
RSI_PERIOD = 14
RSI_MA_PERIOD = 3

# 장대음봉 판정: 과거 N개 2시간봉 중 음봉 TR(High-Low) 상위 몇 퍼센트를 기준으로 사용할지
LARGE_BEAR_TOP_PERCENT = 1.0


def _notify_large_bearish_retracement_for_symbol(symbol: str) -> None:
    """
    과거 7일(≈최근 100개) 2시간봉 기준 장대음봉(음봉 TR 상위 1%) 임계값을 계산하고,
    과거 24시간(최근 12개 2시간봉) 내 장대음봉 존재 여부 및 되돌림 %를 DB(bar_time 기준)로 텔레그램에 알림.
    호출 시점: 해당 심볼의 1시간봉(H1) 마감 봉이 bars 테이블에 새로 저장될 때.
    """
    # 1) 과거 7일(≈100개) 2시간봉에서 음봉 TR 상위 1% 임계값 계산
    bars_2h = pm_db.get_bars_from_db(symbol, "H2", limit=100)
    if not bars_2h or len(bars_2h) < 2:
        return

    bearish_tr_list: List[float] = []
    for b in bars_2h:
        try:
            o = float(b["open"])
            c = float(b["close"])
            h = float(b["high"])
            l_ = float(b["low"])
        except Exception:
            continue
        if c < o:
            bearish_tr_list.append(h - l_)

    if len(bearish_tr_list) < 2:
        return

    bearish_tr_list.sort(reverse=True)
    idx = max(0, int(math.ceil(len(bearish_tr_list) * LARGE_BEAR_TOP_PERCENT / 100.0)) - 1)
    threshold = bearish_tr_list[idx]

    now_kst = datetime.now(KST)

    # 2) 과거 24시간(최근 12개 2시간봉) 내 봉 크기·장대음봉 여부
    bars_24h_desc = bars_2h[:12]  # bars_2h는 bar_time DESC (최신→과거)
    has_large_bearish = False
    latest_large_bar: Optional[dict] = None
    lines_24h: List[str] = []

    for idx_rel, b in enumerate(reversed(bars_24h_desc)):  # 오래된 → 최신 순으로 출력
        bar_time_str = (b.get("bar_time") or "").strip()
        try:
            o = float(b["open"])
            c = float(b["close"])
            h = float(b["high"])
            l_ = float(b["low"])
        except Exception:
            continue
        tr_val = h - l_
        bearish = c < o
        is_large = bearish and tr_val >= threshold
        if is_large:
            has_large_bearish = True
            latest_large_bar = b  # 반복 끝나면 가장 최근 장대음봉이 남음
        kind = "음봉" if bearish else "양봉"
        mark = " ★장대음봉" if is_large else ""
        lines_24h.append(
            f"  └ #{idx_rel + 1:02d} {bar_time_str}  H={h:.2f} L={l_:.2f} TR={tr_val:.2f}  {kind}{mark}"
        )

    # 3) 텔레그램 메시지 본문 구성
    header_lines = [
        f"📉 **2H 장대음봉 되돌림 체크** [{symbol}]",
        f"• 기준: 과거 7일 2H 음봉 TR 상위 {LARGE_BEAR_TOP_PERCENT:.1f}% (임계값: {threshold:.2f})",
        f"• 기준 시각: {now_kst.strftime('%Y-%m-%d %H:%M:%S')} KST",
        "",
        "• 과거 24시간 2H 봉 (bars.bar_time 기준):",
    ]
    header_lines.extend(lines_24h or ["  └ (데이터 부족)"])
    header_lines.append(f"• 점검결과: 과거 24시간 내 장대음봉 {'있음' if has_large_bearish else '없음'}")

    # 장대음봉이 없으면 여기까지 보고만
    if not has_large_bearish or latest_large_bar is None:
        try:
            _send_telegram("\n".join(header_lines))
        except Exception:
            pass
        return

    # 4) 기준 장대음봉 TR 대비 현재 되돌림 % (현재가는 DB H1 종가 사용)
    h = float(latest_large_bar["high"])
    l_ = float(latest_large_bar["low"])
    c_large = float(latest_large_bar["close"])
    tr_large = h - l_
    base_time = (latest_large_bar.get("bar_time") or "").strip()

    retrace_line = ""
    try:
        bars_h1 = pm_db.get_bars_from_db(symbol, "H1", limit=1)
        if bars_h1:
            cur_bar = bars_h1[0]
            cur_close = float(cur_bar["close"])
            cur_time = (cur_bar.get("bar_time") or "").strip()
            if tr_large > 0:
                retrace_pct = (cur_close - l_) / tr_large * 100.0
                retrace_pct = max(0.0, min(100.0, retrace_pct))
                retrace_line = (
                    f"• 현재가(H1 종가 {cur_time}): {cur_close:.2f}\n"
                    f"• TR 대비 되돌림: {retrace_pct:.1f}% (0%=저가 근처, 100%=고가 근처)"
                )
    except Exception:
        retrace_line = ""

    detail_lines = [
        "",
        "• 기준 장대음봉(2H):",
        f"  └ {base_time}  H={h:.2f} L={l_:.2f} C={c_large:.2f} TR={tr_large:.2f}",
    ]
    if retrace_line:
        detail_lines.append(retrace_line)

    body = "\n".join(header_lines + detail_lines)
    try:
        _send_telegram(body)
    except Exception:
        pass

def should_close_on_doji_upper_wick(rates: Any) -> tuple[bool, str, List[str]]:
    """
    20이평·4이평 위에서, 이전 봉은 양봉이었는데 직전 봉이 긴 윗꼬리를 가진 도지로 마감된 경우 청산.
    - 20이평 위, 4이평 위: 직전 마감 봉(-2) 종가 기준
    - 이전 봉(-3): 양봉(Close > Open)
    - 직전 봉(-2): 도지(몸통 비율 작음) + 윗꼬리가 범위의 50% 이상
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    if rates is None or len(rates) < 25:
        detail_lines.append("  • 도지 윗꼬리 청산: 봉 수 부족")
        return False, "", detail_lines

    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    last_close = float(rates["close"][-2])
    sma20 = sma_last(closes, SMA_PERIOD)
    sma4 = sma_last(closes, SMA4_PERIOD)
    if sma20 is None or sma4 is None:
        detail_lines.append("  • 도지 윗꼬리 청산: 20이평/4이평 계산 불가")
        return False, "", detail_lines

    above_sma20 = last_close > sma20
    above_sma4 = last_close > sma4
    zone_ok = above_sma20 and above_sma4

    # 이전 봉(-3) 양봉
    open_prev = float(rates["open"][-3])
    close_prev = float(rates["close"][-3])
    bullish_prev = close_prev > open_prev

    # 직전 봉(-2): 도지 + 긴 윗꼬리
    o2 = float(rates["open"][-2])
    c2 = float(rates["close"][-2])
    h2 = float(rates["high"][-2])
    l2 = float(rates["low"][-2])
    body2 = abs(c2 - o2)
    range2 = h2 - l2
    if range2 <= 0:
        detail_lines.append("  • 도지 윗꼬리: 직전 봉 범위 0 → 해당 없음")
        return False, "", detail_lines
    doji = (body2 / range2) <= DOJI_BODY_RATIO_MAX
    top = max(o2, c2)
    upper_wick = h2 - top
    long_upper_wick = upper_wick >= range2 * LONG_UPPER_WICK_RATIO_MIN

    trigger = zone_ok and bullish_prev and doji and long_upper_wick
    reason = "20이평·4이평 위 이전봉 양봉 후 직전봉 긴윗꼬리 도지" if trigger else ""

    detail_lines.append(
        f"  • 도지 윗꼬리: 20이평 위={above_sma20}, 4이평 위={above_sma4} | "
        f"이전봉 양봉={bullish_prev} | 직전봉 body/range={body2/range2:.2f}(도지={doji}), 윗꼬리/range={upper_wick/range2:.2f}(긴윗꼬리={long_upper_wick}) → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    return trigger, reason, detail_lines


def should_close_on_4ema_above_doji_or_long_upper_wick_rsi_below_ma(rates: Any) -> tuple[bool, str, List[str]]:
    """
    돌파더블비 매수 청산: 4이평이 20이평 위, 4이평 위에서 마감된 도지 또는 긴 윗꼬리 캔들, RSI <= RSI이동평균.
    - 4이평 > 20이평
    - 직전 봉(-2): 종가 > 4이평 (4이평 위에서 마감)
    - 직전 봉: 도지(body/range <= 0.2) OR 긴 윗꼬리(윗꼬리 > 몸통)
    - 직전 봉 시점 RSI(14) <= RSI(14)의 3봉 이동평균
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    min_bars = max(21, 14 + RSI_MA_PERIOD + 2)  # 20이평 + RSI·RSI_MA 계산에 필요한 봉 수
    if rates is None or len(rates) < min_bars:
        detail_lines.append("  • 4이평위 도지/긴윗꼬리+RSI 청산: 봉 수 부족")
        return False, "", detail_lines

    # 현재 봉 제외, 직전 마감 봉까지의 종가
    closes_ex_cur = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    last_close = closes_ex_cur[-1]
    sma4 = sma_last(closes_ex_cur, SMA4_PERIOD)
    sma20 = sma_last(closes_ex_cur, SMA_PERIOD)
    if sma4 is None or sma20 is None:
        detail_lines.append("  • 4이평위 도지/긴윗꼬리+RSI 청산: 4/20이평 계산 불가")
        return False, "", detail_lines

    # 조건 1: 4이평 > 20이평
    ema4_above_ema20 = sma4 > sma20
    if not ema4_above_ema20:
        detail_lines.append(
            f"  • 4이평위 도지/긴윗꼬리+RSI: 4이평 {sma4:.2f} <= 20이평 {sma20:.2f} → 해당 없음"
        )
        return False, "", detail_lines

    # 조건 2: 직전 봉 4이평 위에서 마감
    above_sma4 = last_close > sma4
    if not above_sma4:
        detail_lines.append(
            f"  • 4이평위 도지/긴윗꼬리+RSI: 직전봉 종가 {last_close:.2f} <= 4이평 {sma4:.2f} → 해당 없음"
        )
        return False, "", detail_lines

    # 직전 봉(-2): 도지 또는 긴 윗꼬리 (몸통보다 윗꼬리가 긴 캔들)
    o2 = float(rates["open"][-2])
    c2 = float(rates["close"][-2])
    h2 = float(rates["high"][-2])
    l2 = float(rates["low"][-2])
    body2 = abs(c2 - o2)
    range2 = h2 - l2
    top2 = max(o2, c2)
    upper_wick2 = h2 - top2
    if range2 <= 0:
        detail_lines.append("  • 4이평위 도지/긴윗꼬리+RSI: 직전 봉 범위 0 → 해당 없음")
        return False, "", detail_lines
    doji = (body2 / range2) <= DOJI_BODY_RATIO_MAX
    long_upper_wick = upper_wick2 > body2  # 몸통보다 윗꼬리가 긴 캔들
    candle_ok = doji or long_upper_wick
    if not candle_ok:
        detail_lines.append(
            f"  • 4이평위 도지/긴윗꼬리+RSI: 직전봉 도지={doji}, 윗꼬리>몸통={long_upper_wick} → 해당 없음"
        )
        return False, "", detail_lines

    # RSI(14) 및 RSI 3봉 이동평균 (직전 봉 시점)
    rsi_series = _rsi_series(closes_ex_cur, RSI_PERIOD)
    if rsi_series is None or len(rsi_series) != len(closes_ex_cur):
        detail_lines.append("  • 4이평위 도지/긴윗꼬리+RSI: RSI 계산 불가")
        return False, "", detail_lines
    # RSI 시리즈에서 직전 봉에 해당하는 값 = 마지막 유효 RSI
    rsi_last_val: Optional[float] = None
    for i in range(len(rsi_series) - 1, -1, -1):
        if rsi_series[i] is not None:
            rsi_last_val = rsi_series[i]
            break
    if rsi_last_val is None:
        detail_lines.append("  • 4이평위 도지/긴윗꼬리+RSI: 직전 봉 RSI 없음")
        return False, "", detail_lines
    # 직전 3개 RSI의 평균
    rsi_valid = [v for v in rsi_series if v is not None]
    if len(rsi_valid) < RSI_MA_PERIOD:
        detail_lines.append("  • 4이평위 도지/긴윗꼬리+RSI: RSI MA 계산 불가 (데이터 부족)")
        return False, "", detail_lines
    rsi_ma = sum(rsi_valid[-RSI_MA_PERIOD:]) / RSI_MA_PERIOD
    rsi_below_or_eq_ma = rsi_last_val <= rsi_ma
    trigger = rsi_below_or_eq_ma
    reason = "4이평 위 도지/긴윗꼬리 + RSI<=RSI이동평균" if trigger else ""
    detail_lines.append(
        f"  • 4이평위 도지/긴윗꼬리+RSI: 4>20={ema4_above_ema20}, 직전봉 4위마감={above_sma4}, "
        f"도지={doji} 윗꼬리>몸통={long_upper_wick} | RSI={rsi_last_val:.1f} RSI_MA={rsi_ma:.1f} → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    return trigger, reason, detail_lines


def should_close_on_sma20_120_failure(rates: Any) -> tuple[bool, str, List[str]]:
    """
    20이평·120이평을 돌파하지 못하고 종가가 두 이평선 아래에 머무르는 경우 청산.
    - 직전 마감 봉 종가(현재 봉 제외)가 20이평, 120이평 모두 아래이면 트리거.
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    if rates is None or len(rates) < 121:
        detail_lines.append("  • 20/120이평 돌파 실패 청산: 봉 수 부족")
        return False, "", detail_lines

    # 직전 마감 봉 기준: 현재 봉(-1) 제외한 종가로 20/120이평 계산
    closes_ex_cur = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    last_close = closes_ex_cur[-1]
    sma20 = sma_last(closes_ex_cur, SMA_PERIOD)
    sma120 = sma_last(closes_ex_cur, SMA_PERIOD_120)
    if sma20 is None or sma120 is None:
        detail_lines.append("  • 20/120이평 돌파 실패 청산: 20이평/120이평 계산 불가")
        return False, "", detail_lines

    below_20 = last_close < sma20
    below_120 = last_close < sma120
    trigger = below_20 and below_120

    detail_lines.append(
        f"  • 20/120이평 돌파 실패: 직전봉 Close {last_close:.2f} < 20이평 {sma20:.2f} = {below_20}, "
        f"< 120이평 {sma120:.2f} = {below_120} → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    reason = "20/120이평 돌파 실패 (종가 둘 다 아래)" if trigger else ""
    return trigger, reason, detail_lines


def get_20b_upper_from_rates(rates: Any) -> Optional[float]:
    """1H 봉 rates에서 BB(20,2) 상단 값 반환 (직전 봉들 종가 기준). 현재 봉 제외."""
    if rates is None or len(rates) < 21:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    return bollinger_upper(closes, BB_PERIOD_20, BB_STD_20)


def get_20b_upper_from_rates_including_current(rates: Any) -> Optional[float]:
    """
    BB(20,2) 상단 값 반환. 직전 19봉 + 현재 봉 종가 포함(최근 20봉). T/P 갱신용.

    MT5 copy_rates_from_pos() 경로: rates[0]이 과거(가장 오래된), rates[-1]이 현재(가장 최신).
    DB get_rates_from_db() 경로: bars를 최신→과거로 정렬 후 time_ts를 생성하므로 rates[0]이 현재, rates[-1]이 가장 오래된.
    두 경우 모두에서 "최근 20봉"을 올바르게 선택하도록 time 배열의 정렬 방향을 확인해 처리한다.
    """
    if rates is None or len(rates) < 20:
        return None
    try:
        t0 = int(rates["time"][0])
        t_end = int(rates["time"][-1])
    except Exception:
        return None

    closes_all = [float(c) for c in rates["close"]]
    if t0 < t_end:
        # 과거→현재 (MT5 직접 조회): 마지막 20개가 최근 20봉
        closes = closes_all[-20:]
    else:
        # 현재→과거 (DB 조회): 처음 20개가 최근 20봉
        closes = closes_all[:20]
    return bollinger_upper(closes, BB_PERIOD_20, BB_STD_20)


def get_20b_upper_from_db_or_rates(symbol: str, tf_str: str, rates: Any) -> Optional[float]:
    """20B(20,2) 상단: DB bars 테이블의 bb20_upper 우선 사용, 없거나 해당 심볼/TF가 아니면 rates로 계산."""
    if symbol in _DB_SYMBOLS and tf_str in _DB_TIMEFRAMES:
        bars = pm_db.get_bars_from_db(symbol, tf_str, limit=30)
        if bars and bars[0].get("bb20_upper") is not None:
            return float(bars[0]["bb20_upper"])
    return get_20b_upper_from_rates(rates)


def get_20b_upper_for_prev_bar(rates: Any) -> Optional[float]:
    """1H 봉 rates에서 직전 봉(-2) 시점의 BB(20,2) 상단. 직전 봉·현재 봉 종가 제외."""
    if rates is None or len(rates) < 22:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 2)]
    return bollinger_upper(closes, BB_PERIOD_20, BB_STD_20)


def get_4b_upper_from_rates(rates: Any) -> Optional[float]:
    """봉 rates에서 4B(4,4) 상단 값 반환 (직전 봉들 종가 기준). 현재 봉 제외."""
    if rates is None or len(rates) < 5:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    return bollinger_upper(closes, BB_PERIOD_4, BB_STD_4)


def get_20b_lower_from_rates(rates: Any) -> Optional[float]:
    """rates에서 직전 봉까지의 BB(20,2) 하단. 현재 봉 종가 제외."""
    if rates is None or len(rates) < 21:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    return bollinger_lower(closes, BB_PERIOD_20, BB_STD_20)


def get_20b_lower_for_prev_bar(rates: Any) -> Optional[float]:
    """rates에서 직전 봉(-2) 시점의 BB(20,2) 하단. 직전 봉·현재 봉 종가 제외."""
    if rates is None or len(rates) < 22:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 2)]
    return bollinger_lower(closes, BB_PERIOD_20, BB_STD_20)


def get_4b_lower_from_rates(rates: Any) -> Optional[float]:
    """rates에서 직전 봉까지의 BB(4,4) 하단. 현재 봉 종가 제외."""
    if rates is None or len(rates) < 5:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    return bollinger_lower(closes, BB_PERIOD_4, BB_STD_4)


def should_close_on_10m_20b_upper_wick_rejection(symbol: str) -> tuple[bool, str, List[str]]:
    """
    10분봉 20B 상단: 1개 봉이 돌파한 뒤, 다음 봉이 윗꼬리를 만들며 돌파에 실패한 경우 청산.
    - 이전 봉: Close >= 20B(20,2) 상단 (돌파)
    - 직전 봉: High > 20B 상단, Close < 20B 상단 (윗꼬리 되튐)
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    rates = get_rates_for_tf(symbol, "M10", count=25)
    if rates is None or len(rates) < 22:
        detail_lines.append("  • 10분 20B 윗꼬리: 10M 봉 수 부족")
        return False, "", detail_lines
    t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
    if t0 < t_end:
        idx_prev = len(rates) - 2
        idx_prev2 = len(rates) - 3
    else:
        idx_prev = 1
        idx_prev2 = 2
    # 20B(20,2) 상단: 해당 봉 포함 직전 20종가 (MT5: 과거→현재, DB: 최신→과거)
    if t0 < t_end:
        closes_prev2 = [float(rates["close"][i]) for i in range(idx_prev2 - 19, idx_prev2 + 1)]
        closes_prev = [float(rates["close"][i]) for i in range(idx_prev - 19, idx_prev + 1)]
    else:
        closes_prev2 = [float(rates["close"][i]) for i in range(idx_prev2, min(idx_prev2 + 20, len(rates)))]
        closes_prev = [float(rates["close"][i]) for i in range(idx_prev, min(idx_prev + 20, len(rates)))]
    if len(closes_prev2) < 20 or len(closes_prev) < 20:
        detail_lines.append("  • 10분 20B 윗꼬리: 20B 계산용 봉 부족")
        return False, "", detail_lines
    bb20_up_prev2 = bollinger_upper(closes_prev2, BB_PERIOD_20, BB_STD_20)
    bb20_up_prev = bollinger_upper(closes_prev, BB_PERIOD_20, BB_STD_20)
    if bb20_up_prev2 is None or bb20_up_prev is None:
        detail_lines.append("  • 10분 20B 윗꼬리: 20B 상단 계산 불가")
        return False, "", detail_lines
    close_prev2 = float(rates["close"][idx_prev2])
    high_prev = float(rates["high"][idx_prev])
    close_prev = float(rates["close"][idx_prev])
    broke_prev2 = close_prev2 >= bb20_up_prev2
    wick_reject_prev = high_prev > bb20_up_prev and close_prev < bb20_up_prev
    trigger = broke_prev2 and wick_reject_prev
    reason = "10분 20B 상단 1봉 돌파 후 다음 봉 윗꼬리 되튐" if trigger else ""
    detail_lines.append(
        f"  • 10분 20B 윗꼬리: 이전봉 Close {close_prev2:.2f} >= 20B상단 {bb20_up_prev2:.2f} = {broke_prev2} | "
        f"직전봉 High {high_prev:.2f} > 20B {bb20_up_prev:.2f}, Close {close_prev:.2f} < 20B = {wick_reject_prev} → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    return trigger, reason, detail_lines


def should_close_on_20b_upper_wick_rejection(rates: Any) -> tuple[bool, str, List[str]]:
    """
    진입 TF 봉 기준 20B(20,2) 상단: 1개 봉이 돌파한 뒤, 다음 봉이 윗꼬리를 만들며 되튐한 경우 청산.
    - 이전 봉: Close >= 20B 상단 (돌파)
    - 직전 봉: High > 20B 상단, Close < 20B 상단 (윗꼬리 되튐)
    rates는 어떤 타임프레임이든 가능(진입 TF 데이터 전달).
    returns: (청산 여부, 사유 문자열, 점검 상세 로그 목록)
    """
    detail_lines: List[str] = []
    if rates is None or len(rates) < 22:
        detail_lines.append("  • 20B 윗꼬리: 봉 수 부족")
        return False, "", detail_lines
    t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
    if t0 < t_end:
        idx_prev = len(rates) - 2
        idx_prev2 = len(rates) - 3
    else:
        idx_prev = 1
        idx_prev2 = 2
    if t0 < t_end:
        closes_prev2 = [float(rates["close"][i]) for i in range(idx_prev2 - 19, idx_prev2 + 1)]
        closes_prev = [float(rates["close"][i]) for i in range(idx_prev - 19, idx_prev + 1)]
    else:
        closes_prev2 = [float(rates["close"][i]) for i in range(idx_prev2, min(idx_prev2 + 20, len(rates)))]
        closes_prev = [float(rates["close"][i]) for i in range(idx_prev, min(idx_prev + 20, len(rates)))]
    if len(closes_prev2) < 20 or len(closes_prev) < 20:
        detail_lines.append("  • 20B 윗꼬리: 20B 계산용 봉 부족")
        return False, "", detail_lines
    bb20_up_prev2 = bollinger_upper(closes_prev2, BB_PERIOD_20, BB_STD_20)
    bb20_up_prev = bollinger_upper(closes_prev, BB_PERIOD_20, BB_STD_20)
    if bb20_up_prev2 is None or bb20_up_prev is None:
        detail_lines.append("  • 20B 윗꼬리: 20B 상단 계산 불가")
        return False, "", detail_lines
    close_prev2 = float(rates["close"][idx_prev2])
    high_prev = float(rates["high"][idx_prev])
    close_prev = float(rates["close"][idx_prev])
    broke_prev2 = close_prev2 >= bb20_up_prev2
    wick_reject_prev = high_prev > bb20_up_prev and close_prev < bb20_up_prev
    trigger = broke_prev2 and wick_reject_prev
    reason = "20B 상단 1봉 돌파 후 다음 봉 윗꼬리 되튐" if trigger else ""
    detail_lines.append(
        f"  • 20B 윗꼬리: 이전봉 Close {close_prev2:.2f} >= 20B상단 {bb20_up_prev2:.2f} = {broke_prev2} | "
        f"직전봉 High {high_prev:.2f} > 20B {bb20_up_prev:.2f}, Close {close_prev:.2f} < 20B = {wick_reject_prev} → "
        f"{'해당사항 있음 (1)' if trigger else '해당사항 없음 (0)'}"
    )
    return trigger, reason, detail_lines


def get_20b_upper_and_std_from_rates(rates: Any) -> Optional[Tuple[float, float]]:
    """1H 봉 rates에서 BB(20,2) 상단과 표준편차 반환."""
    if rates is None or len(rates) < 21:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    return bollinger_upper_and_std(closes, BB_PERIOD_20, BB_STD_20)


def get_20b_bands_from_rates(rates: Any) -> Optional[Tuple[float, float]]:
    """1H 봉 rates에서 BB(20,2) 상단·하단 반환. (upper, lower). 현재 봉 제외."""
    if rates is None or len(rates) < 21:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    u = bollinger_upper(closes, BB_PERIOD_20, BB_STD_20)
    if u is None:
        return None
    use = closes[-BB_PERIOD_20:]
    mid = sum(use) / BB_PERIOD_20
    variance = sum((x - mid) ** 2 for x in use) / (BB_PERIOD_20 - 1)
    std = variance ** 0.5 if variance > 0 else 0.0
    lower = mid - BB_STD_20 * std
    return (u, lower)


def get_4b_bands_from_rates(rates: Any) -> Optional[Tuple[float, float]]:
    """rates에서 BB(4,4) 상단·하단 반환. (upper, lower). 현재 봉 제외."""
    if rates is None or len(rates) < 6:
        return None
    closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    u = bollinger_upper(closes, BB_PERIOD_4, BB_STD_4)
    if u is None:
        return None
    use = closes[-BB_PERIOD_4:]
    mid = sum(use) / BB_PERIOD_4
    variance = sum((x - mid) ** 2 for x in use) / (BB_PERIOD_4 - 1)
    std = variance ** 0.5 if variance > 0 else 0.0
    lower = mid - BB_STD_4 * std
    return (u, lower)


def _get_contract_size(symbol: str) -> float:
    """MT5 심볼의 계약 규모. order_calc_margin 실패 시 fallback용."""
    if not mt5.symbol_select(symbol, True):
        return 1.0
    info = mt5.symbol_info(symbol)
    if info is None:
        return 1.0
    size = getattr(info, "trade_contract_size", None)
    if size is not None and size > 0:
        return float(size)
    if symbol == "XAUUSD":
        return 100.0
    if "NAS" in symbol:
        return 1.0
    return 1.0


def _position_margin(pos: Any) -> float:
    """포지션 증거금(마진). MT5 order_calc_margin 사용 → 수익률 = 수익금/마진*100 (예: 마진200, 수익100 → 50%)."""
    if mt5.symbol_select(pos.symbol, True):
        margin = mt5.order_calc_margin(pos.type, pos.symbol, pos.volume, pos.price_open)
        if margin is not None and margin > 0:
            return float(margin)
    contract = _get_contract_size(pos.symbol)
    return pos.price_open * pos.volume * contract


def _pending_order_margin(order: Any) -> float:
    """미체결 주문(예약오더) 1건의 예상 증거금."""
    vol = getattr(order, "volume_current", None) or getattr(order, "volume_initial", 0) or 0
    price = float(getattr(order, "price_open", 0) or 0)
    if not vol or not price:
        return 0.0
    if mt5.symbol_select(order.symbol, True):
        margin = mt5.order_calc_margin(order.type, order.symbol, vol, price)
        if margin is not None and margin > 0:
            return float(margin)
    contract = _get_contract_size(order.symbol)
    return price * vol * contract


def _position_roi(pos: Any) -> float:
    """포지션 수익률(%) = (손익+스왑) / 마진 * 100. 마진 200, 수익 100 이면 50%."""
    margin = _position_margin(pos)
    return (pos.profit + pos.swap) / margin * 100 if margin > 0 else 0.0


def _position_has_tp(pos: Any) -> bool:
    """포지션에 T/P(테이크 프로핏)가 설정되어 있으면 True. MT5에서 미설정 시 tp는 0."""
    tp = getattr(pos, "tp", 0)
    return tp is not None and float(tp) != 0.0


def _position_comment_tf(pos: Any) -> str:
    """포지션 comment에서 진입 타임프레임(1H/5M/10M 등) 추출. 없으면 1H."""
    comment = getattr(pos, "comment", "") or ""
    parsed = _parse_comment(comment)
    return (parsed[3] if parsed and len(parsed) > 3 else "").strip().upper() or "1H"


def get_session_ref_and_closes(
    symbol: str,
    ref_date: Any,
    ref_hour: int,
    window_date: Any,
    window_start_hour: int,
    window_end_hour: int,
    now_ts: float,
) -> Optional[Tuple[float, float, List[float]]]:
    """
    기준일 ref_date의 ref_hour 봉 High/Low와, window_date의 window_start_hour~window_end_hour 구간
    이미 종료된 1H 봉들의 Close 목록 반환. (미국 세션: ref_date=전날, window_date=오늘)
    returns: (ref_high, ref_low, closes) or None
    """
    if not mt5.symbol_select(symbol, True):
        return None
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_H1, 0, 40)
    if rates is None or len(rates) < 2:
        return None

    ref_high = ref_low = None
    closes: List[float] = []

    for i in range(len(rates)):
        bar_ts = int(rates["time"][i])
        bar_ts_corrected = bar_ts + MT5_SESSION_OFFSET_SEC
        bar_dt = datetime.fromtimestamp(bar_ts_corrected, tz=KST)
        bar_date = bar_dt.date()
        h = bar_dt.hour
        bar_close = float(rates["close"][i])
        bar_high = float(rates["high"][i])
        bar_low = float(rates["low"][i])
        # 기준 봉
        if bar_date == ref_date and h == ref_hour:
            ref_high = bar_high
            ref_low = bar_low
            continue
        # 구간 봉 (이미 종료된 것만)
        if bar_date != window_date:
            continue
        if window_start_hour <= window_end_hour:
            in_window = window_start_hour <= h <= window_end_hour
        else:
            # 0~7 구간: 0,1,...,7
            in_window = h >= window_start_hour or h <= window_end_hour
        if in_window and bar_ts_corrected + 3600 <= now_ts:
            closes.append(bar_close)

    if ref_high is None or ref_low is None:
        return None
    return (ref_high, ref_low, closes)


def get_asia_session_ref_and_closes(symbol: str) -> Optional[Tuple[float, float, List[float]]]:
    """오늘 8~9시 봉 기준, 9~17시 구간 Close. (get_session_ref_and_closes 래퍼)"""
    now_kst = datetime.now(KST)
    today = now_kst.date()
    return get_session_ref_and_closes(
        symbol, today, ASIA_REF_HOUR, today, ASIA_WINDOW_START, ASIA_WINDOW_END, now_kst.timestamp()
    )


def close_positions_by_side(symbol: str, is_long: bool) -> List[dict]:
    """해당 심볼의 Long(BUY) 또는 Short(SELL) 포지션만 청산. 2시간 제한 없음."""
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    to_close = [p for p in positions if (p.type == mt5.ORDER_TYPE_BUY) == is_long]
    closed = []
    for pos in to_close:
        ok, msg = tr.close_market_order(symbol=symbol, ticket=pos.ticket, volume=pos.volume)
        if ok:
            closed.append({
                "symbol": symbol,
                "ticket": pos.ticket,
                "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                "volume": pos.volume,
                "profit": pos.profit + pos.swap,
            })
        else:
            print(f"❌ 청산 실패 {symbol} #{pos.ticket}: {msg}")
    if closed:
        n, errs = tr.cancel_pending_orders(symbol)
        if n:
            print(f"  [예약 취소] {symbol} 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {symbol}: {e}")
    return closed


def _comment_tf_to_rates_tf(timeframe_from_comment: str) -> str:
    """comment의 timeframe(1H/5M/10M/2H/4H) → get_rates_for_tf용 문자열(H1/M5/M10/H2/H4)."""
    t = (timeframe_from_comment or "").strip().upper()
    if t == "1H":
        return "H1"
    if t == "2H":
        return "H2"
    if t == "4H":
        return "H4"
    if t in ("5M", "10M"):
        return "M5" if t == "5M" else "M10"
    return "H1"


def close_all_positions_for_symbol_by_timeframe(symbol: str, timeframe_from_comment: str) -> List[dict]:
    """해당 심볼 중 comment의 timeframe이 일치하는 포지션만 청산. timeframe_from_comment: 1H/5M/10M. 매수 포지션만 대상."""
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    positions = [p for p in positions if p.type == mt5.ORDER_TYPE_BUY]
    if not positions:
        return []
    target_tf = (timeframe_from_comment or "").strip().upper()
    closed = []
    for pos in positions:
        comment = getattr(pos, "comment", "") or ""
        parsed = _parse_comment(comment)
        pos_tf = (parsed[3] if parsed and len(parsed) > 3 else "") or ""
        pos_tf = pos_tf.strip().upper()
        if pos_tf != target_tf:
            continue
        ok, msg = tr.close_market_order(symbol=symbol, ticket=pos.ticket, volume=pos.volume)
        if ok:
            closed.append({
                "symbol": symbol,
                "ticket": pos.ticket,
                "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                "volume": pos.volume,
                "profit": pos.profit + pos.swap,
            })
        else:
            print(f"❌ 청산 실패 {symbol} #{pos.ticket}: {msg}")
    if closed:
        n, errs = tr.cancel_pending_orders(symbol)
        if n:
            print(f"  [예약 취소] {symbol} 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {symbol}: {e}")
    return closed


def close_positions_for_symbol_by_timeframe_sell(symbol: str, timeframe_from_comment: str) -> List[dict]:
    """해당 심볼 중 comment의 timeframe이 일치하는 매도(SELL) 포지션만 청산. 20B 하단 터치 청산용."""
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    positions = [p for p in positions if p.type == mt5.ORDER_TYPE_SELL]
    if not positions:
        return []
    target_tf = (timeframe_from_comment or "").strip().upper()
    closed = []
    for pos in positions:
        comment = getattr(pos, "comment", "") or ""
        parsed = _parse_comment(comment)
        pos_tf = (parsed[3] if parsed and len(parsed) > 3 else "") or ""
        pos_tf = pos_tf.strip().upper()
        if pos_tf != target_tf:
            continue
        ok, msg = tr.close_market_order(symbol=symbol, ticket=pos.ticket, volume=pos.volume)
        if ok:
            closed.append({
                "symbol": symbol,
                "ticket": pos.ticket,
                "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                "volume": pos.volume,
                "profit": pos.profit + pos.swap,
            })
        else:
            print(f"❌ 청산 실패 {symbol} #{pos.ticket}: {msg}")
    if closed:
        n, errs = tr.cancel_pending_orders(symbol)
        if n:
            print(f"  [예약 취소] {symbol} 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {symbol}: {e}")
    return closed


def close_positions_for_symbol_by_timeframe_has_tp(
    symbol: str, timeframe_from_comment: str
) -> List[dict]:
    """해당 심볼 중 comment의 timeframe이 일치하고 T/P가 설정된 포지션만 청산 (20이평 하단 터치 청산용). 매수 포지션만 대상."""
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    positions = [p for p in positions if p.type == mt5.ORDER_TYPE_BUY]
    if not positions:
        return []
    target_tf = (timeframe_from_comment or "").strip().upper()
    closed = []
    for pos in positions:
        if not _position_has_tp(pos):
            continue
        comment = getattr(pos, "comment", "") or ""
        parsed = _parse_comment(comment)
        pos_tf = (parsed[3] if parsed and len(parsed) > 3 else "") or ""
        pos_tf = pos_tf.strip().upper()
        if pos_tf != target_tf:
            continue
        ok, msg = tr.close_market_order(symbol=symbol, ticket=pos.ticket, volume=pos.volume)
        if ok:
            closed.append({
                "symbol": symbol,
                "ticket": pos.ticket,
                "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                "volume": pos.volume,
                "profit": pos.profit + pos.swap,
            })
        else:
            print(f"❌ 청산 실패 {symbol} #{pos.ticket}: {msg}")
    if closed:
        n, errs = tr.cancel_pending_orders(symbol)
        if n:
            print(f"  [예약 취소] {symbol} 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {symbol}: {e}")
    return closed


def close_all_positions_for_symbol_unconditional(symbol: str) -> List[dict]:
    """해당 심볼의 모든 포지션 청산 (2시간/레벨 조건 없음)."""
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    closed = []
    for pos in positions:
        ok, msg = tr.close_market_order(symbol=symbol, ticket=pos.ticket, volume=pos.volume)
        if ok:
            closed.append({
                "symbol": symbol,
                "ticket": pos.ticket,
                "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                "volume": pos.volume,
                "profit": pos.profit + pos.swap,
            })
        else:
            print(f"❌ 청산 실패 {symbol} #{pos.ticket}: {msg}")
    if closed:
        n, errs = tr.cancel_pending_orders(symbol)
        if n:
            print(f"  [예약 취소] {symbol} 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {symbol}: {e}")
    return closed


def close_all_if_overall_loss_rate_below(threshold: float = OVERALL_LOSS_RATE_STOP_THRESHOLD) -> List[Tuple[str, List[dict], float]]:
    """balance(잔고) 대비 손실금액 손실률이 threshold 이하이면 전 포지션 청산. 손실율 = (전체 손익합계)/balance*100. 반환: [(symbol, closed_list, loss_rate_pct), ...]"""
    positions = mt5.positions_get()
    if not positions:
        return []
    acc = tr.get_account_info()
    if acc is None:
        print(f"  • 계정 정보 조회 실패 → 손실율 청산 스킵")
        return []
    balance = float(acc.get("balance", 0) or 0)
    if balance <= 0:
        print(f"  • 잔고 0 이하 → 스킵 (balance={balance:.2f})")
        return []
    total_profit = sum(p.profit + p.swap for p in positions)
    loss_rate_pct = total_profit / balance * 100
    print(f"[손실율] balance 대비 손실률: {loss_rate_pct:.2f}% (잔고 {balance:.0f}, 손익 {total_profit:+.2f}, 청산 기준: ≤{threshold}%)", end="")
    if loss_rate_pct > threshold:
        print(" → 청산 안 함")
        return []
    print(" → 전 포지션 청산 실행")
    by_symbol: dict = {}
    for pos in positions:
        sym = pos.symbol
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(pos)
    result: List[Tuple[str, List[dict], float]] = []
    for symbol in by_symbol:
        closed = close_all_positions_for_symbol_unconditional(symbol)
        if closed:
            result.append((symbol, closed, loss_rate_pct))
            print(f"✅ 손절 청산: {symbol} ({len(closed)}건)")
    if result:
        n, errs = tr.cancel_pending_orders()
        if n:
            print(f"  [예약 취소] 전 심볼 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {e}")
    return result


def close_all_if_margin_level_below(threshold: float = MARGIN_LEVEL_CLOSE_PCT) -> List[Tuple[str, List[dict], float]]:
    """계정 마진레벨(equity/증거금*100)이 threshold 이하이면 전 포지션 청산. 반환: [(symbol, closed_list, margin_level_pct), ...]"""
    positions = mt5.positions_get()
    if not positions:
        return []
    acc = tr.get_account_info()
    if acc is None:
        print(f"  • 계정 정보 조회 실패 → 마진레벨 청산 스킵")
        return []
    margin_level = acc.get("margin_level")
    if margin_level is None or (isinstance(margin_level, (int, float)) and margin_level <= 0):
        print(f"  [마진레벨] 포지션 없거나 마진 0 → 스킵")
        return []
    margin_level_pct = float(margin_level)
    print(f"[마진레벨] {margin_level_pct:.1f}% (청산 기준: ≤{threshold}%)", end="")
    if margin_level_pct > threshold:
        print(" → 청산 안 함")
        return []
    print(" → 전 포지션 청산 실행")
    by_symbol: dict = {}
    for pos in positions:
        sym = pos.symbol
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(pos)
    result: List[Tuple[str, List[dict], float]] = []
    for symbol in by_symbol:
        closed = close_all_positions_for_symbol_unconditional(symbol)
        if closed:
            result.append((symbol, closed, margin_level_pct))
            print(f"✅ 마진레벨 청산: {symbol} ({len(closed)}건)")
    if result:
        n, errs = tr.cancel_pending_orders()
        if n:
            print(f"  [예약 취소] 전 심볼 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {e}")
    return result


def close_last_position_if_margin_over_pct(threshold_pct: float = MARGIN_PCT_CLOSE_LAST_ORDER) -> Optional[Tuple[List[dict], float]]:
    """잔액(balance) 대비 마진이 threshold_pct(%)를 초과하면 가장 나중에 만들어진 포지션 1건만 청산.
    반환: (청산된 포지션 정보 목록 1건, 마진비율%) 또는 조건 미충족/실패 시 None."""
    positions = mt5.positions_get()
    if not positions:
        return None
    acc = tr.get_account_info()
    if acc is None:
        return None
    balance = float(acc.get("balance", 0) or 0)
    if balance <= 0:
        return None
    margin = float(acc.get("margin", 0) or 0)
    margin_ratio_pct = (margin / balance * 100.0) if balance > 0 else 0.0
    if margin_ratio_pct <= threshold_pct:
        return None
    # 가장 나중에 만들어진 포지션 1건 (time 또는 time_msc 기준 최대)
    latest = max(
        positions,
        key=lambda p: (getattr(p, "time_msc", 0) or 0) or (getattr(p, "time", 0) or 0) * 1000,
    )
    ok, msg = tr.close_market_order(symbol=latest.symbol, ticket=latest.ticket, volume=latest.volume)
    if not ok:
        print(f"  [마진7%%초과 청산] 최신 포지션 #{latest.ticket} {latest.symbol} 청산 실패: {msg}", flush=True)
        return None
    closed_list = [{
        "symbol": latest.symbol,
        "ticket": latest.ticket,
        "type": "BUY" if latest.type == mt5.ORDER_TYPE_BUY else "SELL",
        "volume": latest.volume,
        "profit": latest.profit + latest.swap,
    }]
    print(f"[마진7%%초과] 잔액 대비 마진 {margin_ratio_pct:.1f}% > {threshold_pct}% → 최신 포지션 1건 청산: {latest.symbol} #{latest.ticket}", flush=True)
    return (closed_list, margin_ratio_pct)


def _cancel_pending_orders_interval_less_than_5m_ktr() -> None:
    """예약 오더(KTR 매직)를 점검해, 인접 예약 간격이 5분봉 KTR(또는 KTR<10이면 2*KTR)보다 작으면 해당 예약 오더 삭제."""
    orders = mt5.orders_get() or []
    ktr_pending = [o for o in orders if getattr(o, "magic", 0) == MAGIC_KTR]
    if not ktr_pending:
        return
    ot_buy_limit = getattr(mt5, "ORDER_TYPE_BUY_LIMIT", 2)
    ot_sell_limit = getattr(mt5, "ORDER_TYPE_SELL_LIMIT", 3)
    limit_orders = [o for o in ktr_pending if getattr(o, "type", -1) in (ot_buy_limit, ot_sell_limit)]
    if not limit_orders:
        return
    symbols = list({o.symbol for o in limit_orders})
    to_cancel: List[int] = []
    for symbol in symbols:
        ktr_value, _ = get_ktr_from_db_auto(symbol, "5M")
        if not ktr_value or ktr_value <= 0:
            continue
        min_interval = (2.0 * ktr_value) if ktr_value < 10 else float(ktr_value)
        buy_orders = [o for o in limit_orders if o.symbol == symbol and o.type == ot_buy_limit]
        sell_orders = [o for o in limit_orders if o.symbol == symbol and o.type == ot_sell_limit]
        for order_list, descending in [(buy_orders, True), (sell_orders, False)]:
            if len(order_list) < 2:
                continue
            order_list = sorted(order_list, key=lambda o: float(getattr(o, "price_open", 0) or getattr(o, "price_current", 0)), reverse=descending)
            for i in range(len(order_list) - 1):
                p1 = float(getattr(order_list[i], "price_open", 0) or getattr(order_list[i], "price_current", 0))
                p2 = float(getattr(order_list[i + 1], "price_open", 0) or getattr(order_list[i + 1], "price_current", 0))
                gap = abs(p1 - p2)
                if gap < min_interval:
                    to_cancel.append(order_list[i + 1].ticket)
    if not to_cancel:
        return
    cancelled = 0
    for ticket in to_cancel:
        result = mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": ticket})
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            cancelled += 1
        else:
            print(f"  [예약 간격 삭제] #{ticket} 취소 실패: {getattr(result, 'comment', '')}", flush=True)
    if cancelled:
        print(f"  [예약 간격] 5M KTR 기준 간격 미달 예약 오더 {cancelled}건 삭제", flush=True)


def close_all_and_reenter_if_profit_over_pct(threshold_pct: float = PROFIT_TAKE_PCT) -> Optional[Tuple[List[dict], float, List[dict], List[Tuple[str, str, float, str]]]]:
    """수익금이 잔액의 threshold_pct(%)를 초과하면 전 포지션 청산 후, 청산한 포지션과 동일한 심볼·방향·랏수로 시장가 재진입.
    반환: (청산 목록, 수익률%, 재진입 성공 목록, 재진입 실패 목록 [(symbol, side, volume, err)]) 또는 조건 미충족 시 None."""
    positions = mt5.positions_get()
    if not positions:
        return None
    acc = tr.get_account_info()
    if acc is None:
        return None
    balance = float(acc.get("balance", 0) or 0)
    if balance <= 0:
        return None
    total_profit = sum(p.profit + p.swap for p in positions)
    profit_rate_pct = total_profit / balance * 100
    if profit_rate_pct <= threshold_pct:
        return None
    # 청산 전에 재진입할 (심볼, 방향, 랏수 합계) 집계
    reentry_plan: dict = {}  # (symbol, "BUY"|"SELL") -> volume sum
    for p in positions:
        sym = p.symbol
        side = "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL"
        key = (sym, side)
        reentry_plan[key] = reentry_plan.get(key, 0.0) + p.volume

    # 청산·취소 전에 대기 중인 KTR 예약 정보 수집 (재진입 후 동일 구조로 복원용)
    ktr_restore: dict = {}  # (symbol, side) -> (step_ktr, [(volume, order_index_1based), ...])
    orders_before = mt5.orders_get() or []
    ktr_pending_before = [o for o in orders_before if getattr(o, "magic", 0) == MAGIC_KTR]
    for (symbol, side) in reentry_plan.keys():
        pos_same = [p for p in positions if p.symbol == symbol and (side == "BUY" and p.type == mt5.ORDER_TYPE_BUY or side == "SELL" and p.type == mt5.ORDER_TYPE_SELL)]
        if not pos_same:
            continue
        entry_old = sum(p.price_open for p in pos_same) / len(pos_same)
        is_buy = side == "BUY"
        ot_limit = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT
        same_orders = [o for o in ktr_pending_before if o.symbol == symbol and getattr(o, "type", 0) == ot_limit]
        if not same_orders:
            continue
        # 가격 순: 매수는 진입가에 가까운 것(가격 높은 것) 먼저, 매도는 진입가에 가까운 것(가격 낮은 것) 먼저
        same_orders.sort(key=lambda o: getattr(o, "price_open", 0) or getattr(o, "price_current", 0), reverse=is_buy)
        first_price = getattr(same_orders[0], "price_open", None) or getattr(same_orders[0], "price_current", 0)
        if first_price is None:
            continue
        step = (entry_old - float(first_price)) if is_buy else (float(first_price) - entry_old)
        if step <= 0:
            continue
        order_list = [(getattr(o, "volume_current", 0) or getattr(o, "volume", 0), i + 1) for i, o in enumerate(same_orders)]
        ktr_restore[(symbol, side)] = (step, order_list)

    # 전 포지션 청산
    closed_all: List[dict] = []
    symbols = list({p.symbol for p in positions})
    for symbol in symbols:
        closed = close_all_positions_for_symbol_unconditional(symbol)
        closed_all.extend(closed)
    if not closed_all:
        return None
    n, errs = tr.cancel_pending_orders()
    if n:
        print(f"  [수익실현 청산] 예약 취소 {n}건")
    for e in errs:
        print(f"  [예약 취소 실패] {e}")
    # 동일 규모 시장가 재진입
    reentry_ok: List[dict] = []
    reentry_fail: List[Tuple[str, str, float, str]] = []
    for (symbol, side), volume in reentry_plan.items():
        if volume <= 0:
            continue
        vol_rounded = round(float(volume), 2)
        if vol_rounded <= 0:
            continue
        ok, msg = tr.execute_market_order(symbol, side, vol_rounded, magic=MAGIC_KTR, comment="ProfitTakeReentry")
        if ok:
            reentry_ok.append({"symbol": symbol, "type": side, "volume": vol_rounded})
            print(f"  [수익실현 재진입] {symbol} {side} {vol_rounded}랏 성공")
        else:
            reentry_fail.append((symbol, side, vol_rounded, msg or "실패"))
            print(f"  ❌ [수익실현 재진입 실패] {symbol} {side} {vol_rounded}랏: {msg}")

    # 재진입 성공한 (symbol, side) 중 대기했던 KTR 예약이 있으면 재진입가 기준으로 KTR 예약 재생성
    for rec in reentry_ok:
        symbol = rec.get("symbol")
        side = rec.get("type")
        if not symbol or not side:
            continue
        key = (symbol, side)
        if key not in ktr_restore:
            continue
        step, order_list = ktr_restore[key]
        new_pos = mt5.positions_get(symbol=symbol) or []
        new_pos = [p for p in new_pos if (side == "BUY" and p.type == mt5.ORDER_TYPE_BUY) or (side == "SELL" and p.type == mt5.ORDER_TYPE_SELL)]
        if not new_pos:
            continue
        new_entry = sum(p.price_open for p in new_pos) / len(new_pos)
        is_buy = side == "BUY"
        num_positions = len(order_list) + 1
        placed = 0
        for vol, idx in order_list:
            if vol <= 0:
                continue
            limit_price = (new_entry - step * idx) if is_buy else (new_entry + step * idx)
            mult_j = num_positions - idx - 0.5
            sl = (limit_price - mult_j * step) if is_buy else (limit_price + mult_j * step)
            comment = f"PTKTR{idx + 1}"[:31]
            ok, msg = tr.place_pending_limit(symbol, side, vol, limit_price, sl=sl, tp=0.0, magic=MAGIC_KTR, comment=comment)
            if ok:
                placed += 1
                print(f"  [수익실현 KTR 복원] {symbol} {side} {idx + 1}차 예약 {vol}랏 @ {limit_price:.2f}")
            else:
                print(f"  [수익실현 KTR 복원 실패] {symbol} {side} {idx + 1}차: {msg}")
            if len(order_list) > 1:
                time.sleep(0.4)
        if placed:
            print(f"  [수익실현 KTR 복원] {symbol} {side} 예약 {placed}건 생성 (재진입가 {new_entry:.2f} 기준)", flush=True)

    return (closed_all, profit_rate_pct, reentry_ok, reentry_fail)


def close_all_positions_weekday_2325() -> List[dict]:
    """월~금 23:25(KST) 전 포지션 강제 청산. 반환: 청산된 목록."""
    positions = mt5.positions_get()
    if not positions:
        return []
    closed_list: List[dict] = []
    symbols = list({p.symbol for p in positions})
    for symbol in symbols:
        for c in close_all_positions_for_symbol_unconditional(symbol):
            closed_list.append(c)
    if closed_list:
        n, errs = tr.cancel_pending_orders()
        if n:
            print(f"  [예약 취소] 23:25 청산 후 미체결 주문 {n}건 취소")
        for sym in {c["symbol"] for c in closed_list}:
            _remove_reservations_for_symbol(sym)
    return closed_list


def run_weekday_2325_close_if_needed(now_kst: datetime) -> bool:
    """월~금 23:25~23:29 구간에서 하루 1회만 전 포지션 강제 청산. 프로세스 간 중복 방지를 위해 날짜별 마커 파일 사용. 실행했으면 True."""
    global _last_2325_close_date
    if now_kst.weekday() > 4:  # 5=토, 6=일
        return False
    if now_kst.hour != WEEKDAY_2325_CLOSE_HOUR:
        return False
    if not (WEEKDAY_2325_CLOSE_MIN_START <= now_kst.minute <= WEEKDAY_2325_CLOSE_MIN_END):
        return False
    today_str = now_kst.strftime("%Y-%m-%d")
    # 프로세스 간 중복 방지: 오늘 날짜 마커 파일을 선점한 프로세스만 실행 (모니터 인스턴스가 여러 개여도 1회만 실행)
    marker_path = _path_2325_done_marker(today_str)
    try:
        fd = os.open(marker_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        return False
    _last_2325_close_date = today_str

    print(f"  [23:25] 월~금 전 포지션 강제 청산 실행", flush=True)
    closed_list = close_all_positions_weekday_2325()
    if closed_list:
        _log_closes_to_file("23:25_전량", closed_list)

    lines = [
        f"🕐 **23:25 전 포지션 강제 청산**",
        f"• 시각: {now_kst.strftime('%Y-%m-%d %H:%M')} KST",
        "",
    ]
    if closed_list:
        total_profit = sum(c["profit"] for c in closed_list)
        lines.append(f"**청산 {len(closed_list)}건**")
        for c in closed_list:
            lines.append(f"  └ {c['symbol']} #{c['ticket']} {c['type']} {c['volume']}랏 → ${c['profit']:+,.2f}")
        lines.append(f"• 합계: ${total_profit:+,.2f}")
    else:
        lines.append("• 청산할 포지션 없음")
    try:
        _send_telegram("\n".join(lines))
    except Exception as e:
        print(f"  ⚠️ 23:25 청산 결과 텔레그램 전송 실패: {e}")
    return True


def _position_open_time_kst(pos: Any) -> Optional[datetime]:
    """포지션 오픈 시각을 KST datetime으로 반환. pos.time이 초 또는 밀리초일 수 있음."""
    pt = int(getattr(pos, "time", 0) or 0)
    if pt <= 0:
        return None
    if pt > 1e10:
        pt = pt // 1000
    return mt5_ts_to_kst(pt)


def close_manual_orders_outside_allowed_time() -> bool:
    """(비활성화) 수작업 오더 진입 시간 제한 삭제됨 — 항상 청산하지 않음."""
    return False


def close_all_positions_for_symbol(symbol: str, timeframe_from_comment: Optional[str] = None) -> List[dict]:
    """[현재 미호출] should_close_on_* 등 복잡 청산 조건으로 호출되던 함수. run_one_check()는 20B 상단/하단 터치만 사용.
    해당 심볼의 포지션 중 오픈 후 2시간 이상 지났고, T/P가 없는 것만 청산.
    timeframe_from_comment가 주어지면 해당 진입 TF(1H/5M/10M 등)와 일치하는 포지션만 대상.
    청산 조건은 매수(Long) 포지션에만 적용, 매도(Short) 포지션은 대상에서 제외."""
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    positions = [p for p in positions if p.type == mt5.ORDER_TYPE_BUY]
    if not positions:
        return []
    if timeframe_from_comment is not None:
        target_tf = timeframe_from_comment.strip().upper()
        positions = [p for p in positions if _position_comment_tf(p) == target_tf]
        if not positions:
            return []
    now_sec = int(datetime.now().timestamp())
    # MT5 time이 밀리초일 수 있음
    def _age_sec(p: Any) -> float:
        pt = int(p.time)
        if pt > 1e10:
            return (now_sec * 1000 - pt) / 1000.0
        return float(now_sec - pt)
    # 2시간 이상 + T/P 미설정인 포지션만 청산 (T/P 있으면 청산 제외)
    to_close = [p for p in positions if _age_sec(p) >= MIN_AGE_SEC and not _position_has_tp(p)]
    tp_count = sum(1 for p in positions if _position_has_tp(p))
    if tp_count:
        tf_label = timeframe_from_comment or "해당"
        print(f"⚠️ [{symbol}] {tf_label} T/P 설정 포지션 {tp_count}건은 레벨 청산 대상에서 제외")
    if not to_close and positions:
        skip_count = len(positions)
        print(f"⚠️ [{symbol}] 청산 조건 충족했으나 포지션 {skip_count}건 모두 2시간 미만 또는 T/P 설정 → 청산 생략")
    closed = []
    for pos in to_close:
        ok, msg = tr.close_market_order(
            symbol=symbol,
            ticket=pos.ticket,
            volume=pos.volume,
        )
        if ok:
            closed.append({
                "symbol": symbol,
                "ticket": pos.ticket,
                "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                "volume": pos.volume,
                "profit": pos.profit + pos.swap,
            })
        else:
            print(f"❌ 청산 실패 {symbol} #{pos.ticket}: {msg}")
    if closed:
        n, errs = tr.cancel_pending_orders(symbol)
        if n:
            print(f"  [예약 취소] {symbol} 미체결 주문 {n}건 취소")
        for e in errs:
            print(f"  [예약 취소 실패] {symbol}: {e}")
    return closed


def _format_elapsed(sec: float) -> str:
    """초 단위 경과시간을 'Nh Nm' 또는 'Nm' 형식으로 반환"""
    if sec < 0:
        return "0m"
    m = int(sec // 60)
    h = m // 60
    m = m % 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def _get_mt5_server_offset_sec(symbol: str) -> float:
    """MT5 서버 시각과 로컬 시각 차이(초). 서버가 로컬보다 빠르면 양수. 보정 시 now_sec - pos_time + offset 사용."""
    try:
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return 0.0
        server_sec = int(tick.time) if int(tick.time) <= 1e10 else int(tick.time) // 1000
        local_sec = int(datetime.now().timestamp())
        return float(server_sec - local_sec)
    except Exception:
        return 0.0


def _position_time_to_elapsed_sec(pos_time: int, now_sec: int, server_offset_sec: float = 0.0) -> float:
    """포지션 오픈 시각(pos_time)과 현재 시각(now_sec)으로 경과 초 반환. 서버/로컬 시차는 server_offset_sec로 보정."""
    if pos_time > 1e10:
        # 밀리초 단위
        now_ms = now_sec * 1000
        pos_sec = pos_time / 1000.0
        return (now_sec - pos_sec) + server_offset_sec
    elapsed = (now_sec - pos_time) + server_offset_sec
    return float(elapsed)


def _emit_position_update(positions: List[Any]) -> None:
    """GUI 런처용: 현재 포지션 현황을 특수 포맷으로 stdout에 출력 (5분마다 갱신)."""
    now_sec = int(datetime.now().timestamp())
    # MT5 서버 시간이 로컬과 다르면 경과가 음수로 나올 수 있음 → 서버 기준 보정
    server_offset_sec = _get_mt5_server_offset_sec(positions[0].symbol) if positions else 0.0

    print("[POSITION_UPDATE]", flush=True)
    if positions:
        for pos in positions:
            margin = _position_margin(pos)
            roi = (pos.profit + pos.swap) / margin * 100 if margin > 0 else 0.0
            elapsed_sec = _position_time_to_elapsed_sec(int(pos.time), now_sec, server_offset_sec)
            elapsed_str = _format_elapsed(elapsed_sec)
            profit_swap = pos.profit + pos.swap
            print(f"{pos.symbol}|{profit_swap:.2f}|{roi:.2f}|{elapsed_str}", flush=True)
        # 심볼별 합계 (런처에서 합계 행 표시용)
        by_sym: dict = {}
        for pos in positions:
            sym = pos.symbol
            if sym not in by_sym:
                by_sym[sym] = {"margin": 0.0, "profit": 0.0}
            by_sym[sym]["margin"] += _position_margin(pos)
            by_sym[sym]["profit"] += pos.profit + pos.swap
        print("[POSITION_SUMMARY]", flush=True)
        for sym, d in by_sym.items():
            total_roi = (d["profit"] / d["margin"] * 100) if d["margin"] > 0 else 0.0
            count = sum(1 for p in positions if p.symbol == sym)
            print(f"{sym}|{count}|{d['profit']:.2f}|{total_roi:.2f}", flush=True)
        print("[/POSITION_SUMMARY]", flush=True)
    print("[/POSITION_UPDATE]", flush=True)


def _emit_bb_bands() -> None:
    """GUI 런처용: 선택된 타임프레임 기준 직전 봉 OHLC, 20/2·4/4 볼린저 상·하단(절대값만) stdout 출력.
    직전 봉은 KST 기준으로 선택(mt5_ts_to_kst 보정), 차트와 동일한 봉이 표시됨."""
    tf_str = _get_bb_tf_from_file()
    print("[BB_BANDS]", flush=True)
    print(f"TF|{tf_str}", flush=True)
    for symbol in ("XAUUSD+", "NAS100+"):
        rates = get_rates_for_bb(symbol)
        if rates is None or len(rates) < 2:
            print(f"{symbol}|" + "|".join([""] * 8), flush=True)
            continue
        idx = _index_of_last_closed_bar_kst(rates, tf_str)
        if idx is None:
            idx = 1
        o = float(rates["open"][idx])
        h = float(rates["high"][idx])
        l_ = float(rates["low"][idx])
        c = float(rates["close"][idx])
        bands20 = get_20b_bands_from_rates(rates)
        bands4 = get_4b_bands_from_rates(rates)
        if bands20 is not None:
            u20, lo20 = bands20
        else:
            u20 = lo20 = None
        if bands4 is not None:
            u4, lo4 = bands4
        else:
            u4 = lo4 = None
        parts = [f"{o:.2f}", f"{h:.2f}", f"{l_:.2f}", f"{c:.2f}"]
        for v in (u20, lo20, u4, lo4):
            parts.append(f"{v:.2f}" if v is not None else "")
        print(f"{symbol}|" + "|".join(parts), flush=True)
    print("[/BB_BANDS]", flush=True)


def _timeframes_to_update_now_kst() -> List[str]:
    """현재 시각(KST) 기준으로 이번에 갱신할 타임프레임 목록.
    - 5분봉(M5)·10분봉(M10): 매 주기(30초)마다 항상 포함 → 직전 마감 봉이 테이블에 없을 때만 MT5에서 조회해 저장.
    - 1시간봉(H1): 매시 1분~59분 (직전 1시간봉 마감 후 전 구간)
    - 2시간봉(H2): 02, 04, ...시 1분~59분 (직전 2시간봉 마감 후)
    - 4시간봉(H4): 04, 08, ...시 1분~59분 (직전 4시간봉 마감 후)
    """
    now = datetime.now(KST)
    minute = now.minute
    hour = now.hour
    out: List[str] = []
    out.append("M5")
    out.append("M10")
    if minute >= 1:
        out.append("H1")
        if (hour % 2) == 0:
            out.append("H2")
        if (hour % 4) == 0:
            out.append("H4")
    return out


# KTR 테이블 자동 업데이트 스케줄 (KST 시, 분, 타임프레임). 포지션 모니터 루프에서 해당 시각에 실행.
# 아시아 5M 08:06 / 10M 08:11 / 1H 09:01 | 유럽 5M 17:06 / 10M 17:11 / 1H 18:01 | 미국 5M 23:36 / 10M 23:41 / 1H 00:01
KTR_SCHEDULE = (
    (8, 6, "5M"),   # 아시아 5분봉
    (8, 11, "10M"), # 아시아 10분봉
    (9, 1, "1H"),   # 아시아 1시간봉
    (17, 6, "5M"),  # 유럽 5분봉
    (17, 11, "10M"),# 유럽 10분봉
    (18, 1, "1H"),  # 유럽 1시간봉
    (23, 36, "5M"), # 미국 5분봉
    (23, 41, "10M"),# 미국 10분봉
    (0, 1, "1H"),   # 미국 1시간봉
)
# KTR 스케줄 슬롯 (시, 분, tf) → 세션명 (DB에 이미 있는지 확인할 때 사용)
KTR_SCHEDULE_SESSION = {
    (8, 6, "5M"): "Asia", (8, 11, "10M"): "Asia", (9, 1, "1H"): "Asia",
    (17, 6, "5M"): "Europe", (17, 11, "10M"): "Europe", (18, 1, "1H"): "Europe",
    (23, 36, "5M"): "US", (23, 41, "10M"): "US", (0, 1, "1H"): "US",
}
# 보충 실행 허용 시간: 예정 시각 지난 뒤 이 시간(분) 이내일 때만 실행. 그 외(예: 모니터 08:24 시작 시 00:01 슬롯)는 실행 안 함.
KTR_CATCHUP_WINDOW_MINUTES = 5

# 매시 :00, :15, :30, :45 포지션 점검 결과 텔레그램 전송 (mt5_position_status) 중복 방지
# 프로세스 내: _last_position_status_sent / 프로세스 간: DB( Supabase 우선, 로컬 백업 )
_last_position_status_sent: Optional[Tuple[int, int]] = None
_SCRIPT_DIR_PMC = os.path.dirname(os.path.abspath(__file__))


def _position_status_sent_db_path() -> str:
    try:
        from db_config import UNIFIED_DB_PATH
        return UNIFIED_DB_PATH
    except ImportError:
        return os.path.join(_SCRIPT_DIR_PMC, "scheduler.db")


def _ensure_position_status_sent_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS position_status_sent (
            slot_name TEXT PRIMARY KEY,
            sent_at TEXT NOT NULL
        )"""
    )
    conn.commit()


def _run_position_status_telegram_if_scheduled() -> None:
    """현재 시각(KST)이 매시 0, 15, 30, 45분이면 mt5_position_status로 점검 결과를 텔레그램 전송.
    07:00~07:59는 전송하지 않음. 여러 프로세스가 동시에 돌아도 같은 슬롯에는 1회만 전송(DB)."""
    global _last_position_status_sent
    now = datetime.now(KST)
    h, m = now.hour, now.minute
    if m not in (0, 15, 30, 45):
        return
    if h == 7:
        return
    if _last_position_status_sent == (h, m):
        return
    date_str = now.strftime("%Y-%m-%d")
    slot_name = f"{date_str}_{h:02d}_{m:02d}"
    sent_at = now.strftime("%Y-%m-%d %H:%M:%S")
    # 2일 이전 전송 이력 정리 (Supabase + 로컬)
    cutoff = (now - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        from supabase_sync import position_status_sent_delete_old_supabase, SUPABASE_SYNC_ENABLED
        if SUPABASE_SYNC_ENABLED:
            position_status_sent_delete_old_supabase(cutoff)
    except Exception:
        pass
    try:
        conn = sqlite3.connect(_position_status_sent_db_path(), timeout=10)
        _ensure_position_status_sent_table(conn)
        conn.execute("DELETE FROM position_status_sent WHERE sent_at < ?", (cutoff,))
        conn.commit()
        conn.close()
    except Exception:
        pass
    # Supabase(주 DB) 우선: 이미 있으면 스킵
    acquired = False
    try:
        from supabase_sync import (
            position_status_sent_exists_supabase,
            position_status_sent_insert_supabase,
            SUPABASE_SYNC_ENABLED,
        )
        if SUPABASE_SYNC_ENABLED:
            if position_status_sent_exists_supabase(slot_name):
                return
            if position_status_sent_insert_supabase(slot_name, sent_at):
                acquired = True
                try:
                    conn = sqlite3.connect(_position_status_sent_db_path(), timeout=10)
                    _ensure_position_status_sent_table(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO position_status_sent (slot_name, sent_at) VALUES (?, ?)",
                        (slot_name, sent_at),
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
    except Exception:
        pass
    if not acquired:
        # Supabase 비활성/실패 시 로컬만 사용
        try:
            conn = sqlite3.connect(_position_status_sent_db_path(), timeout=10)
            _ensure_position_status_sent_table(conn)
            conn.execute(
                "INSERT INTO position_status_sent (slot_name, sent_at) VALUES (?, ?)",
                (slot_name, sent_at),
            )
            conn.commit()
            conn.close()
            acquired = True
        except sqlite3.IntegrityError:
            return
        except Exception:
            return
    if not acquired:
        return
    try:
        if not init_mt5():
            return
        from mt5_position_status import send_status_telegram_current_account
        if send_status_telegram_current_account():
            _last_position_status_sent = (h, m)
            print(f"  [점검 결과] 매시 {m}분 텔레그램 전송 완료", flush=True)
    except Exception as e:
        print(f"  [점검 결과] 텔레그램 전송 실패: {e}", flush=True)


def _run_ktr_if_scheduled() -> None:
    """현재 시각(KST)이 KTR 스케줄에 해당하면 run_5m / run_10m / run_1h 실행 → 통합 DB 반영.
    DB에 해당 세션·타임프레임·당일 데이터가 이미 있으면 측정 스킵. 정확한 분을 놓쳤으면 1분 뒤 한 번만 보충."""
    global _ktr_last_run_date
    now = datetime.now(KST)
    today_str = now.strftime("%Y-%m-%d")
    today = now.date()
    h, m = now.hour, now.minute
    current_min = h * 60 + m

    def _run_tf(sh: int, sm: int, tf: str) -> None:
        # 같은 날 같은 슬롯 이미 실행됐으면 재실행·중복 전송 방지 (같은 분에 루프가 2번 돌 때 등)
        if _ktr_last_run_date.get((sh, sm, tf)) == today:
            return
        session = KTR_SCHEDULE_SESSION.get((sh, sm, tf))
        if session:
            try:
                from ktr_db_utils import KTRDatabase
                db = KTRDatabase(db_name=KTR_DB_PATH)
                if db.has_ktr_for_session_timeframe_date(session, tf, today_str):
                    _ktr_last_run_date[(sh, sm, tf)] = today
                    print(f"  [KTR] {sh:02d}:{sm:02d} {tf} 스킵 (DB에 이미 있음: {session} {tf} {today_str})", flush=True)
                    return
            except Exception as e:
                print(f"  [KTR] DB 확인 오류 (측정 진행): {e}", flush=True)
        try:
            # 실행 직전에 표시해, 같은 분에 루프가 한 번 더 돌아도 중복 실행·중복 전송 방지
            _ktr_last_run_date[(sh, sm, tf)] = today
            if tf == "5M":
                from ktr_measure_calculator import run_5m
                run_5m(ktr_db_path=KTR_DB_PATH)
            elif tf == "10M":
                from ktr_measure_calculator import run_10m
                run_10m(ktr_db_path=KTR_DB_PATH)
            else:
                from ktr_measure_calculator import run_1h
                run_1h(ktr_db_path=KTR_DB_PATH)
            print(f"  [KTR] {sh:02d}:{sm:02d} {tf} 스케줄 실행 완료 (DB 반영)", flush=True)
        except Exception as e:
            print(f"  [KTR] 스케줄 실행 오류: {e}", flush=True)
            if (sh, sm, tf) in _ktr_last_run_date and _ktr_last_run_date[(sh, sm, tf)] == today:
                del _ktr_last_run_date[(sh, sm, tf)]  # 실패 시 재시도 가능하도록 제거

    for sh, sm, tf in KTR_SCHEDULE:
        if (h, m) == (sh, sm):
            _run_tf(sh, sm, tf)
            return
    # 루프 지연 등으로 정확한 분을 놓쳤을 수 있음 → 1분 뒤 한 번만 보충
    for sh, sm, tf in KTR_SCHEDULE:
        sched_min = sh * 60 + sm
        next_min = (sched_min + 1) % (24 * 60)
        if current_min == next_min and _ktr_last_run_date.get((sh, sm, tf)) != today:
            print(f"  [KTR] {sh:02d}:{sm:02d} {tf} 보충 실행 (이전 분 누락)", flush=True)
            _run_tf(sh, sm, tf)
            return


def _run_ktr_catchup_if_missed() -> None:
    """5분 단위 모니터 실행 시점에, 스케줄상 방금 지나간(예정 시각 이후 5분 이내) KTR만 보충 실행. DB에 이미 있으면 스킵.
    예정 시각에서 너무 오래 지난 슬롯(예: 08:24에 00:01 미국 1H)은 실행하지 않음 → run_1h()가 현재 시각으로 세션을 판단해 잘못된 봉을 측정하는 것 방지."""
    global _ktr_last_run_date
    now = datetime.now(KST)
    today = now.date()
    today_str = now.strftime("%Y-%m-%d")
    catchup_end = now - timedelta(minutes=KTR_CATCHUP_WINDOW_MINUTES)

    def _run_tf(sh: int, sm: int, tf: str) -> None:
        # 같은 날 같은 슬롯 이미 실행됐으면 재실행·중복 전송 방지
        if _ktr_last_run_date.get((sh, sm, tf)) == today:
            return
        session = KTR_SCHEDULE_SESSION.get((sh, sm, tf))
        if session:
            try:
                from ktr_db_utils import KTRDatabase
                db = KTRDatabase(db_name=KTR_DB_PATH)
                if db.has_ktr_for_session_timeframe_date(session, tf, today_str):
                    _ktr_last_run_date[(sh, sm, tf)] = today
                    print(f"  [KTR] {sh:02d}:{sm:02d} {tf} 보충 스킵 (DB에 이미 있음)", flush=True)
                    return
            except Exception as e:
                print(f"  [KTR] DB 확인 오류 (측정 진행): {e}", flush=True)
        try:
            # 실행 직전에 표시해 중복 실행·중복 전송 방지
            _ktr_last_run_date[(sh, sm, tf)] = today
            if tf == "5M":
                from ktr_measure_calculator import run_5m
                run_5m(ktr_db_path=KTR_DB_PATH)
            elif tf == "10M":
                from ktr_measure_calculator import run_10m
                run_10m(ktr_db_path=KTR_DB_PATH)
            else:
                from ktr_measure_calculator import run_1h
                run_1h(ktr_db_path=KTR_DB_PATH)
            print(f"  [KTR] {sh:02d}:{sm:02d} {tf} 보충 실행 완료 (5분 점검 시 누락 확인)", flush=True)
        except Exception as e:
            print(f"  [KTR] 스케줄 보충 실행 오류: {e}", flush=True)
            if (sh, sm, tf) in _ktr_last_run_date and _ktr_last_run_date[(sh, sm, tf)] == today:
                del _ktr_last_run_date[(sh, sm, tf)]  # 실패 시 재시도 가능하도록 제거

    for sh, sm, tf in KTR_SCHEDULE:
        scheduled_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        # 예정 시각이 오늘인지 전날인지: 0시대 슬롯(0,1 1H)은 자정 직후이므로 당일 00:01
        if scheduled_dt > now:
            scheduled_dt -= timedelta(days=1)  # 전날 해당 시각으로 보정
        if scheduled_dt < catchup_end:
            continue  # 예정 시각에서 5분 초과 지남 → 보충 안 함 (잘못된 세션 측정 방지)
        if now >= scheduled_dt and _ktr_last_run_date.get((sh, sm, tf)) != today:
            _run_tf(sh, sm, tf)


def _update_candle_db() -> None:
    """XAUUSD+, NAS100+ × 5/10분·1/2/4시간봉 캔들 + BB20·SMA20·SMA120 DB 저장.
    5분봉·10분봉: 반드시 MT5 전용으로 조회해 저장(DB/기타 소스 미사용). 매 주기마다 직전 마감 봉이 테이블에 없으면 저장.
    H1/H2/H4: 매시 1분 등 해당 시각에 직전 마감 봉 저장.
    장 미개장(07:00~07:49 KST) 구간에서는 캔들이 없으므로 DB 갱신을 스킵하고 로그 한 줄만 출력."""
    now_kst = datetime.now(KST)
    if now_kst.hour == 7 and now_kst.minute < 50:
        print(f"  [DB] 장 미개장 구간(07:00~07:49 KST) — 봉 갱신 스킵", flush=True)
        return
    try:
        tf_list = _timeframes_to_update_now_kst()
        if not tf_list:
            return
        db_file = os.path.abspath(PM_DB_PATH)
        conn = pm_db.get_connection(db_file)
        pm_db.create_tables(conn)
        total = 0
        skip_by_symbol: dict = {}  # symbol -> [tf_str, ...]
        update_by_symbol: dict = {}  # symbol -> [tf_str, ...]
        for symbol in ("XAUUSD+", "NAS100+"):
            if mt5.symbol_select(symbol, True):
                mt5.symbol_info_tick(symbol)  # 캐시 갱신 유도 (차트 미오픈 시에도 최신 봉 반영)
            for ti, tf_str in enumerate(tf_list):
                if ti > 0:
                    time.sleep(0.15)
                # 5·10분봉은 MT5 전용. H1/H2/H4도 MT5에서만 조회(DB 미사용).
                rates = _get_rates_from_mt5_only(symbol, tf_str, count=150)
                if rates is not None:
                    # 타임프레임별 독립 복사(MT5 버퍼 재사용 시 서로 덮어쓰는 것 방지)
                    if hasattr(rates, "copy"):
                        rates = rates.copy()
                    # [0]=최신봉이 되도록 정규화
                    if len(rates) > 1 and int(rates["time"][0]) < int(rates["time"][-1]):
                        rates = rates[::-1].copy() if hasattr(rates, "copy") else rates[::-1]
                    # KST 기준 직전 마감 봉만 저장(5분봉/10분봉이 같은 봉으로 덮어쓰이지 않도록)
                    bar_idx = _index_of_last_closed_bar_kst(rates, tf_str)
                    if bar_idx is None:
                        print(f"  [DB] {symbol} {tf_str} 스킵: 직전 마감 봉을 KST 기준으로 찾지 못함 (rates 내 매칭 봉 없음)", flush=True)
                        continue
                    bar_ts = int(rates["time"][bar_idx])
                    bar_dt = mt5_ts_to_kst(bar_ts)
                    # H1: MT5가 봉 끝(12:00)으로 주면 봉 시작(11:00)으로 정규화
                    if tf_str == "H1" and bar_dt.minute == 0:
                        target_h1 = (datetime.now(KST) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        if bar_dt.hour == (target_h1.hour + 1) % 24 and bar_dt.date() == target_h1.date():
                            bar_dt = (bar_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        elif target_h1.hour == 23 and bar_dt.hour == 0 and (bar_dt.date() - target_h1.date()).days == 1:
                            bar_dt = target_h1
                    # H2/H4: MT5 서버 시각이 KST와 어긋나 11시·13시 등으로 올 수 있음 → 봉 시작 시각으로 정규화
                    if tf_str == "H2" and bar_dt.minute == 0 and bar_dt.hour % 2 == 1:
                        bar_dt = bar_dt.replace(hour=bar_dt.hour - 1)
                    elif tf_str == "H4" and bar_dt.minute == 0 and bar_dt.hour % 4 != 0:
                        bar_dt = bar_dt.replace(hour=(bar_dt.hour // 4) * 4)
                    bar_kst = bar_dt.strftime("%Y-%m-%d %H:%M:%S")
                    # 타임프레임별 봉 정렬 검증: M5=5분 단위, M10=10분 단위, H1/H2/H4=시 정각
                    if tf_str == "M5" and bar_dt.minute % 5 != 0:
                        print(f"  [DB] {symbol} {tf_str} 스킵: 봉 정렬 불일치 (분={bar_dt.minute})", flush=True)
                        continue
                    if tf_str == "M10" and bar_dt.minute % 10 != 0:
                        print(f"  [DB] {symbol} {tf_str} 스킵: 봉 정렬 불일치 (분={bar_dt.minute})", flush=True)
                        continue
                    if tf_str == "H1" and bar_dt.minute != 0:
                        print(f"  [DB] {symbol} {tf_str} 스킵: 봉 정렬 불일치 (분={bar_dt.minute})", flush=True)
                        continue
                    if tf_str == "H2" and (bar_dt.minute != 0 or bar_dt.hour % 2 != 0):
                        print(f"  [DB] {symbol} {tf_str} 스킵: 봉 정렬 불일치 (시={bar_dt.hour} 분={bar_dt.minute})", flush=True)
                        continue
                    if tf_str == "H4" and (bar_dt.minute != 0 or bar_dt.hour % 4 != 0):
                        print(f"  [DB] {symbol} {tf_str} 스킵: 봉 정렬 불일치 (시={bar_dt.hour} 분={bar_dt.minute})", flush=True)
                        continue
                    bar_time_str = pm_db.bar_time_string_for_latest(bar_dt, tf_str)
                    if pm_db.bar_exists(conn, symbol, tf_str, bar_time_str):
                        if symbol not in skip_by_symbol:
                            skip_by_symbol[symbol] = []
                        skip_by_symbol[symbol].append(tf_str)
                        continue
                    n = pm_db.update_latest_bar(
                        conn, symbol, tf_str, rates, mt5_ts_to_kst,
                        bar_index=bar_idx, bar_time_override=bar_time_str,
                    )
                    total += n
                    if symbol not in update_by_symbol:
                        update_by_symbol[symbol] = []
                    update_by_symbol[symbol].append(tf_str)
                    # 1시간봉: 심볼·세션별 최고/최저가 테이블 갱신 (직전 마감 봉 + 현재 진행 봉)
                    if tf_str == "H1":
                        h_closed = float(rates["high"][bar_idx])
                        l_closed = float(rates["low"][bar_idx])
                        pm_db.upsert_session_high_low(conn, symbol, bar_kst, h_closed, l_closed)
                        bar_ts_cur = int(rates["time"][0])
                        bar_dt_cur = mt5_ts_to_kst(bar_ts_cur)
                        bar_time_cur = bar_dt_cur.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                        h_cur = float(rates["high"][0])
                        l_cur = float(rates["low"][0])
                        pm_db.upsert_session_high_low(conn, symbol, bar_time_cur, h_cur, l_cur)
                        # 새 H1 봉이 DB에 저장될 때마다 2H 장대음봉 되돌림 상황을 텔레그램으로 보고
                        _notify_large_bearish_retracement_for_symbol(symbol)
        conn.close()
        # 매 주기마다 테이블 갱신 결과 한 줄 로그 (반영 건수 명시)
        if total > 0:
            parts_up = [f"{sym} {', '.join(tfs)}" for sym, tfs in update_by_symbol.items()]
            print(f"  [DB] 테이블 갱신: 봉 {total}건 반영 — {' | '.join(parts_up)}", flush=True)
        elif skip_by_symbol:
            parts_skip = [f"{sym} {', '.join(tfs)}" for sym, tfs in skip_by_symbol.items()]
            print(f"  [DB] 테이블 갱신: 반영 0건 (기존 데이터 있음) — {' | '.join(parts_skip)}", flush=True)
        else:
            print(f"  [DB] 테이블 갱신: 반영 0건 (조회 실패/정렬 불일치 등)", flush=True)
    except Exception as e:
        print(f"  [DB] 갱신 오류: {e}", flush=True)


def _run_startup_bar_backfill_24h() -> None:
    """포지션 모니터 시작 시 1회: 꺼져 있던 동안 갱신되지 않은 과거 24시간 봉을 MT5에서 조회해 bar 테이블에 바로 반영.
    가장 최근 봉(아직 마감 안 된 진행 중 봉)은 제외하고, 봉 마감된 데이터까지만 반영. INSERT OR REPLACE만 사용.
    대량 반영이므로 Supabase 동기화는 하지 않음(정상 점검 시 갱신분만 동기화)."""
    if not init_mt5():
        print("  [DB] 시작 시 24시간 보충 스킵: MT5 연결 실패", flush=True)
        return
    try:
        print("  [DB] MT5 연결됨, 24시간 봉 조회·로컬 반영 중(Supabase 제외)...", flush=True)
        db_file = os.path.abspath(PM_DB_PATH)
        conn = pm_db.get_connection(db_file)
        pm_db.create_tables(conn)
        count_bars = 300
        total = 0
        for sym_idx, symbol in enumerate(_DB_SYMBOLS):
            if mt5.symbol_select(symbol, True):
                mt5.symbol_info_tick(symbol)
            for ti, tf_str in enumerate(_DB_TIMEFRAMES):
                if ti > 0 or sym_idx > 0:
                    time.sleep(0.15)
                rates = _get_rates_from_mt5_only(symbol, tf_str, count=count_bars)
                if rates is None or len(rates) < 120:
                    continue
                if hasattr(rates, "copy"):
                    rates = rates.copy()
                if len(rates) > 1 and int(rates["time"][0]) < int(rates["time"][-1]):
                    rates = rates[::-1].copy() if hasattr(rates, "copy") else rates[::-1]
                # [0]=최신(진행 중 봉) 제외 → 봉 마감된 데이터만 반영
                if len(rates) > 1:
                    rates = rates[1:]
                if len(rates) < 120:
                    continue
                n = pm_db.update_bars(conn, symbol, tf_str, rates, mt5_ts_to_kst, sync_to_supabase=False)
                total += n
                if tf_str == "H1":
                    for i in range(len(rates)):
                        bar_ts = int(rates["time"][i])
                        bar_dt = mt5_ts_to_kst(bar_ts)
                        bar_start = (bar_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        bar_time = bar_start.strftime("%Y-%m-%d %H:%M:%S")
                        pm_db.upsert_session_high_low(conn, symbol, bar_time, float(rates["high"][i]), float(rates["low"][i]), sync_to_supabase=False)
        conn.close()
        if total > 0:
            print(f"  [DB] 시작 시 과거 24시간 bar 보충 완료(마감 봉만): {total}봉", flush=True)
    except Exception as e:
        print(f"  [DB] 시작 시 24시간 보충 오류: {e}", flush=True)


# Bar 보충 버튼: 과거 몇 일치를 DB에 반영할지 (1년 = 365일)
BAR_BACKFILL_DAYS = 365


def _bar_count_for_1_year(tf_str: str) -> int:
    """타임프레임별 1년치 봉 개수. MT5 요청 한도 고려해 상한 적용."""
    bars_per_day = {"M5": 24 * 12, "M10": 24 * 6, "H1": 24, "H2": 12, "H4": 6}
    n = BAR_BACKFILL_DAYS * (bars_per_day.get(tf_str) or 24)
    return min(n, 100000)


def run_bar_backfill() -> Tuple[List[str], int]:
    """BAR 테이블 보충 + 볼린저밴드 재계산: MT5에서 각 심볼·타임프레임별로 과거 1년치 봉 조회 후 DB 반영.
    대상: 5분봉·10분봉·1시간봉·2시간봉·4시간봉 OHLC + BB20/2·BB4/4·SMA20·SMA120. 반환: (로그 라인 목록, 갱신된 봉 수)."""
    log_lines: List[str] = []
    total = 0
    try:
        db_file = os.path.abspath(PM_DB_PATH)
        log_lines.append(f"📂 DB: {db_file}")
        log_lines.append(f"📅 과거 {BAR_BACKFILL_DAYS}일치 봉 조회 후 테이블 갱신")
        conn = pm_db.get_connection(db_file)
        pm_db.create_tables(conn)
        deleted = pm_db.delete_bars_last_24h(conn)
        if deleted > 0:
            log_lines.append(f"🗑 과거 24시간 bar {deleted}건 삭제 후 재갱신")
        for sym_idx, symbol in enumerate(_DB_SYMBOLS):
            if mt5.symbol_select(symbol, True):
                mt5.symbol_info_tick(symbol)
            for ti, tf_str in enumerate(_DB_TIMEFRAMES):
                if ti > 0 or sym_idx > 0:
                    time.sleep(0.15)
                count_bars = _bar_count_for_1_year(tf_str)
                rates = _get_rates_from_mt5_only(symbol, tf_str, count=count_bars)
                if rates is None or len(rates) < 120:
                    log_lines.append(f"  ⚠ {symbol} {tf_str}: 봉 부족({len(rates) if rates is not None else 0}) → 스킵")
                    continue
                if hasattr(rates, "copy"):
                    rates = rates.copy()
                if len(rates) > 1 and int(rates["time"][0]) < int(rates["time"][-1]):
                    rates = rates[::-1].copy() if hasattr(rates, "copy") else rates[::-1]
                n = pm_db.update_bars(conn, symbol, tf_str, rates, mt5_ts_to_kst, sync_to_supabase=False)
                total += n
                if tf_str == "H1":
                    for i in range(len(rates)):
                        bar_ts = int(rates["time"][i])
                        bar_dt = mt5_ts_to_kst(bar_ts)
                        bar_start = (bar_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        bar_time = bar_start.strftime("%Y-%m-%d %H:%M:%S")
                        pm_db.upsert_session_high_low(conn, symbol, bar_time, float(rates["high"][i]), float(rates["low"][i]), sync_to_supabase=False)
                log_lines.append(f"  ✓ {symbol} {tf_str}: {n}봉 저장 (BB·SMA 반영)")
        conn.close()
        log_lines.append(f"✅ BAR 보충·BB 갱신 완료: 총 {total}봉")
    except Exception as e:
        log_lines.append(f"❌ 오류: {e}")
    return log_lines, total


def update_bars_for_simulator(symbol: str, timeframe: str = "H1", days_back: int = 90) -> Tuple[bool, int]:
    """시뮬레이터용: 지정 심볼·타임프레임의 봉을 MT5에서 가져와 DB에 반영. 반환 (성공 여부, 저장된 봉 수)."""
    if symbol not in _DB_SYMBOLS or timeframe not in _DB_TIMEFRAMES:
        return False, 0
    if not init_mt5():
        return False, 0
    try:
        # H1 기준 days_back일 + 여유분
        count = days_back * 24 + 300
        if count > 5000:
            count = 5000
        rates = _get_rates_from_mt5_only(symbol, timeframe, count=count)
        if rates is None or len(rates) < 120:
            return False, 0
        if hasattr(rates, "copy"):
            rates = rates.copy()
        if len(rates) > 1 and int(rates["time"][0]) < int(rates["time"][-1]):
            rates = rates[::-1].copy() if hasattr(rates, "copy") else rates[::-1]
        db_file = os.path.abspath(PM_DB_PATH)
        conn = pm_db.get_connection(db_file)
        pm_db.create_tables(conn)
        n = pm_db.update_bars(conn, symbol, timeframe, rates, mt5_ts_to_kst)
        if timeframe == "H1":
            for i in range(len(rates)):
                bar_ts = int(rates["time"][i])
                bar_dt = mt5_ts_to_kst(bar_ts)
                bar_start = (bar_dt - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                bar_time = bar_start.strftime("%Y-%m-%d %H:%M:%S")
                pm_db.upsert_session_high_low(conn, symbol, bar_time, float(rates["high"][i]), float(rates["low"][i]))
        conn.close()
        return True, n
    except Exception:
        return False, 0


def run_one_check() -> None:
    """한 번의 모니터링: 포지션 조회 → 심볼별 1H 레벨 체크 → 조건 충족 시 청산 및 텔레그램.
    07:00~07:59(KST)에는 점검하지 않음."""
    _reload_stop_params_from_files()  # 런처에서 손실율/마진레벨 변경 시 반영
    now_kst = datetime.now(KST)
    if now_kst.hour == 7:
        return
    print("  [점검] MT5 연결 중...", flush=True)
    if not init_mt5():
        print("  MT5 연결 실패. 5분 후 재시도.", flush=True)
        return
    positions = mt5.positions_get()
    print(f"  [점검] 포지션 {len(positions) if positions else 0}건 조회", flush=True)

    closing_enabled = _is_closing_enabled()
    if closing_enabled:
        now_kst = datetime.now(KST)

        # 23:25 전 포지션 강제 청산 (월~금 23:25~23:29, 하루 1회)
        if run_weekday_2325_close_if_needed(now_kst):
            positions = mt5.positions_get()

        # 수익금이 잔액의 50% 초과 시 전 포지션 청산 후 동일 규모로 재진입 (수익 실현)
        profit_take_result = close_all_and_reenter_if_profit_over_pct(PROFIT_TAKE_PCT)
        if profit_take_result:
            closed_all_pt, profit_rate_pct, reentry_ok_pt, reentry_fail_pt = profit_take_result
            total_profit_pt = sum(c.get("profit", 0) for c in closed_all_pt)
            print(f"[수익실현] 수익률 {profit_rate_pct:.1f}% > {PROFIT_TAKE_PCT:.0f}% → 전 포지션 청산 후 재진입 ({len(closed_all_pt)}건 청산, 재진입 성공 {len(reentry_ok_pt)}건)", flush=True)
            _log_closes_to_file("수익실현청산", closed_all_pt, f"수익률{profit_rate_pct:.1f}%")
            _send_telegram(
                f"💰 **수익 실현 청산·재진입**\n"
                f"• 수익금이 잔액의 {PROFIT_TAKE_PCT:.0f}% 초과 ({profit_rate_pct:.1f}%) → 전 포지션 청산 후 동일 규모 재진입\n"
                f"• 청산 {len(closed_all_pt)}건, 실현 손익: ${total_profit_pt:+,.2f}\n"
                f"• 재진입: 성공 {len(reentry_ok_pt)}건" + (f", 실패 {len(reentry_fail_pt)}건" if reentry_fail_pt else "")
            )
            positions = mt5.positions_get()

        # 손실률 -5% 단위 구간 도달 시 텔레그램 알림 (청산 여부와 무관하게 현재 손실률 기준)
        if positions:
            acc = tr.get_account_info()
            if acc is not None:
                balance = float(acc.get("balance", 0) or 0)
                if balance > 0:
                    total_profit = sum(p.profit + p.swap for p in positions)
                    _send_loss_rate_alert_if_stepped(positions, balance, total_profit)

        # 마진레벨(equity/증거금*100)이 기준 이하이면 전 포지션 청산 (예: 200% 이하)
        margin_stops = close_all_if_margin_level_below(MARGIN_LEVEL_CLOSE_PCT)
        for symbol, closed_list, margin_level_pct in margin_stops:
            _log_closes_to_file("마진레벨청산", closed_list, f"{symbol} 마진레벨 {margin_level_pct:.1f}%")
            total_profit = sum(c["profit"] for c in closed_list)
            _send_telegram(
                f"🛑 **마진레벨 청산** [{symbol}]\n"
                f"• 마진레벨 {margin_level_pct:.1f}% ≤ {MARGIN_LEVEL_CLOSE_PCT:.0f}% → 전 포지션 청산 ({len(closed_list)}건)\n"
                f"• 해당 심볼 청산 손익: ${total_profit:+,.2f}"
            )
            _remove_reservations_for_symbol(symbol)
        if margin_stops:
            positions = mt5.positions_get()

        # 잔액 대비 마진이 기준(7%) 초과 시 가장 나중에 만들어진 오더 1건 청산
        margin_last_result = close_last_position_if_margin_over_pct(MARGIN_PCT_CLOSE_LAST_ORDER)
        if margin_last_result:
            closed_list, margin_ratio_pct = margin_last_result
            _log_closes_to_file("마진7%초과_최신1건청산", closed_list, f"잔액대비 마진 {margin_ratio_pct:.1f}%")
            total_profit = sum(c["profit"] for c in closed_list)
            _send_telegram(
                f"⚠️ **잔액 대비 마진 초과 → 최신 포지션 1건 청산**\n"
                f"• 잔액 대비 마진 {margin_ratio_pct:.1f}% > {MARGIN_PCT_CLOSE_LAST_ORDER:.0f}% → 가장 나중에 만들어진 오더 1건 청산\n"
                f"• 청산 손익: ${total_profit:+,.2f}"
            )
            positions = mt5.positions_get()

        # 전체 증거금 대비 합계 손실률이 기준 이하이면 전 포지션 청산 (예: 증거금 1000$, 손실 100$ → -10%)
        symbol_stops = close_all_if_overall_loss_rate_below(OVERALL_LOSS_RATE_STOP_THRESHOLD)
        for symbol, closed_list, loss_rate_pct in symbol_stops:
            _log_closes_to_file("손실율손절", closed_list, f"{symbol} 손실률{loss_rate_pct:.1f}%")
            total_profit = sum(c["profit"] for c in closed_list)
            _send_telegram(
                f"🛑 **전체 손실율 손절 청산** [{symbol}]\n"
                f"• balance 대비 손실률 {loss_rate_pct:.2f}% ≤ {OVERALL_LOSS_RATE_STOP_THRESHOLD:.0f}% → 전 포지션 청산 ({len(closed_list)}건)\n"
                f"• 해당 심볼 청산 손익: ${total_profit:+,.2f}"
            )
            _remove_reservations_for_symbol(symbol)
        if symbol_stops:
            positions = mt5.positions_get()
    else:
        print("  [점검] 청산 기능 중지 상태 → 청산 로직 스킵 (포지션/DB/BB 갱신은 계속)", flush=True)

    # GUI용 포지션 현황 출력 (포지션 없어도 빈 목록으로 갱신)
    _emit_position_update(positions if positions else [])

    # GUI용 XAUUSD / NAS100 1H 20/2 볼린저 상·하단 (1시간마다 출력)
    _emit_bb_bands()

    # 화면 표시와 동일한 시점의 캐시로 DB 저장 (표시 직후 실행해 누락 방지)
    _update_candle_db()

    # 10분봉 4B/20B 자동오더: 예약 주문 가격을 현재 10M 20B/4B 하단(오프셋)으로 갱신
    _update_m10_bb_auto_order_prices()

    # 예약 오더 간격 점검: 5분봉 KTR보다 작으면 삭제 (KTR<10이면 2*KTR 기준)
    _cancel_pending_orders_interval_less_than_5m_ktr()

    # T/P·S/L 등으로 포지션이 없어진 심볼: 해당 심볼 KTR 대기 오더 전부 삭제
    orders = mt5.orders_get() or []
    ktr_pending = [o for o in orders if getattr(o, "magic", 0) == MAGIC_KTR]
    symbols_with_ktr_pending = {o.symbol for o in ktr_pending}
    for sym in symbols_with_ktr_pending:
        ktr_pos = [p for p in (positions or []) if p.symbol == sym and getattr(p, "magic", 0) == MAGIC_KTR]
        if not ktr_pos:
            n, errs = tr.cancel_pending_orders(sym, magic=MAGIC_KTR)
            if n:
                print(f"  [T/P 등 청산 후 예약 취소] {sym} 미체결 KTR 주문 {n}건 취소", flush=True)
            for e in errs:
                print(f"  [예약 취소 실패] {sym}: {e}", flush=True)

    if not positions:
        return

    # Long 포지션 T/P: 진입 타임프레임 기준 20이평 위일 때 20B상단(현재봉 포함)-오프셋%로 갱신. 20이평 아래면 기존 T/P 유지.
    # 실시간 오더에서 T/P를 수동 업데이트한 티켓은 20B로 덮어쓰지 않음.
    realtime_tp_tickets = _load_realtime_tp_tickets()
    bb_offset_map = _load_bb_offset_pct()
    long_positions = [p for p in positions if p.type == mt5.ORDER_TYPE_BUY]
    skip_realtime = sum(1 for p in long_positions if p.ticket in realtime_tp_tickets)
    to_check = [p for p in long_positions if p.ticket not in realtime_tp_tickets]
    if long_positions:
        print(f"  [Long T/P] 점검 대상 {len(long_positions)}건 (실시간 T/P 제외 {skip_realtime}건, 갱신 대상 {len(to_check)}건)", flush=True)
    for pos in to_check:
        symbol = pos.symbol
        comment = getattr(pos, "comment", "") or ""
        parsed = _parse_comment(comment)
        tf_comment = (parsed[3] if parsed and len(parsed) > 3 else "") or "1H"
        tf_comment = tf_comment.strip().upper() or "1H"
        rates_tf_str = _comment_tf_to_rates_tf(tf_comment)
        rates_tp = get_rates_for_tf(symbol, rates_tf_str, count=30)
        if rates_tp is None or len(rates_tp) < 21:
            print(f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: 봉 부족({len(rates_tp) if rates_tp is not None else 0})", flush=True)
            continue
        t0, t_end = int(rates_tp["time"][0]), int(rates_tp["time"][-1])
        closes_ex_cur = [float(rates_tp["close"][i]) for i in range(0, len(rates_tp) - 1)]
        if len(closes_ex_cur) < 20:
            print(f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: 종가 20개 미만", flush=True)
            continue
        # 20이평: 현재 봉 제외, 직전 마감 봉 포함 최근 20개 봉 종가의 단순평균(SMA).
        # t0<t_end(과거→현재): sma_last = 마지막 20개 종가 평균. t0>=t_end: 앞 20개 = 최근 20봉 종가 평균.
        if t0 < t_end:
            sma20 = sma_last(closes_ex_cur, SMA_PERIOD)
        else:
            sma20 = sum(closes_ex_cur[0:20]) / 20.0
        # 20B 상단: 현재 봉 포함 최근 20봉으로 계산 (캔들 변동 반영)
        bb20_up = get_20b_upper_from_rates_including_current(rates_tp)
        # 조건 변경: 앞으로는 "현재 봉의 open이 20이평 위"이면 T/P 업데이트.
        # MT5/DB 모두에서 현재 봉 open을 올바르게 선택하기 위해 time 정렬 방향을 고려한다.
        if sma20 is None:
            print(f"  [Long T/P] #{pos.ticket} {symbol} 스킵: 20이평 없음", flush=True)
            continue
        try:
            if t0 < t_end:
                current_open = float(rates_tp["open"][-1])
            else:
                current_open = float(rates_tp["open"][0])
        except Exception:
            print(f"  [Long T/P] #{pos.ticket} {symbol} 스킵: open 값 조회 실패", flush=True)
            continue
        if current_open < sma20:
            print(
                f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: 봉 시가 {current_open:.2f} < 20이평 {sma20:.2f}",
                flush=True,
            )
            continue
        if bb20_up is None:
            print(f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: 20B 상단 계산 불가", flush=True)
            continue
        # 포지션 현재가가 20B 밴드 상단에 있으면 T/P 업데이트 하지 않음 (이미 목표 구간 도달로 간주)
        ask, bid = tr.get_market_price(symbol)
        if bid is not None and float(bid) >= float(bb20_up):
            print(
                f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: 현재가 {bid:.2f} >= 20B상단 {bb20_up:.2f}",
                flush=True,
            )
            continue
        offset_pct = bb_offset_map.get(symbol, 0) or 0
        tp_level = _apply_bb_offset_upper(float(bb20_up), offset_pct)
        tp_label = f"20B상단(현재봉포함)-오프셋{offset_pct}%"
        # Long 포지션 T/P는 현재가(bid)보다 위에 있어야 함. 이하이면 브로커 Invalid stops 발생 → 스킵
        if bid is not None and float(tp_level) <= float(bid):
            print(
                f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: T/P {tp_level:.2f} <= 현재가 {bid:.2f} (Invalid stops 방지)",
                flush=True,
            )
            continue
        # 현재가 대비 T/P가 브로커 최소 거리 이상 위에 있어야 함
        min_dist = getattr(tr, "get_min_stops_distance_price", lambda s: 0.0)(symbol)
        if min_dist > 0 and bid is not None and float(tp_level) < float(bid) + min_dist:
            print(
                f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 스킵: T/P {tp_level:.2f} < 현재가 {bid:.2f} + 최소거리 (Invalid stops 방지)",
                flush=True,
            )
            continue
        sl = getattr(pos, "sl", 0) or 0
        current_tp = getattr(pos, "tp", 0) or 0
        ok, msg = tr.modify_position_sltp(pos.ticket, symbol, sl, tp_level)
        if ok:
            if "변경 없음" in (msg or ""):
                print(f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 20B상단={bb20_up:.2f} → T/P {tp_level:.2f} (변경 없음, 기존과 동일)", flush=True)
            else:
                print(f"  [Long T/P] #{pos.ticket} {symbol} {tf_comment} 20B상단={bb20_up:.2f} 오프셋{offset_pct}% → T/P {current_tp:.2f} → {tp_level:.2f} 갱신됨", flush=True)
        else:
            print(f"  [Long T/P] #{pos.ticket} {symbol} 갱신 실패: {msg}", flush=True)

    # 실시간 T/P 티켓 목록에서 이미 청산된 포지션 제거 (파일 정리)
    open_tickets = {p.ticket for p in positions}
    still_realtime = realtime_tp_tickets & open_tickets
    if still_realtime != realtime_tp_tickets:
        _save_realtime_tp_tickets(still_realtime)

    if closing_enabled:
        # 심볼별로 묶기 (중복 제거)
        symbols = list({p.symbol for p in positions})

        for symbol in symbols:
            positions_for_symbol = [p for p in positions if p.symbol == symbol]
            timeframes_in_use = set()
            for p in positions_for_symbol:
                comment = getattr(p, "comment", "") or ""
                parsed = _parse_comment(comment)
                tf = (parsed[3] if parsed and len(parsed) > 3 else "") or ""
                if tf:
                    timeframes_in_use.add(tf.strip().upper())
            if not timeframes_in_use:
                timeframes_in_use.add("1H")

            # 4B 터치·4B 아래 마감 청산 (매수 Long): 이번 봉이 4B 상단에 닿기만 하고(돌파 아님) 4B 아래에서 마감 시 해당 TF 포지션 청산.
            closed_4b = False
            for tf_comment in timeframes_in_use:
                rates_tf_str = _comment_tf_to_rates_tf(tf_comment)
                rates = get_rates_for_tf(symbol, rates_tf_str, count=30)
                if rates is None or len(rates) < 5:
                    continue
                current_high = float(rates["high"][-1])
                current_close = float(rates["close"][-1])
                bb4_up = get_4b_upper_from_rates(rates)
                if bb4_up is None:
                    continue
                # 닿기만(High >= 4B) + 돌파 아님·4B 아래 마감(Close < 4B)
                touch_4b = current_high >= bb4_up
                close_below_4b = current_close < bb4_up
                if touch_4b and close_below_4b:
                    closed = close_all_positions_for_symbol_by_timeframe(symbol, tf_comment)
                    if closed:
                        _log_closes_to_file("4B터치_4B아래마감", closed, f"{symbol} {tf_comment}")
                        total_profit = sum(c["profit"] for c in closed)
                        _send_telegram(
                            f"🔒 **4B 터치·4B 아래 마감 청산** [{symbol}] ({tf_comment})\n"
                            f"• {tf_comment} 봉 기준: 이번 봉 High {current_high:.2f} ≥ 4B상단 {bb4_up:.2f}, Close {current_close:.2f} < 4B상단 → 해당 TF 포지션 청산 ({len(closed)}건)\n"
                            f"• 합계 수익: ${total_profit:+,.2f}"
                        )
                        _remove_reservations_for_symbol(symbol)
                        print(f"✅ [{symbol}] 4B 터치·4B 아래 마감({tf_comment}) → 청산 {len(closed)}건")
                        positions = mt5.positions_get()
                        if not positions:
                            return
                        closed_4b = True
                        break
            if closed_4b:
                continue

            # 역배열(20이평 < 120이평) 구간에서 매수(Long) 20이평 터치 청산: 이번 봉 Low ≤ 20이평이면 해당 TF 포지션 청산.
            closed_sma20_inverse = False
            for tf_comment in timeframes_in_use:
                rates_tf_str = _comment_tf_to_rates_tf(tf_comment)
                rates = get_rates_for_tf(symbol, rates_tf_str, count=130)
                if rates is None or len(rates) < 121:
                    continue
                closes_ex_cur = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
                sma20 = sma_last(closes_ex_cur, SMA_PERIOD)
                sma120 = sma_last(closes_ex_cur, SMA_PERIOD_120)
                if sma20 is None or sma120 is None:
                    continue
                is_inverse = sma20 < sma120
                current_low = float(rates["low"][-1])
                touch_sma20 = current_low <= sma20
                if is_inverse and touch_sma20:
                    closed = close_all_positions_for_symbol_by_timeframe(symbol, tf_comment)
                    if closed:
                        _log_closes_to_file("역배열_20이평터치", closed, f"{symbol} {tf_comment}")
                        total_profit = sum(c["profit"] for c in closed)
                        _send_telegram(
                            f"🔒 **역배열 20이평 터치 청산** [{symbol}] ({tf_comment})\n"
                            f"• {tf_comment} 봉 기준: 20이평({sma20:.2f}) < 120이평({sma120:.2f}) 역배열, 이번 봉 Low {current_low:.2f} ≤ 20이평 → 해당 TF 포지션 청산 ({len(closed)}건)\n"
                            f"• 합계 수익: ${total_profit:+,.2f}"
                        )
                        _remove_reservations_for_symbol(symbol)
                        print(f"✅ [{symbol}] 역배열 20이평 터치({tf_comment}) → 청산 {len(closed)}건")
                        positions = mt5.positions_get()
                        if not positions:
                            return
                        closed_sma20_inverse = True
                        break
            if closed_sma20_inverse:
                continue

            # 돌파더블비 매수 청산: 4이평 > 20이평, 4이평 위에서 마감된 도지 또는 긴 윗꼬리 캔들, RSI <= RSI이동평균
            closed_4ema_doji_rsi = False
            for tf_comment in timeframes_in_use:
                rates_tf_str = _comment_tf_to_rates_tf(tf_comment)
                rates = get_rates_for_tf(symbol, rates_tf_str, count=130)
                if rates is None or len(rates) < max(21, 14 + RSI_MA_PERIOD + 2):
                    continue
                trigger, reason, detail_lines = should_close_on_4ema_above_doji_or_long_upper_wick_rsi_below_ma(rates)
                if not trigger:
                    continue
                closed = close_all_positions_for_symbol_by_timeframe(symbol, tf_comment)
                if closed:
                    _log_closes_to_file("4이평위_도지긴윗꼬리_RSI", closed, f"{symbol} {tf_comment}")
                    total_profit = sum(c["profit"] for c in closed)
                    lines = [
                        f"🔒 **4이평 위 도지/긴윗꼬리 + RSI≤RSI이동평균 청산** [{symbol}] ({tf_comment})",
                        f"• {reason}",
                        f"• 해당 TF Long 포지션 {len(closed)}건 청산. 합계 수익: ${total_profit:+,.2f}",
                        "",
                    ]
                    lines.extend(detail_lines)
                    _send_telegram("\n".join(lines))
                    _remove_reservations_for_symbol(symbol)
                    print(f"✅ [{symbol}] 4이평위 도지/긴윗꼬리+RSI({tf_comment}) → Long 청산 {len(closed)}건")
                    positions = mt5.positions_get()
                    if not positions:
                        return
                    closed_4ema_doji_rsi = True
                    break
            if closed_4ema_doji_rsi:
                continue

            # 20/120 이평 모두 돌파 실패 청산: 직전 마감 봉 종가가 20이평·120이평 모두 아래이면,
            # 해당 TF Long 포지션을 T/P 설정 여부와 무관하게 전부 청산.
            closed_sma_failure = False
            for tf_comment in timeframes_in_use:
                rates_tf_str = _comment_tf_to_rates_tf(tf_comment)
                rates = get_rates_for_tf(symbol, rates_tf_str, count=130)
                if rates is None or len(rates) < 121:
                    continue
                trigger, reason, detail_lines = should_close_on_sma20_120_failure(rates)
                if not trigger:
                    continue
                closed = close_all_positions_for_symbol_by_timeframe(symbol, tf_comment)
                if closed:
                    _log_closes_to_file("20/120이평_돌파실패", closed, f"{symbol} {tf_comment}")
                    total_profit = sum(c["profit"] for c in closed)
                    lines = [
                        f"🔒 **20/120 이평 돌파 실패 청산** [{symbol}] ({tf_comment})",
                        "• 조건: 직전 마감 봉 종가가 20이평·120이평 모두 아래에서 마감.",
                        f"• 해당 TF Long 포지션 (T/P 설정 여부와 무관하게) {len(closed)}건 전부 청산.",
                        f"• 합계 수익: ${total_profit:+,.2f}",
                        "",
                    ]
                    lines.extend(detail_lines)
                    _send_telegram("\n".join(lines))
                    _remove_reservations_for_symbol(symbol)
                    print(f"✅ [{symbol}] 20/120 이평 돌파 실패({tf_comment}) → Long 청산 {len(closed)}건")
                    positions = mt5.positions_get()
                    if not positions:
                        return
                    closed_sma_failure = True
                    break
            if closed_sma_failure:
                continue

            # 20B 하단 터치 청산 (매도 Short 전용): 진입 TF별 봉으로 직전 봉 20B 미터치·이번 봉만 20B 하단 터치 시 해당 TF 매도 포지션만 청산. 매수(Long)는 20B 하단에서 청산하지 않음.
            closed_20b_lower = False
            for tf_comment in timeframes_in_use:
                rates_tf_str = _comment_tf_to_rates_tf(tf_comment)
                rates = get_rates_for_tf(symbol, rates_tf_str, count=30)
                if rates is None or len(rates) < 22:
                    continue
                current_low = float(rates["low"][-1])
                prev_low = float(rates["low"][-2])
                bb20_lo = get_20b_lower_from_rates(rates)
                prev_bb20_lo = get_20b_lower_for_prev_bar(rates)
                prev_touched = prev_bb20_lo is not None and prev_low <= prev_bb20_lo
                current_touched = bb20_lo is not None and current_low <= bb20_lo
                if current_touched and not prev_touched:
                    closed = close_positions_for_symbol_by_timeframe_sell(symbol, tf_comment)
                    if closed:
                        _log_closes_to_file("20B하단터치", closed, f"{symbol} {tf_comment}")
                        total_profit = sum(c["profit"] for c in closed)
                        _send_telegram(
                            f"🔒 **20B 하단 터치 청산** [{symbol}] ({tf_comment})\n"
                            f"• {tf_comment} 봉 기준: 직전 봉 20B 미터치, 이번 봉 Low {current_low:.2f} ≤ 20B하단 {bb20_lo:.2f} → 해당 TF 매도 포지션 청산 ({len(closed)}건)\n"
                            f"• 합계 수익: ${total_profit:+,.2f}"
                        )
                        _remove_reservations_for_symbol(symbol)
                        print(f"✅ [{symbol}] 20B 하단 터치({tf_comment} 직전봉 미터치) → 매도 청산 {len(closed)}건")
                        positions = mt5.positions_get()
                        if not positions:
                            return
                        closed_20b_lower = True
                        break
            if closed_20b_lower:
                continue


def main() -> None:
    # 로그에 바로 보이도록 시작 메시지를 먼저 출력 (이후 초기화가 오래 걸릴 수 있음)
    print("[모니터] 포지션 모니터 프로세스 시작됨.", flush=True)
    print(
        f"🚀 1H 레벨 청산 모니터 시작 (전체 점검 {CHECK_INTERVAL_SEC // 60}분, DB 갱신 {DB_UPDATE_INTERVAL_SEC}초). 종료: Ctrl+C",
        flush=True,
    )
    # 꺼져 있던 동안 못 쌓인 과거 24시간 봉을 bar 테이블에 바로 보충
    print("  [시작] 24시간 봉 보충 중...", flush=True)
    _run_startup_bar_backfill_24h()
    print("  [시작] 24시간 봉 보충 완료.", flush=True)
    # 시작 시 KTR 테이블 누락 슬롯 확인 및 자동 보충
    print("  [KTR] 누락 점검 중...", flush=True)
    try:
        from ktr_db_utils import KTRDatabase
        db = KTRDatabase(db_name=KTR_DB_PATH)
        now = datetime.now(KST)
        today_str = now.strftime("%Y-%m-%d")
        yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        missing = db.get_missing_ktr_slots([today_str, yesterday_str])
        db.conn.close()
        # 누락이 있으면 어제·오늘 모두 자동 측정(MT5/DB)으로 입력 시도
        if missing:
            try:
                from ktr_measure_calculator import run_fill_missing_ktr_for_today
                filled = run_fill_missing_ktr_for_today(ktr_db_path=KTR_DB_PATH, quiet=True)
                if filled > 0:
                    print(f"  [KTR] 누락 {filled}개 슬롯 자동 입력 완료.", flush=True)
                else:
                    print(f"  [KTR] 누락 {len(missing)}건 자동 입력 시도했으나 측정 데이터 없음 0건.", flush=True)
            except Exception as e:
                print(f"  [KTR] 자동 입력 실패: {e}", flush=True)
            # 재조회
            db2 = KTRDatabase(db_name=KTR_DB_PATH)
            missing = db2.get_missing_ktr_slots([today_str, yesterday_str])
            db2.conn.close()
        if missing:
            summary = ", ".join([f"{d} {s} {t}" for s, t, d in missing[:5]])
            if len(missing) > 5:
                summary += f" 외 {len(missing) - 5}건"
            print(f"  [KTR] 누락 {len(missing)}건: {summary}. 수동 입력은 KTR 예약 GUI에서 진행하세요.", flush=True)
            try:
                from supabase_sync import SUPABASE_SYNC_ENABLED
                if SUPABASE_SYNC_ENABLED:
                    print("  [KTR] 조회: Supabase(주) + 로컬 백업", flush=True)
                else:
                    print(f"  [KTR] 조회 DB: {KTR_DB_PATH}", flush=True)
            except Exception:
                print(f"  [KTR] 조회 DB: {KTR_DB_PATH}", flush=True)
        else:
            print("  [KTR] 누락 없음.", flush=True)
    except Exception as e:
        print(f"  [KTR] 누락 조회 생략: {e}", flush=True)
    print("  [시작] 정상 점검 루프 진입.", flush=True)
    loop_count = 0
    while True:
        try:
            if _is_weekend_off_window():
                if loop_count % 4 == 0:
                    print(
                        f"  ⏸ 주말 휴장 구간 (토 07:00 ~ 월 07:30) — {datetime.now(KST).strftime('%H:%M:%S')} 대기 중",
                        flush=True,
                    )
                loop_count += 1
                time.sleep(DB_UPDATE_INTERVAL_SEC)
                continue
            print(f"  [{datetime.now(KST).strftime('%H:%M:%S')}] 포지션/청산/T·P 점검 중...", flush=True)
            _run_ktr_if_scheduled()  # 08:06/09:01/17:06/18:01/23:36/00:01 KST 시 KTR 테이블 DB 업데이트
            _run_position_status_telegram_if_scheduled()  # 매시 :00, :15, :30, :45 KST 포지션 점검 결과 텔레그램 전송
            if loop_count % (CHECK_INTERVAL_SEC // DB_UPDATE_INTERVAL_SEC) == 0:
                _run_ktr_catchup_if_missed()  # 점검 주기마다: 스케줄상 지났는데 안 돌린 KTR 있으면 보충 실행
                run_one_check()  # 1분마다 포지션·청산·20B T/P 전체 점검 (DB 갱신 포함)
            else:
                # 30초마다 DB 캔들만 갱신
                if init_mt5():
                    _update_candle_db()
            loop_count += 1
        except Exception as e:
            print(f"❌ 체크 중 오류: {e}", flush=True)
        time.sleep(DB_UPDATE_INTERVAL_SEC)


if __name__ == "__main__":
    main()
