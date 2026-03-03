# -*- coding: utf-8 -*-
"""
KTR 예약·실시간 오더 통합 GUI.
- 예약 오더: 진입 조건(기본더블비, 20B상단 등) 모니터링 후 조건 충족 시 KTR 진입.
- 실시간 오더: 즉시 1차 시장가 + 2~N차 예약 주문 실행.
"""
import re
import shutil
import sys
import os
import json
import sqlite3
import subprocess
import threading
import time
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime, timedelta

import pytz

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import MetaTrader5 as mt5

import mt5_trade_utils as tr
from ktr_lots import get_ktrlots_lots, CONTRACT_BY_SYMBOL
from ktr_sltp_utils import (
    get_tp_level,
    get_sl_price,
    get_ktr_from_db,
    get_ktr_from_db_auto,
    get_ktr_from_db_with_fallback,
    resolve_session as resolve_ktr_session,
    symbol_for_db,
    get_rates_for_timeframe,
    _sma,
    _bb_upper,
    _bb_lower,
    load_sltp_overrides,
    save_sltp_override,
)
from ktr_sltp_updater import _parse_comment, _compute_sl_for_n
from telegram_sender_utils import send_telegram_msg
from mt5_time_utils import mt5_ts_to_kst
from ktr_db_utils import KTRDatabase

try:
    import position_monitor_db as _pm_db
except ImportError:
    _pm_db = None

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# MT5 타임프레임 → DB 타임프레임 문자열 (BB/직전봉 조회용)
_MT5_TF_TO_STR = {
    getattr(mt5, "TIMEFRAME_M5", 5): "M5",
    getattr(mt5, "TIMEFRAME_M10", 10): "M10",
    getattr(mt5, "TIMEFRAME_H1", 16385): "H1",
    getattr(mt5, "TIMEFRAME_H2", 16386): "H2",
    getattr(mt5, "TIMEFRAME_H4", 16388): "H4",
}


def _is_bar_just_closed_for_timeframe_kst(mt5_timeframe: int, now_kst: Optional[datetime] = None) -> bool:
    """
    해당 타임프레임 봉이 방금 마감된 시점(새 봉의 첫 1분 구간)이면 True.
    1H → 매시 정각(minute==0), 10M → :00/:10/:20/..., 2H → 0,2,4..시 정각, 4H → 0,4,8,12,16,20시 정각.
    예약 점검은 이 함수가 True일 때만 수행하면 됨.
    """
    if now_kst is None:
        now_kst = datetime.now(KST)
    m = now_kst.minute
    h = now_kst.hour
    h1 = getattr(mt5, "TIMEFRAME_H1", 16385)
    h2 = getattr(mt5, "TIMEFRAME_H2", 16386)
    h4 = getattr(mt5, "TIMEFRAME_H4", 16388)
    m10 = getattr(mt5, "TIMEFRAME_M10", 10)
    if mt5_timeframe == h1:
        return m == 0
    if mt5_timeframe == h2:
        return m == 0 and (h % 2) == 0
    if mt5_timeframe == h4:
        return m == 0 and (h % 4) == 0
    if mt5_timeframe == m10:
        return (m % 10) == 0
    m5 = getattr(mt5, "TIMEFRAME_M5", 5)
    if mt5_timeframe == m5:
        return (m % 5) == 0
    return True  # 알 수 없는 TF면 매 회 점검 (폴백)


def _short_bar_time_display(bar_time_kst: str) -> str:
    """직전봉 시각을 짧은 표시로. '2026-02-26 10:00' → '2/26 10:00'."""
    if not (bar_time_kst or "").strip():
        return ""
    s = bar_time_kst.strip()
    for fmt, size in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16)):
        if len(s) >= size:
            try:
                dt = datetime.strptime(s[:size], fmt)
                return f"{dt.month}/{dt.day} {dt.hour:02d}:{dt.minute:02d}"
            except ValueError:
                pass
    return bar_time_kst


def _format_reservation_check_log(
    index: int,
    symbol: str,
    tf_str: str,
    condition: str,
    why_msg: Optional[str],
    bar_time_kst: Optional[str] = None,
    brief_second_line: Optional[str] = None,
) -> List[str]:
    """예약 체크 미충족 로그. [n] 심볼 | TF (직전봉 2/26 10:00) | 조건 → 미충족, 다음 줄에 요약. 반환: 로그 줄 리스트."""
    short_bar = _short_bar_time_display(bar_time_kst) if bar_time_kst else ""
    bar_part = f" (직전봉 {short_bar})" if short_bar else ""
    line1 = f"[{index}] {symbol} | {tf_str}{bar_part} | {condition}  → 미충족"
    if brief_second_line and brief_second_line.strip():
        return [line1, brief_second_line.strip()]
    if not why_msg or not why_msg.strip():
        return [line1]
    # key=숫자 패턴만 추출해 한 줄로 (예: Low=5193.4 Close=5193.8)
    key_vals = re.findall(r"[^\s|→]+=[\d.]+", why_msg)
    line2 = " ".join(key_vals) if key_vals else why_msg.split("→")[0].strip()[:60]
    return [line1, line2]


def _get_prev_bar_from_db(symbol: str, tf_str: str, prev_bar_ts: int):
    """직전 봉 시각(ts)에 해당하는 DB 봉 반환. BB·SMA 포함. 없거나 DB 미사용 시 None."""
    if _pm_db is None or symbol not in ("XAUUSD+", "NAS100+"):
        return None
    dt = datetime.fromtimestamp(prev_bar_ts, tz=KST)
    if tf_str == "H1":
        dt = dt.replace(minute=0, second=0, microsecond=0)
    elif tf_str == "M10":
        dt = dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)
    else:
        dt = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)
    bar_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    bars = _pm_db.get_bars_from_db(symbol, tf_str, limit=50)
    for b in bars:
        if (b.get("bar_time") or "").strip() == bar_time_str:
            return b
    return None


MAGIC_KTR = 888001
KST = pytz.timezone("Asia/Seoul")

# 예약 오더 점검 비가동: 토 07:00(KST) ~ 월 07:30(KST)
WEEKEND_OFF_START_WEEKDAY = 5  # Saturday
WEEKEND_OFF_START_HOUR, WEEKEND_OFF_START_MIN = 7, 0
WEEKEND_OFF_END_WEEKDAY = 0  # Monday
WEEKEND_OFF_END_HOUR, WEEKEND_OFF_END_MIN = 7, 30


def _is_system_order_time_window_kst(now_kst: Optional[datetime] = None) -> bool:
    """매시 정시~10분이면 True (시스템 주문 입력 가능). 11~59분이면 False."""
    if now_kst is None:
        now_kst = datetime.now(KST)
    return now_kst.minute <= 10


def _is_weekend_off_window(now_kst: Optional[datetime] = None) -> bool:
    """토 07:00(KST) ~ 월 07:30(KST) 구간이면 True (예약 점검 미실행)."""
    if now_kst is None:
        now_kst = datetime.now(KST)
    wd = now_kst.weekday()  # 0=Mon .. 6=Sun
    h, m = now_kst.hour, now_kst.minute
    if wd == WEEKEND_OFF_START_WEEKDAY:
        return h > WEEKEND_OFF_START_HOUR or (h == WEEKEND_OFF_START_HOUR and m >= WEEKEND_OFF_START_MIN)
    if wd == 6:
        return True
    if wd == WEEKEND_OFF_END_WEEKDAY:
        return h < WEEKEND_OFF_END_HOUR or (h == WEEKEND_OFF_END_HOUR and m < WEEKEND_OFF_END_MIN)
    return False


def _parse_scheduled_time_kst(s: str):
    """진입 예약 시간 문자열을 KST datetime으로 파싱. 비어있거나 잘못된 형식이면 None.
    지원 형식: YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS, HH:MM(오늘 날짜)."""
    s = (s or "").strip()
    if not s:
        return None
    now = datetime.now(KST)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%H:%M"):
        try:
            dt = datetime.strptime(s.strip(), fmt)
            if fmt == "%H:%M":
                dt = now.replace(hour=dt.hour, minute=dt.minute, second=0, microsecond=0)
            else:
                if dt.tzinfo is None:
                    dt = KST.localize(dt)
            if dt.tzinfo is None:
                dt = KST.localize(dt)
            return dt
        except ValueError:
            continue
    return None


# 같은 심볼은 동일 1시간봉(KST) 안에서 예약 실행 1회만 허용
EXECUTION_1H_BAR_PATH = os.path.join(_SCRIPT_DIR, "ktr_execution_1h_bar.json")

# 봉 마감 텔레그램 ("📊 **[10분봉 마감]** ..." 진입조건 충족 여부) — 이 프로그램(ktr_order_reservation_gui)에서만 전송함.
# position_monitoring_closing.py(포지션 모니터 런처로 실행)는 해당 메시지를 보내지 않음. 중복 시 = 본 GUI가 두 개 떠 있는지 확인.
# 프로세스 간 중복 전송 방지: Supabase(주 DB) telegram_bar_sent 테이블 우선, 로컬은 백업.
_BAR_TELEGRAM_LOG_FILE = os.path.join(_SCRIPT_DIR, "bar_telegram_sent.log")


def _get_telegram_bar_sent_db_path() -> str:
    """봉 마감 텔레그램 전송 이력 로컬 백업용 DB 경로."""
    try:
        from db_config import UNIFIED_DB_PATH
        return UNIFIED_DB_PATH
    except ImportError:
        return os.path.join(_SCRIPT_DIR, "scheduler.db")


def _ensure_telegram_bar_sent_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS telegram_bar_sent (
            tf_label TEXT NOT NULL,
            bar_key TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            PRIMARY KEY (tf_label, bar_key)
        )"""
    )
    conn.commit()


def _try_acquire_bar_telegram_sent(tf_label: str, bar_key: str) -> bool:
    """(tf_label, bar_key)에 대해 전송 권한 획득 시도. Supabase(주 DB) 우선, 이미 있으면 False. 성공 시 로컬 백업 저장."""
    try:
        from supabase_sync import (
            telegram_bar_sent_exists_supabase,
            telegram_bar_sent_insert_supabase,
            SUPABASE_SYNC_ENABLED,
        )
        if SUPABASE_SYNC_ENABLED:
            if telegram_bar_sent_exists_supabase(tf_label, bar_key):
                return False
            sent_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            if telegram_bar_sent_insert_supabase(tf_label, bar_key, sent_at):
                _save_telegram_bar_sent_local_backup(tf_label, bar_key, sent_at)
                return True
    except Exception:
        pass
    # Supabase 비활성 또는 실패 시 로컬만 사용(백업/폴백)
    db_path = _get_telegram_bar_sent_db_path()
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        _ensure_telegram_bar_sent_table(conn)
        sent_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO telegram_bar_sent (tf_label, bar_key, sent_at) VALUES (?, ?, ?)",
            (tf_label, bar_key, sent_at),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception:
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _save_telegram_bar_sent_local_backup(tf_label: str, bar_key: str, sent_at: str) -> None:
    """Supabase 반영 후 로컬 백업 저장."""
    try:
        db_path = _get_telegram_bar_sent_db_path()
        conn = sqlite3.connect(db_path, timeout=10)
        _ensure_telegram_bar_sent_table(conn)
        conn.execute(
            "INSERT OR REPLACE INTO telegram_bar_sent (tf_label, bar_key, sent_at) VALUES (?, ?, ?)",
            (tf_label, bar_key, sent_at),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _clean_old_telegram_bar_sent_locks() -> None:
    """오늘(KST) 이전 봉 마감 텔레그램 전송 이력 삭제. Supabase 먼저, 로컬 백업도 동일 정리. 레거시 .telegram_bar_sent 폴더 삭제(파일 락 미사용)."""
    for _dir_name in (".telegram_bar_sent", "telegram_bar_sent"):
        legacy_dir = os.path.join(_SCRIPT_DIR, _dir_name)
        if os.path.isdir(legacy_dir):
            try:
                shutil.rmtree(legacy_dir, ignore_errors=True)
            except Exception:
                pass
    today_start = datetime.now(KST).strftime("%Y-%m-%d 00:00:00")
    try:
        from supabase_sync import telegram_bar_sent_delete_old_supabase, SUPABASE_SYNC_ENABLED
        if SUPABASE_SYNC_ENABLED:
            telegram_bar_sent_delete_old_supabase(today_start)
    except Exception:
        pass
    try:
        db_path = _get_telegram_bar_sent_db_path()
        conn = sqlite3.connect(db_path, timeout=10)
        _ensure_telegram_bar_sent_table(conn)
        conn.execute("DELETE FROM telegram_bar_sent WHERE sent_at < ?", (today_start,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _bar_telegram_lock_path(tf_label: str, bar_key: str) -> str:
    """로그 표시용. DB 사용 시 '(DB)' 반환."""
    return "(DB)"


def _bar_telegram_log(tf_label: str, bar_key: str, action: str) -> None:
    """봉 마감 텔레그램 시도/전송/스킵을 로그 파일에 기록 (중복 원인 확인용)."""
    try:
        pid = os.getpid()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} PID={pid} {tf_label} {bar_key} {action}\n"
        with open(_BAR_TELEGRAM_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

# 타임프레임 우선순위: 숫자가 클수록 더 큰 TF. 큰 TF 조건 충족 시 작은 TF 포지션 청산 후 실행.
_TF_ORDER = {"10M": 0, "1H": 1}
# 예약 동시 충족 시 실행할 TF 우선순위. 숫자가 클수록 우선 (1시간봉 > 10분봉)
_TF_EXECUTION_ORDER = {"10분봉": 0, "1시간봉": 1}


def _is_larger_timeframe(new_tf: str, existing_tf: Optional[str]) -> bool:
    """new_tf가 existing_tf보다 더 큰 타임프레임이면 True (예: 1H > 10M)."""
    if not (new_tf and existing_tf):
        return False
    n = _TF_ORDER.get(str(new_tf).strip().upper(), -1)
    e = _TF_ORDER.get(str(existing_tf).strip().upper(), -1)
    return n > e


# MT5 comment: 일부 브로커는 파이프(|)·한글 불가. ASCII만 사용, 공백 구분.
_TP_TO_CODE = {
    "20이평": "TP20", "120이평": "TP120", "20B상단": "TP20B", "4B상단": "TP4B", "사용하지 않음": "TPx",
    "KTR×1": "TPk1", "KTR×1.5": "TPk15", "KTR×2": "TPk2", "KTR×2.5": "TPk25", "KTR×3": "TPk3", "KTR×3.5": "TPk35",
}
_SL_TO_CODE = {"N기준": "SLN", "잔액비 -10%": "SLa10", "잔액비 -20%": "SLa20", "잔액비 50%": "SLa50", "사용하지 않음": "SLx"}


def _build_order_comment(
    prefix: str, session: str, tf: str, tp_option: str, sl_option: str, sl_from_n: bool
) -> str:
    """주문 코멘트: ASCII만 사용 (MT5 Invalid comment 방지). 형식: KTR1 Europe TF:10M TP20 SLN (31자 이내)."""
    base = f"{prefix} {session}"
    tf_part = f"TF:{tf}" if tf else ""
    tp_code = _TP_TO_CODE.get(tp_option, "TP20")
    sl_code = "SLN" if sl_from_n else _SL_TO_CODE.get(sl_option, "SLa10")
    parts = [base, tf_part, tp_code, sl_code]
    comment = " ".join(p for p in parts if p)
    return comment[:31]  # MT5 comment 최대 31자
MARGIN_LEVEL_MIN_PCT = 500  # 이 값 초과일 때만 주문 실행 (500% 이하에서는 진입 불가)
# 기존 진입 오더(포지션) 비중이 이 값 이상이면 예약/실시간 추가 오더 생성 금지
WEIGHT_PCT_MAX_FOR_NEW_ORDER = 20.0
# 예약 오더 저장 파일 (스크립트와 같은 폴더, 절대경로로 고정 → 재실행 시 동일 경로에서 로드)
RESERVATIONS_PATH = os.path.normpath(os.path.abspath(os.path.join(_SCRIPT_DIR, "ktr_reservations.json")))
BB_OFFSET_PATH = os.path.normpath(os.path.abspath(os.path.join(_SCRIPT_DIR, "ktr_bb_offset.json")))
# 10분봉 4B/20B 자동오더: 예약 주문 티켓 저장 (포지션 모니터에서 가격 갱신용)
M10_BB_AUTO_ORDERS_PATH = os.path.normpath(os.path.abspath(os.path.join(_SCRIPT_DIR, "m10_bb_auto_orders.json")))
BB_OFFSET_SYMBOLS = ("XAUUSD+", "NAS100+")
# 심볼별 기본 BB 오프셋 %. 지수(나스닥 등)는 절대가격이 골드 대비 약 5배 크므로 오프셋 %를 더 크게 권장.
DEFAULT_BB_OFFSET_PCT: Dict[str, float] = {"XAUUSD+": 0.5, "NAS100+": 2.5}
# 실시간 오더에서 T/P를 수동 업데이트한 티켓 목록. 포지션 모니터는 이 티켓에 20B T/P를 넣지 않음.
REALTIME_TP_TICKETS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "position_monitor_realtime_tp_tickets.json"))
# 트레일링 스탑 적용 티켓 (이익 50% 보전용 S/L 캔들가 따라 갱신)
REALTIME_TRAILING_STOP_TICKETS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "position_monitor_realtime_trailing_stop_tickets.json"))


def _load_trailing_stop_tickets() -> List[int]:
    """트레일링 스탑 적용 중인 티켓 목록 로드."""
    if not os.path.isfile(REALTIME_TRAILING_STOP_TICKETS_PATH):
        return []
    try:
        with open(REALTIME_TRAILING_STOP_TICKETS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [int(x) for x in (data if isinstance(data, list) else []) if isinstance(x, (int, float))]
    except Exception:
        return []


def _save_trailing_stop_tickets(tickets: List[int]) -> None:
    """트레일링 스탑 티켓 목록 저장."""
    try:
        with open(REALTIME_TRAILING_STOP_TICKETS_PATH, "w", encoding="utf-8") as f:
            json.dump(list(tickets), f)
    except Exception:
        pass


def _add_trailing_stop_ticket(ticket: int) -> None:
    """실시간 오더 진입 시 트레일링 스탑 체크된 경우 티켓 등록."""
    tickets = _load_trailing_stop_tickets()
    if ticket not in tickets:
        tickets.append(ticket)
        _save_trailing_stop_tickets(tickets)


def _get_last_closed_candle_close(symbol: str, mt5_tf: int = None) -> Optional[float]:
    """해당 심볼·타임프레임 직전 마감 봉 종가. 10M 기본."""
    if mt5_tf is None:
        mt5_tf = getattr(mt5, "TIMEFRAME_M10", 10)
    rates = get_rates_for_timeframe(symbol, mt5_tf, count=5)
    if rates is None or len(rates) < 2:
        return None
    return float(rates["close"][1])


def _update_trailing_stop_50pct(log_fn) -> None:
    """트레일링 스탑 등록 티켓에 대해 캔들가(10M 직전 봉 종가) 기준 이익 50% 보전 S/L 갱신."""
    tickets = _load_trailing_stop_tickets()
    if not tickets:
        return
    if not tr.init_mt5():
        return
    still_active = []
    m10 = getattr(mt5, "TIMEFRAME_M10", 10)
    for ticket in tickets:
        pos = mt5.positions_get(ticket=ticket)
        if not pos or len(pos) == 0:
            continue
        pos = pos[0]
        still_active.append(ticket)
        sym = getattr(pos, "symbol", "")
        if not sym:
            continue
        entry = float(getattr(pos, "price_open", 0) or 0)
        current_sl = getattr(pos, "sl", 0) or 0
        current_tp = getattr(pos, "tp", 0) or 0
        is_buy = pos.type == mt5.ORDER_TYPE_BUY
        price = _get_last_closed_candle_close(sym, m10)
        if price is None:
            ask, bid = tr.get_market_price(sym) if hasattr(tr, "get_market_price") else (None, None)
            price = (ask if is_buy else bid) if (ask is not None or bid is not None) else None
        if price is None:
            continue
        if is_buy:
            profit = price - entry
            if profit <= 0:
                continue
            new_sl = entry + 0.5 * profit
            if new_sl <= entry or (current_sl > 0 and new_sl <= current_sl):
                continue
            if new_sl >= price:
                continue
        else:
            profit = entry - price
            if profit <= 0:
                continue
            new_sl = entry - 0.5 * profit
            if new_sl >= entry or (current_sl > 0 and new_sl >= current_sl):
                continue
            if new_sl <= price:
                continue
        ok, msg = tr.modify_position_sltp(ticket, sym, new_sl, current_tp)
        if ok:
            log_fn(f"  [트레일링 스탑] #{ticket} {sym} S/L 갱신 → {new_sl:.5g} (이익 50% 보전)")
    _save_trailing_stop_tickets(still_active)


def _add_realtime_tp_ticket(ticket: int) -> None:
    """실시간 오더에서 T/P를 설정한 티켓을 기록. 포지션 모니터가 20B T/P로 덮어쓰지 않도록."""
    try:
        data = []
        if os.path.isfile(REALTIME_TP_TICKETS_PATH):
            with open(REALTIME_TP_TICKETS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        if not isinstance(data, list):
            data = []
        if ticket not in data:
            data.append(ticket)
        with open(REALTIME_TP_TICKETS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass


def _current_ktr_weight_pct(balance: float) -> float:
    """기존 KTR 포지션들의 증거금 합 / 잔고 * 100. 포지션 없거나 balance<=0이면 0."""
    if balance <= 0:
        return 0.0
    positions = mt5.positions_get()
    if not positions:
        return 0.0
    ktr_positions = [p for p in positions if getattr(p, "magic", 0) == MAGIC_KTR]
    if not ktr_positions:
        return 0.0
    total_margin = 0.0
    for p in ktr_positions:
        m = mt5.order_calc_margin(p.type, p.symbol, p.volume, p.price_open)
        if m is not None:
            total_margin += float(m)
    return (total_margin / balance) * 100.0


def _load_execution_1h_bar() -> Dict[str, str]:
    """심볼별 마지막 예약 실행된 1시간봉 키 로드. {"XAUUSD+": "2025-02-14 15:00"}."""
    if not os.path.isfile(EXECUTION_1H_BAR_PATH):
        return {}
    try:
        with open(EXECUTION_1H_BAR_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_execution_1h_bar(symbol: str, bar_key: str) -> None:
    """해당 심볼이 bar_key 1시간봉에 실행됐음을 기록."""
    data = _load_execution_1h_bar()
    data[symbol] = bar_key
    try:
        with open(EXECUTION_1H_BAR_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def _load_bb_offset() -> Dict[str, float]:
    """심볼별 볼린저 밴드 오프셋 % 로드. {"XAUUSD+": 0.5, "NAS100+": 2.5} 형태. 파일에 없으면 DEFAULT_BB_OFFSET_PCT 사용."""
    result: Dict[str, float] = {}
    if os.path.isfile(BB_OFFSET_PATH):
        try:
            with open(BB_OFFSET_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            result = {k: float(v) for k, v in (data or {}).items() if isinstance(v, (int, float))}
        except Exception:
            pass
    # 파일에 없는 심볼은 지수 규모에 따른 기본값 적용 (나스닥 등은 오프셋 % 더 큼)
    for sym in BB_OFFSET_SYMBOLS:
        if sym not in result:
            result[sym] = DEFAULT_BB_OFFSET_PCT.get(sym, 0.0)
    return result


def _save_bb_offset(data: Dict[str, float]) -> None:
    """심볼별 BB 오프셋 % 저장. 기존 파일이 있으면 덮어씀."""
    try:
        with open(BB_OFFSET_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _apply_bb_offset(value: float, offset_pct: float, is_upper: bool) -> float:
    """볼린저 밴드 값에 오프셋 % 적용. 상단은 낮아지게 *(1-offset%), 하단은 높아지게 *(1+offset%)."""
    if not offset_pct:
        return value
    if is_upper:
        return value * (1.0 - offset_pct / 100.0)
    return value * (1.0 + offset_pct / 100.0)


def _get_m10_bb_lower_levels_with_offset(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """10분봉 직전 봉 기준 20B(20,2) 하단·4B(4,4) 하단에 오프셋 적용한 값. (20b_lower, 4b_lower)."""
    m10 = getattr(mt5, "TIMEFRAME_M10", 10)
    rates = get_rates_for_timeframe(symbol, m10, count=25)
    if rates is None or len(rates) < 21:
        return None, None
    t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
    if t0 < t_end:
        closes = [float(rates["close"][i]) for i in range(0, len(rates) - 1)]
    else:
        closes = [float(rates["close"][i]) for i in range(1, len(rates))]
    if len(closes) < 20:
        return None, None
    lower20 = _bb_lower(closes, 20, 2)
    lower4 = _bb_lower(closes, 4, 4)
    if lower20 is None and lower4 is None:
        return None, None
    offset_pct = _load_bb_offset().get(symbol, 0) or 0
    l20 = _apply_bb_offset(float(lower20), offset_pct, False) if lower20 is not None else None
    l4 = _apply_bb_offset(float(lower4), offset_pct, False) if lower4 is not None else None
    return l20, l4


def _1h_last_closed_bar_touched_20b_upper_or_lower(symbol: str) -> Tuple[bool, bool]:
    """
    1시간봉 기준 가장 최근 마감 봉이 20B(20,2) 상단 Offset / 하단 Offset 에 닿았는지 판정.
    반환: (touched_upper, touched_lower)
    - touched_upper True → 해당 봉 고가 >= 20B 상단(오프셋 적용) → 10분봉 매수 진입 차단용.
    - touched_lower True → 해당 봉 저가 <= 20B 하단(오프셋 적용) → 10분봉 매도 진입 차단용.
    """
    h1 = getattr(mt5, "TIMEFRAME_H1", 16385)
    rates = get_rates_for_timeframe(symbol, h1, count=25)
    if rates is None or len(rates) < 21:
        return False, False
    idx_prev = _index_of_last_closed_bar_kst(rates, "H1")
    if idx_prev is None:
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        idx_prev = len(rates) - 2 if t0 < t_end else 1
    t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
    if t0 < t_end:
        close_20_range = range(max(0, idx_prev - 19), idx_prev + 1)
    else:
        close_20_range = range(idx_prev, min(idx_prev + 20, len(rates)))
    closes_20 = [float(rates["close"][j]) for j in close_20_range if 0 <= j < len(rates)]
    if len(closes_20) < 20:
        return False, False
    upper_20 = _bb_upper(closes_20, 20, 2)
    lower_20 = _bb_lower(closes_20, 20, 2)
    if upper_20 is None or lower_20 is None:
        return False, False
    offset_pct = _load_bb_offset().get(symbol, 0) or 0
    upper_off = _apply_bb_offset(float(upper_20), offset_pct, True)
    lower_off = _apply_bb_offset(float(lower_20), offset_pct, False)
    high = float(rates["high"][idx_prev])
    low = float(rates["low"][idx_prev])
    touched_upper = high >= upper_off
    touched_lower = low <= lower_off
    return touched_upper, touched_lower


def _current_1h_bar_key_kst() -> str:
    """현재 시각(KST) 기준 1시간봉 시작 키. 예: 2025-02-14 15:00."""
    return datetime.now(KST).strftime("%Y-%m-%d %H:00")


def _next_bar_close_kst(active_list: List[Dict[str, Any]]) -> Optional[datetime]:
    """예약 목록에 있는 타임프레임 중 가장 가까운 '다음 봉 마감 시각'(KST) 반환. 1시간봉이면 다음 매시 정각, 10분봉이면 다음 :00/:10/..."""
    if not active_list:
        return None
    now_kst = datetime.now(KST)
    next_close: Optional[datetime] = None
    h1 = getattr(mt5, "TIMEFRAME_H1", 16385)
    m10 = getattr(mt5, "TIMEFRAME_M10", 10)
    for r in active_list:
        mt5_tf = r.get("mt5_timeframe", mt5.TIMEFRAME_H1)
        if mt5_tf == h1:
            n = (now_kst.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            if next_close is None or n < next_close:
                next_close = n
        elif mt5_tf == m10:
            # 10분봉: 다음 :00, :10, :20, :30, :40, :50
            cur_min = now_kst.minute
            next_min = ((cur_min // 10) + 1) * 10
            if next_min >= 60:
                n = (now_kst.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            else:
                n = now_kst.replace(minute=next_min, second=0, microsecond=0)
            if next_close is None or n < next_close:
                next_close = n
    return next_close


def _index_of_last_closed_bar_kst(rates: Any, tf_str: str) -> Optional[int]:
    """
    KST 기준 '직전 마감 봉'의 rates 인덱스 반환.
    DB는 [0]=최신 저장 봉이라, 아직 현재 봉이 저장 안 됐을 수 있으므로 시각으로 직전 봉을 찾음.
    """
    if rates is None or len(rates) == 0:
        return None
    now_kst = datetime.now(KST)
    if tf_str == "H1":
        target = (now_kst - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
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
    for i in range(len(rates)):
        bar_ts = int(rates["time"][i])
        bar_dt = mt5_ts_to_kst(bar_ts)
        if tf_str == "H1":
            if bar_dt.date() == target.date() and bar_dt.hour == target.hour and bar_dt.minute == 0:
                return i
        elif tf_str == "M10":
            if bar_dt.date() == target.date() and bar_dt.hour == target.hour and bar_dt.minute == target.minute:
                return i
        elif tf_str == "M5":
            if bar_dt.date() == target.date() and bar_dt.hour == target.hour and bar_dt.minute == target.minute:
                return i
        else:
            if bar_dt.date() == target.date() and bar_dt.hour == target.hour and bar_dt.minute == target.minute:
                return i
    return None


def _last_closed_bar_display_kst(tf_str: Optional[str]):
    """KST 기준 '직전 마감 봉'의 시작 시각. 로그 표시용(타임존/오프셋 무관)."""
    now_kst = datetime.now(KST)
    if not tf_str:
        return (now_kst - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    if tf_str == "H1":
        return (now_kst - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    if tf_str == "M10":
        if now_kst.minute < 10:
            return (now_kst - timedelta(hours=1)).replace(minute=50, second=0, microsecond=0)
        return now_kst.replace(minute=(now_kst.minute // 10 - 1) * 10, second=0, microsecond=0)
    if tf_str == "M5":
        if now_kst.minute < 5:
            return (now_kst - timedelta(hours=1)).replace(minute=55, second=0, microsecond=0)
        return now_kst.replace(minute=(now_kst.minute // 5 - 1) * 5, second=0, microsecond=0)
    return (now_kst - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


# 타임프레임 표시명 -> MT5 상수 (예약/실시간 공통)
TF_MAP = {
    "10분봉": getattr(mt5, "TIMEFRAME_M10", 10),
    "1시간봉": mt5.TIMEFRAME_H1,
}
# 예약 오더에서 선택 가능한 타임프레임
RESERVATION_TF_OPTIONS = ("10분봉", "1시간봉")

ENTRY_CONDITIONS = ("기본더블비", "돌파더블비", "20B상단")
REALTIME_KTR_CONDITIONS = ("실시간+KTR1", "실시간+KTR2")  # 가격 도달 시 1건 예약 주문 실행 (0.5 KTR 방식 제거, 항상 1 KTR 단위)


def _rma_series(values: List[float], length: int) -> Optional[List[float]]:
    """Wilder's RMA (smoothed MA). First value = SMA of first length values, then rma[i] = (rma[i-1]*(length-1) + values[i])/length."""
    if not values or length < 1 or len(values) < length:
        return None
    out: List[float] = []
    sma_first = sum(values[:length]) / length
    out.append(sma_first)
    for i in range(length, len(values)):
        rma_prev = out[-1]
        out.append((rma_prev * (length - 1) + values[i]) / length)
    return out


def _rsi_series(closes_chron: List[float], period: int = 14) -> Optional[List[float]]:
    """RSI(period) 시리즈. closes_chron = 과거→현재 순. Wilder RMA 사용. 반환 길이 = len(closes_chron), 앞 period개는 None."""
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
            rs = g / l
            result.append(100.0 - 100.0 / (1.0 + rs))
    return result


def _is_rsi_downtrend(symbol: str) -> bool:
    """10분봉 직전 마감 봉 RSI가 그 이전 봉 RSI보다 낮으면 True(하향 추세). 판별 불가 시 False(매수 허용)."""
    m10 = getattr(mt5, "TIMEFRAME_M10", 10)
    rates = get_rates_for_timeframe(symbol, m10, count=30)
    if rates is None or len(rates) < 17:
        return False
    # MT5: rates[0]=현재봉, rates[1]=직전 마감, rates[2]=그 이전. 과거→현재 순으로 종가 리스트(마감 봉만)
    close_reversed = [float(rates["close"][i]) for i in range(len(rates) - 1, 0, -1)]
    closes_chron = close_reversed
    rsi_list = _rsi_series(closes_chron, 14)
    if rsi_list is None or len(rsi_list) < 2:
        return False
    rsi_last = rsi_list[-1]
    rsi_prev = rsi_list[-2]
    if rsi_last is None or rsi_prev is None:
        return False
    return rsi_last < rsi_prev


def _compute_adx_series(rates: Any, di_len: int = 14, adx_len: int = 14, chronological: bool = True) -> Optional[List[float]]:
    """ADX 시리즈 계산 (Pine Script 동일). chronological=True면 rates[0]=과거, rates[-1]=최신.
    반환: 길이 len(rates), result[i] = i번째 봉의 ADX (유효하지 않으면 None)."""
    n = len(rates)
    if n < di_len + adx_len:
        return None
    if not chronological:
        high = [float(rates["high"][n - 1 - i]) for i in range(n)]
        low = [float(rates["low"][n - 1 - i]) for i in range(n)]
        close = [float(rates["close"][n - 1 - i]) for i in range(n)]
    else:
        high = [float(rates["high"][i]) for i in range(n)]
        low = [float(rates["low"][i]) for i in range(n)]
        close = [float(rates["close"][i]) for i in range(n)]
    tr_list = [high[0] - low[0]]
    plus_dm = [0.0]
    minus_dm = [0.0]
    for i in range(1, n):
        tr_list.append(max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1])))
        up = high[i] - high[i - 1]
        down = low[i - 1] - low[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
    rma_tr = _rma_series(tr_list, di_len)
    rma_plus = _rma_series(plus_dm, di_len)
    rma_minus = _rma_series(minus_dm, di_len)
    if not rma_tr or not rma_plus or not rma_minus:
        return None
    # RMA 시리즈 길이 = n - di_len + 1 (첫 값은 인덱스 di_len-1에 대응). 앞쪽 인덱스는 0으로 채움.
    rma_len = len(rma_tr)
    plus_di = [0.0] * (di_len - 1) + [
        100 * rma_plus[j] / rma_tr[j] if rma_tr[j] else 0.0 for j in range(rma_len)
    ]
    minus_di = [0.0] * (di_len - 1) + [
        100 * rma_minus[j] / rma_tr[j] if rma_tr[j] else 0.0 for j in range(rma_len)
    ]
    dx_list = []
    for i in range(n):
        s = plus_di[i] + minus_di[i]
        dx_list.append(100 * abs(plus_di[i] - minus_di[i]) / s if s > 0 else 0.0)
    adx_rma = _rma_series(dx_list, adx_len)
    if not adx_rma:
        return None
    # adx_rma 길이 = n - adx_len + 1. 앞쪽 first_valid개는 None, 이후 adx_rma 순서대로 채움.
    first_valid = adx_len - 1
    adx_chron: List[Optional[float]] = [None] * first_valid + [float(adx_rma[j]) for j in range(len(adx_rma))]
    if not chronological:
        adx_chron = adx_chron[::-1]
    return adx_chron


def _1h_sma20_position(symbol: str) -> Optional[str]:
    """1시간봉 직전 봉 종가 대비 1H 20이평 위치. 'below'=이평 아래, 'above'=이평 위, None=판별 불가."""
    h1 = getattr(mt5, "TIMEFRAME_H1", 16385)
    rates = get_rates_for_timeframe(symbol, h1, count=25)
    if rates is None or len(rates) < 20:
        return None
    idx_prev = _index_of_last_closed_bar_kst(rates, "H1")
    if idx_prev is None:
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        idx_prev = len(rates) - 2 if t0 < t_end else 1
    t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
    if t0 < t_end:
        close_20_range = range(max(0, idx_prev - 19), idx_prev + 1)
    else:
        close_20_range = range(idx_prev, min(idx_prev + 20, len(rates)))
    closes_20 = [float(rates["close"][j]) for j in close_20_range if 0 <= j < len(rates)]
    if len(closes_20) < 20:
        return None
    sma20 = _sma(closes_20, 20)
    if sma20 is None:
        return None
    close_ref = float(rates["close"][idx_prev])
    if close_ref < sma20:
        return "below"
    if close_ref > sma20:
        return "above"
    return None  # 동일 구간은 체크 안 함


def _is_below_1h_sma20(symbol: str) -> bool:
    """1시간봉 직전 마감 봉 종가가 1H 20이평 아래인지."""
    return _1h_sma20_position(symbol) == "below"


def _tf_sma20_position(symbol: str, tf_str: str) -> Optional[str]:
    """지정 타임프레임 직전 봉 종가 대비 해당 TF 20이평 위치. 'below'/'above'/None."""
    mt5_tf = {
        "M5": getattr(mt5, "TIMEFRAME_M5", 5),
        "M10": getattr(mt5, "TIMEFRAME_M10", 10),
        "H1": getattr(mt5, "TIMEFRAME_H1", 16385),
        "H2": getattr(mt5, "TIMEFRAME_H2", 16386),
        "H4": getattr(mt5, "TIMEFRAME_H4", 16388),
    }.get(tf_str)
    if mt5_tf is None:
        return _1h_sma20_position(symbol)
    rates = get_rates_for_timeframe(symbol, mt5_tf, count=25)
    if rates is None or len(rates) < 20:
        return None
    idx_prev = _index_of_last_closed_bar_kst(rates, tf_str)
    if idx_prev is None:
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        idx_prev = len(rates) - 2 if t0 < t_end else 1
    t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
    if t0 < t_end:
        close_20_range = range(max(0, idx_prev - 19), idx_prev + 1)
    else:
        close_20_range = range(idx_prev, min(idx_prev + 20, len(rates)))
    closes_20 = [float(rates["close"][j]) for j in close_20_range if 0 <= j < len(rates)]
    if len(closes_20) < 20:
        return None
    sma20 = _sma(closes_20, 20)
    if sma20 is None:
        return None
    close_ref = float(rates["close"][idx_prev])
    if close_ref < sma20:
        return "below"
    if close_ref > sma20:
        return "above"
    return None


# 매도(20B상단): 1H 20이평 위에서만 오더. 매수: 해당 TF 20이평 아래에서만.
CONDITION_SELL_REQUIRE_ABOVE_1H_SMA20 = "20B상단"


def _allowed_by_sma20_filter(
    symbol: str, matched_condition: str, side: str, tf_str: str
) -> bool:
    """매수: 해당 TF 20이평 아래에서만. 매도(20B상단): 1H 20이평 위에서만. 판별 불가 시 False."""
    side_upper = (side or "").strip().upper()
    cond = (matched_condition or "").strip()
    if side_upper == "BUY":
        pos = _tf_sma20_position(symbol, tf_str)
        return pos == "below" if pos is not None else False
    if side_upper == "SELL" and cond == CONDITION_SELL_REQUIRE_ABOVE_1H_SMA20:
        pos = _1h_sma20_position(symbol)
        return pos == "above" if pos is not None else False
    return True  # 그 외 매도 등은 필터 없음


def _check_entry_condition_one(
    symbol: str, mt5_timeframe: int, condition: str
) -> bool:
    """
    선택 타임프레임의 직전 마감 봉(인덱스 1) 기준으로 진입 조건 하나 검사.
    rates[0]=현재 봉, rates[1]=직전 마감 봉.
    """
    matched, _, _ = _check_entry_condition_one_with_detail(symbol, mt5_timeframe, condition)
    return matched


def _check_entry_condition_one_with_detail(
    symbol: str, mt5_timeframe: int, condition: str
) -> tuple:
    """
    진입 조건 검사 + 상세 메시지 반환.
    반환: (충족 여부, 상세 메시지, brief_second_line 또는 None. 로그 두 번째 줄용)
    20B하단: 예약 타임프레임(1H 등)의 20B 하단에, 직전 마감 봉 또는 현재 진행 중 봉이 닿으면 충족.
    MT5는 [0]=과거·맨 끝=최신 순서이므로, 직전 마감 봉=len-2 / 현재 봉=len-1 로 보정.
    DB는 [0]=최신 순서이므로 직전 마감=1 / 현재=0.
    """
    tf_str = _MT5_TF_TO_STR.get(mt5_timeframe)
    bb_offset_pct = _load_bb_offset().get(symbol, 0) or 0
    # XAUUSD+/NAS100+는 DB 우선 사용( get_rates_for_timeframe 내부). 조건 검증은 DB 테이블 숫자 기준.
    rates = get_rates_for_timeframe(symbol, mt5_timeframe, count=35)
    if rates is None or len(rates) < 22:
        return False, None, None
    # KST 기준 직전 마감 봉 인덱스. DB는 [0]=최신(방금 마감 봉) → 직전 마감 = index 0. MT5(과거→최신)면 직전 마감 = len-2.
    idx_prev = _index_of_last_closed_bar_kst(rates, tf_str) if tf_str else None
    if idx_prev is None:
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        if t0 < t_end:
            idx_prev = len(rates) - 2
            idx_curr = len(rates) - 1
        else:
            idx_prev = 0
            idx_curr = 0
    else:
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        idx_curr = len(rates) - 1 if t0 < t_end else 0
    n_rates = len(rates)
    if not (0 <= idx_prev < n_rates and 0 <= idx_curr < n_rates):
        return False, None, None
    prev_bar_ts = int(rates["time"][idx_prev])
    bar_from_db = _get_prev_bar_from_db(symbol, tf_str, prev_bar_ts) if tf_str else None
    # DB에 해당 봉이 있으면 테이블 값(close/high/low)으로 조건 검증
    if _pm_db and symbol in ("XAUUSD+", "NAS100+") and bar_from_db is not None:
        low = float(bar_from_db.get("low", rates["low"][idx_prev]))
        close = float(bar_from_db.get("close", rates["close"][idx_prev]))
        high = float(bar_from_db.get("high", rates["high"][idx_prev]))
    else:
        low = float(rates["low"][idx_prev])
        close = float(rates["close"][idx_prev])
        high = float(rates["high"][idx_prev])
    bar_kst_str = (_last_closed_bar_display_kst(tf_str).strftime("%Y-%m-%d %H:%M") + " KST") if tf_str else (mt5_ts_to_kst(prev_bar_ts).strftime("%Y-%m-%d %H:%M") + " KST")

    if condition == "20이평하락시 4B상단터치 매도":
        closes_20_1 = [float(rates["close"][i]) for i in range(idx_prev, min(idx_prev + 20, len(rates)))]
        closes_20_2 = [float(rates["close"][i]) for i in range(idx_prev + 1, min(idx_prev + 21, len(rates)))]
        if len(closes_20_1) < 20 or len(closes_20_2) < 20:
            return False, None, None
        sma20_1 = _sma(closes_20_1, 20)
        sma20_2 = _sma(closes_20_2, 20)
        if sma20_1 is None or sma20_2 is None:
            return False, None, None
        if sma20_1 >= sma20_2:
            return False, (
                f"[20이평하락시 4B상단터치 매도] 20이평 비하락(SMA20 직전={sma20_1:.5g} >= 이전={sma20_2:.5g}) → 미충족."
            ), None
        bb_up = None
        if bar_from_db is not None and bar_from_db.get("bb4_upper") is not None:
            bb_up = float(bar_from_db["bb4_upper"])
        if bb_up is None:
            open_4_range = range(idx_prev - 3, idx_prev + 1) if t0 < t_end else range(idx_prev, idx_prev + 4)
            opens_4 = [float(rates["open"][i]) for i in open_4_range if 0 <= i < len(rates)]
            if len(opens_4) >= 4:
                bb_up = _bb_upper(opens_4, 4, 4)
        if bb_up is None:
            return False, None, None
        bb_up = _apply_bb_offset(float(bb_up), bb_offset_pct, True)
        matched = high >= bb_up
        msg = (
            f"[20이평하락시 4B상단터치 매도] 20이평 하락 추세(SMA20 직전={sma20_1:.5g} < 이전={sma20_2:.5g}) | "
            f"직전 봉 High={high:.5g} >= BB(4,4) 상단={bb_up:.5g} 충족."
        ) if matched else (
            f"[20이평하락시 4B상단터치 매도] 직전 봉 High={high:.5g} | BB(4,4)상단={bb_up:.5g} | "
            f"20이평 하락={sma20_1:.5g} < {sma20_2:.5g} → High >= 4B상단 미충족."
        )
        return matched, msg, None

    if condition == "20B하단":
        bb_low = None
        if bar_from_db is not None and bar_from_db.get("bb20_lower") is not None:
            bb_low = float(bar_from_db["bb20_lower"])
        if bb_low is None:
            if t0 < t_end:
                close_20_range = range(idx_prev - 19, idx_prev + 1)
            else:
                close_20_range = range(idx_prev, min(idx_prev + 20, len(rates)))
            closes = [float(rates["close"][i]) for i in close_20_range if 0 <= i < len(rates)]
            if len(closes) >= 20:
                bb_low = _bb_lower(closes, 20, 2)
        if bb_low is None:
            return False, None, None
        bb_low = _apply_bb_offset(float(bb_low), bb_offset_pct, False)
        # 20B하단 기울기: 아랫쪽으로 열려 있으면(직전 봉 20B하단 < 이전 봉 20B하단) 추가 하락 예상 → 진입 스킵
        if t0 < t_end:
            close_20_before = range(idx_prev - 20, idx_prev)
        else:
            close_20_before = range(idx_prev + 1, min(idx_prev + 21, len(rates)))
        closes_before = [float(rates["close"][i]) for i in close_20_before if 0 <= i < len(rates)]
        if len(closes_before) >= 20:
            bb_low_prev = _bb_lower(closes_before, 20, 2)
            if bb_low_prev is not None:
                bb_low_prev = _apply_bb_offset(float(bb_low_prev), bb_offset_pct, False)
            if bb_low_prev is not None and bb_low < bb_low_prev:
                return False, (
                    f"[20B하단] 20B하단 기울기 하락(아랫쪽 열림) 직전={bb_low:.5g} < 이전={bb_low_prev:.5g} → 추가 하락 예상, 진입 스킵."
                ), None
        current_bar_low = float(rates["low"][idx_curr])
        matched = (low <= bb_low) or (current_bar_low <= bb_low)
        if matched:
            if current_bar_low <= bb_low and low > bb_low:
                current_ts = int(rates["time"][idx_curr])
                current_kst_str = mt5_ts_to_kst(current_ts).strftime("%Y-%m-%d %H:%M") + " KST"
                msg = (
                    f"[20B하단] 현재 봉({current_kst_str}) Low={current_bar_low:.5g} <= BB(20,2)하단={bb_low:.5g} 터치 → 실행."
                )
            else:
                msg = (
                    f"[20B하단] 직전 봉({bar_kst_str}) Low={low:.5g}, Close={close:.5g} | "
                    f"BB(20,2) 하단={bb_low:.5g} → Low <= BB하단 충족, 20B 하단 터치."
                )
        else:
            msg = (
                f"[20B하단] 직전 봉 Low={low:.5g}, 현재 봉 Low={current_bar_low:.5g} | "
                f"BB(20,2) 하단={bb_low:.5g} → 터치 미충족."
            )
        return matched, msg, None

    if condition == "하단더블비":
        # 20B하단과 4B하단을 캔들이 모두 터치(직전 봉 Low가 둘 다 이하)일 때 진입
        bb20_low = None
        if bar_from_db is not None and bar_from_db.get("bb20_lower") is not None:
            bb20_low = float(bar_from_db["bb20_lower"])
        if bb20_low is None:
            if t0 < t_end:
                close_20_range = range(idx_prev - 19, idx_prev + 1)
            else:
                close_20_range = range(idx_prev, min(idx_prev + 20, len(rates)))
            closes = [float(rates["close"][i]) for i in close_20_range if 0 <= i < len(rates)]
            if len(closes) >= 20:
                bb20_low = _bb_lower(closes, 20, 2)
        if bb20_low is None:
            return False, None, None
        bb20_low = _apply_bb_offset(float(bb20_low), bb_offset_pct, False)
        # 20B하단 기울기: 아랫쪽으로 열려 있으면 추가 하락 예상 → 진입 스킵
        if t0 < t_end:
            close_20_before = range(idx_prev - 20, idx_prev)
        else:
            close_20_before = range(idx_prev + 1, min(idx_prev + 21, len(rates)))
        closes_20_before = [float(rates["close"][i]) for i in close_20_before if 0 <= i < len(rates)]
        if len(closes_20_before) >= 20:
            bb20_low_prev = _bb_lower(closes_20_before, 20, 2)
            if bb20_low_prev is not None:
                bb20_low_prev = _apply_bb_offset(float(bb20_low_prev), bb_offset_pct, False)
            if bb20_low_prev is not None and bb20_low < bb20_low_prev:
                return False, (
                    f"[하단더블비] 20B하단 기울기 하락(아랫쪽 열림) 직전={bb20_low:.5g} < 이전={bb20_low_prev:.5g} → 추가 하락 예상, 진입 스킵."
                ), None
        bb4_low = None
        if bar_from_db is not None and bar_from_db.get("bb4_lower") is not None:
            bb4_low = float(bar_from_db["bb4_lower"])
        if bb4_low is None:
            open_4_range = range(idx_prev - 3, idx_prev + 1) if t0 < t_end else range(idx_prev, idx_prev + 4)
            opens_4 = [float(rates["open"][i]) for i in open_4_range if 0 <= i < len(rates)]
            if len(opens_4) >= 4:
                bb4_low = _bb_lower(opens_4, 4, 4)
        if bb4_low is None:
            return False, None, None
        bb4_low = _apply_bb_offset(float(bb4_low), bb_offset_pct, False)
        matched = (low <= bb20_low) and (low <= bb4_low)
        msg = (
            f"[하단더블비] 직전 봉({bar_kst_str}) Low={low:.5g} | "
            f"20B하단={bb20_low:.5g}, 4B하단={bb4_low:.5g} → 모두 터치 충족."
        ) if matched else (
            f"[하단더블비] 직전 봉 Low={low:.5g} | 20B하단={bb20_low:.5g}, 4B하단={bb4_low:.5g} → "
            f"터치 미충족 (둘 다 이하여야 함)."
        )
        return matched, msg, None

    if condition == "4원비":
        # Low가 4B(BB(4,4) 하단)에 닿으면 진입. 직전 봉 또는 현재 봉 Low <= 4B하단
        bb4_low = None
        if bar_from_db is not None and bar_from_db.get("bb4_lower") is not None:
            bb4_low = float(bar_from_db["bb4_lower"])
        if bb4_low is None:
            open_4_range = range(idx_prev - 3, idx_prev + 1) if t0 < t_end else range(idx_prev, idx_prev + 4)
            opens_4 = [float(rates["open"][i]) for i in open_4_range if 0 <= i < len(rates)]
            if len(opens_4) >= 4:
                bb4_low = _bb_lower(opens_4, 4, 4)
        if bb4_low is None:
            return False, None, None
        bb4_low = _apply_bb_offset(float(bb4_low), bb_offset_pct, False)
        current_bar_low = float(rates["low"][idx_curr])
        matched = (low <= bb4_low) or (current_bar_low <= bb4_low)
        if matched:
            if current_bar_low <= bb4_low and low > bb4_low:
                current_ts = int(rates["time"][idx_curr])
                current_kst_str = mt5_ts_to_kst(current_ts).strftime("%Y-%m-%d %H:%M") + " KST"
                msg = (
                    f"[4원비] 현재 봉({current_kst_str}) Low={current_bar_low:.5g} <= 4B하단={bb4_low:.5g} 터치 → 실행."
                )
            else:
                msg = (
                    f"[4원비] 직전 봉({bar_kst_str}) Low={low:.5g}, Close={close:.5g} | "
                    f"4B(BB(4,4)) 하단={bb4_low:.5g} → Low <= 4B 터치 충족."
                )
        else:
            msg = (
                f"[4원비] 직전 봉 Low={low:.5g}, 현재 봉 Low={current_bar_low:.5g} | "
                f"4B 하단={bb4_low:.5g} → 터치 미충족."
            )
        return matched, msg, None

    if condition == "기본더블비":
        # 1번: 20B(20,2) 하단 + 4B(4,4) 하단 동시 터치 → Long만. (정배열/숏 조건 삭제)
        # 2번: 정배열 + 4/4 밴드 스퀴즈 + 4/4 밴드 하단 터치 → Long
        if len(rates) < 121:
            rates = get_rates_for_timeframe(symbol, mt5_timeframe, count=125)
        if rates is None or len(rates) < 121:
            return False, None, None
        # 재조회 후 직전 마감 봉 인덱스·데이터 다시 계산(DB/MT5 순서가 다를 수 있음)
        idx_prev = _index_of_last_closed_bar_kst(rates, tf_str) if tf_str else None
        if idx_prev is None:
            t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
            idx_prev = len(rates) - 2 if t0 < t_end else 1
        prev_bar_ts = int(rates["time"][idx_prev])
        bar_from_db = _get_prev_bar_from_db(symbol, tf_str, prev_bar_ts) if tf_str else None
        if bar_from_db is not None and symbol in ("XAUUSD+", "NAS100+"):
            low = float(bar_from_db.get("low", rates["low"][idx_prev]))
            high = float(bar_from_db.get("high", rates["high"][idx_prev]))
        else:
            low = float(rates["low"][idx_prev])
            high = float(rates["high"][idx_prev])
        # 로그 표시용 직전 봉 시각: 현재 KST 기준 한 봉 전으로 통일(타임존/오프셋 이슈 회피)
        bar_kst_str = _last_closed_bar_display_kst(tf_str).strftime("%Y-%m-%d %H:%M") + " KST"
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        db_order = t0 > t_end
        if db_order:
            close_20_range = range(idx_prev, min(idx_prev + 20, len(rates)))
            close_120_range = range(idx_prev, min(idx_prev + 120, len(rates)))
        else:
            close_20_range = range(max(0, idx_prev - 19), idx_prev + 1)
            close_120_range = range(max(0, idx_prev - 119), idx_prev + 1)
        closes_20 = [float(rates["close"][i]) for i in close_20_range if 0 <= i < len(rates)]
        closes_120 = [float(rates["close"][i]) for i in close_120_range if 0 <= i < len(rates)]
        if len(closes_20) < 20 or len(closes_120) < 120:
            return False, None, None
        sma20 = _sma(closes_20, 20)
        sma120 = _sma(closes_120, 120)
        upper20_2 = _bb_upper(closes_20, 20, 2)
        lower20_2 = _bb_lower(closes_20, 20, 2)
        if sma20 is None or sma120 is None or upper20_2 is None or lower20_2 is None:
            return False, None, None
        lower20_2 = _apply_bb_offset(float(lower20_2), bb_offset_pct, False)
        high = float(rates["high"][idx_prev])
        low = float(rates["low"][idx_prev])
        is_correct_arrangement = sma20 > sma120
        # 1번: 20B(20,2) 하단 + 4B(4,4) 하단 동시 터치 → Long만 (정배열/숏 삭제)
        is_touch_20b_lower = low <= lower20_2
        lower_4b_prev = None
        if bar_from_db is not None and bar_from_db.get("bb4_lower") is not None:
            lower_4b_prev = _apply_bb_offset(float(bar_from_db["bb4_lower"]), bb_offset_pct, False)
        if lower_4b_prev is None and len(rates) >= 22:
            def _closes_4_at(bar_idx: int, dbo: bool) -> Optional[list]:
                if dbo:
                    if bar_idx + 4 > len(rates):
                        return None
                    return [float(rates["close"][bar_idx + k]) for k in range(4)]
                if bar_idx < 3:
                    return None
                return [float(rates["close"][bar_idx - k]) for k in range(3, -1, -1)]
            c4 = _closes_4_at(idx_prev, db_order)
            if c4 and len(c4) >= 4:
                lower_4b_prev = _bb_lower(c4, 4, 4)
                if lower_4b_prev is not None:
                    lower_4b_prev = _apply_bb_offset(float(lower_4b_prev), bb_offset_pct, False)
        is_touch_4b_lower = lower_4b_prev is not None and low <= lower_4b_prev
        is_basic_bb = is_touch_20b_lower and is_touch_4b_lower

        # 3번: 20이평·120이평 상승중 + 직전 봉 4B하단 터치 → Long
        is_basic_bb_3 = False
        idx_prev_1 = idx_prev + 1 if db_order else idx_prev - 1
        if not is_basic_bb and is_touch_4b_lower and 0 <= idx_prev_1 < len(rates):
            if db_order:
                close_20_range_prev2 = range(idx_prev_1, min(idx_prev_1 + 20, len(rates)))
                close_120_range_prev2 = range(idx_prev_1, min(idx_prev_1 + 120, len(rates)))
            else:
                close_20_range_prev2 = range(max(0, idx_prev_1 - 19), idx_prev_1 + 1)
                close_120_range_prev2 = range(max(0, idx_prev_1 - 119), idx_prev_1 + 1)
            closes_20_prev2 = [float(rates["close"][j]) for j in close_20_range_prev2 if 0 <= j < len(rates)]
            closes_120_prev2 = [float(rates["close"][j]) for j in close_120_range_prev2 if 0 <= j < len(rates)]
            if len(closes_20_prev2) >= 20 and len(closes_120_prev2) >= 120:
                sma20_prev2 = _sma(closes_20_prev2, 20)
                sma120_prev2 = _sma(closes_120_prev2, 120)
                if sma20_prev2 is not None and sma120_prev2 is not None:
                    is_sma20_rising = sma20 > sma20_prev2
                    is_sma120_rising = sma120 > sma120_prev2
                    is_basic_bb_3 = is_sma20_rising and is_sma120_rising

        # 정배열 + 4/4 스퀴즈 + 4/4 하단 터치 (Long)
        is_basic_bb_4b = False
        bb4_squeeze_msg = ""
        is_bb4_lower_touch = False
        lower_prev = None
        if is_correct_arrangement and len(rates) >= 22:
            def _closes_4_at(bar_idx: int, dbo: bool) -> Optional[list]:
                if dbo:
                    if bar_idx + 4 > len(rates):
                        return None
                    return [float(rates["close"][bar_idx + k]) for k in range(4)]
                if bar_idx < 3:
                    return None
                return [float(rates["close"][bar_idx - k]) for k in range(3, -1, -1)]

            def _bbw4_at(bar_idx: int, dbo: bool):
                c4 = _closes_4_at(bar_idx, dbo)
                if not c4 or len(c4) < 4:
                    return None, None, None
                basis = _sma(c4, 4)
                if basis is None or basis <= 0:
                    return None, None, None
                up = _bb_upper(c4, 4, 4)
                lo = _bb_lower(c4, 4, 4)
                if up is None or lo is None:
                    return None, None, None
                return up, lo, (up - lo) / basis

            BBW4_SMA_LEN = 14
            if bar_from_db is not None and bar_from_db.get("bb4_lower") is not None:
                lower_prev = _apply_bb_offset(float(bar_from_db["bb4_lower"]), bb_offset_pct, False)
            if lower_prev is None:
                up_p, lo_p, _ = _bbw4_at(idx_prev, db_order)
                lower_prev = _apply_bb_offset(float(lo_p), bb_offset_pct, False) if lo_p is not None else None
            if lower_prev is not None:
                idx_prev_1 = idx_prev + 1 if db_order else idx_prev - 1
                if db_order:
                    bbw4_range_cur = range(idx_prev, min(idx_prev + BBW4_SMA_LEN, len(rates)))
                    bbw4_range_prev = range(idx_prev_1, min(idx_prev_1 + BBW4_SMA_LEN, len(rates)))
                else:
                    bbw4_range_cur = range(idx_prev, max(idx_prev - BBW4_SMA_LEN, -1), -1)
                    bbw4_range_prev = range(idx_prev_1, max(idx_prev_1 - BBW4_SMA_LEN, -1), -1)
                bbw4_list_cur = []
                for bi in bbw4_range_cur:
                    _, __, bw = _bbw4_at(bi, db_order)
                    if bw is None:
                        break
                    bbw4_list_cur.append(bw)
                _, __, bbw4_prev_val = _bbw4_at(idx_prev_1, db_order)
                bbw4_list_prev = []
                for bi in bbw4_range_prev:
                    _, __, bw = _bbw4_at(bi, db_order)
                    if bw is None:
                        break
                    bbw4_list_prev.append(bw)
                if len(bbw4_list_cur) >= BBW4_SMA_LEN and bbw4_prev_val is not None and len(bbw4_list_prev) >= BBW4_SMA_LEN:
                    _, __, bbw4_cur = _bbw4_at(idx_prev, db_order)
                    if bbw4_cur is not None:
                        sma_bbw4_cur = sum(bbw4_list_cur) / len(bbw4_list_cur)
                        sma_bbw4_prev = sum(bbw4_list_prev) / len(bbw4_list_prev)
                        is_squeezing = bbw4_cur < sma_bbw4_cur
                        is_prev_squeezing = bbw4_prev_val < sma_bbw4_prev
                        is_bb4_lower_touch = low <= lower_prev
                        is_basic_bb_4b = is_correct_arrangement and is_squeezing and is_prev_squeezing and is_bb4_lower_touch
                        bb4_squeeze_msg = (
                            f" 4/4스퀴즈={is_squeezing} 이전봉스퀴즈={is_prev_squeezing} 4/4하단터치={is_bb4_lower_touch}(Low={low:.5g}<={lower_prev:.5g})"
                            if is_basic_bb_4b else f" 4/4스퀴즈={is_squeezing} 이전봉스퀴즈={is_prev_squeezing} 4/4하단터치(Low<={lower_prev:.5g})={is_bb4_lower_touch}"
                        )

        is_basic_bb = is_basic_bb or is_basic_bb_4b or is_basic_bb_3
        reason_4b = " (정배+4/4스퀴즈+4/4하단)" if is_basic_bb_4b else (" (20·120이평상승+4B하단)" if is_basic_bb_3 else "")
        lower_4b_str = f"{lower_4b_prev:.5g}" if lower_4b_prev is not None else "?"
        msg = (
            f"[기본더블비] 직전 봉({bar_kst_str}) | "
            f"20B하단터치={is_touch_20b_lower}(Low={low:.5g}<={lower20_2:.5g}) 4B하단터치={is_touch_4b_lower}(Low<={lower_4b_str}){bb4_squeeze_msg} → 충족{reason_4b}."
        ) if is_basic_bb else (
            f"[기본더블비] 직전 봉 | "
            f"20B하단터치={is_touch_20b_lower} 4B하단터치={is_touch_4b_lower}{bb4_squeeze_msg} → 미충족."
        )
        brief = None
        if not is_basic_bb and lower_prev is not None and not is_bb4_lower_touch:
            brief = f"4/4하단 ({lower_prev:.2f}) < Low ({low:.2f})"
        # ADX(직전봉) >= 35이면 매수 오더 금지
        if is_basic_bb and len(rates) >= 28:
            chronological = t0 < t_end
            adx_series = _compute_adx_series(rates, 14, 14, chronological)
            if adx_series and idx_prev < len(adx_series) and adx_series[idx_prev] is not None:
                adx_val = adx_series[idx_prev]
                if adx_val >= 35:
                    return False, (
                        f"[기본더블비] 직전 봉({bar_kst_str}) | ADX(직전봉)={adx_val:.1f} ≥ 35 → 매수 금지."
                    ), None
        return is_basic_bb, msg, brief

    if condition == "돌파더블비":
        # 10분봉: 이전봉 돌파 후 45% 되돌림 시 돌파더블비. prev_body_mid = min(open[1],close[1]) + bodySize[1]*0.55, is_breakout = (close>4B상단 or close>20B상단) and (prev_body_mid>=low), is_breakout_new = is_breakout and not is_breakout[1] and not is_breakout[2]
        if tf_str == "M10" and rates is not None and len(rates) >= 25:
            t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
            db_order = t0 > t_end
            idx_1 = idx_prev + 1 if db_order else idx_prev - 1
            idx_2 = idx_prev + 2 if db_order else idx_prev - 2
            idx_3 = idx_prev + 3 if db_order else idx_prev - 3
            if 0 <= idx_3 < len(rates) and (idx_prev - 19 >= 0 if not db_order else idx_prev + 20 <= len(rates)):
                def _upper4_at(ri: int) -> Optional[float]:
                    if db_order:
                        r4 = range(ri, min(ri + 4, len(rates)))
                    else:
                        r4 = range(max(0, ri - 3), ri + 1)
                    closes_4 = [float(rates["close"][k]) for k in r4 if 0 <= k < len(rates)]
                    if len(closes_4) < 4:
                        return None
                    u = _bb_upper(closes_4, 4, 4)
                    return _apply_bb_offset(float(u), bb_offset_pct, True) if u is not None else None

                def _upper20_at(ri: int) -> Optional[float]:
                    if db_order:
                        r20 = range(ri, min(ri + 20, len(rates)))
                    else:
                        r20 = range(max(0, ri - 19), ri + 1)
                    closes_20 = [float(rates["close"][k]) for k in r20 if 0 <= k < len(rates)]
                    if len(closes_20) < 20:
                        return None
                    u = _bb_upper(closes_20, 20, 2)
                    return _apply_bb_offset(float(u), bb_offset_pct, True) if u is not None else None

                def _prev_body_mid(bar_idx: int) -> Optional[float]:
                    """bar_idx 봉의 몸통 55% 높이 (해당 봉이 '돌파한 봉'일 때 그 봉 기준)."""
                    if bar_idx < 0 or bar_idx >= len(rates):
                        return None
                    o = float(rates["open"][bar_idx])
                    c = float(rates["close"][bar_idx])
                    body = abs(c - o)
                    return min(o, c) + body * 0.55

                # 각 bar i에 대해: is_breakout(i) = (이전 봉의 close > 이전 봉의 upper) and (이전 봉의 prev_body_mid >= bar i의 low)
                # 즉 돌파한 봉 = bar i의 이전 봉. prev_body_mid는 그 이전 봉(돌파 봉) 몸통 55%.
                is_breakout_list = []
                for i in [idx_2, idx_1, idx_prev]:
                    prev_i = i + 1 if db_order else i - 1
                    if prev_i < 0 or prev_i >= len(rates):
                        is_breakout_list.append(False)
                        continue
                    low_i = float(rates["low"][i])
                    close_prev = float(rates["close"][prev_i])
                    u4 = _upper4_at(prev_i)
                    u20 = _upper20_at(prev_i)
                    prev_mid = _prev_body_mid(prev_i)
                    if u4 is None or u20 is None or prev_mid is None:
                        is_breakout_list.append(False)
                        continue
                    br = (close_prev > u4 or close_prev > u20) and (prev_mid >= low_i)
                    is_breakout_list.append(br)
                if len(is_breakout_list) == 3:
                    is_breakout_new = is_breakout_list[2] and not is_breakout_list[1] and not is_breakout_list[0]
                    matched = is_breakout_new
                    prev_mid_0 = _prev_body_mid(idx_1)
                    u4_0 = _upper4_at(idx_1)
                    u20_0 = _upper20_at(idx_1)
                    msg = (
                        f"[돌파더블비 10분봉] 직전 봉({bar_kst_str}) 이전봉55%선(prev_body_mid)={prev_mid_0:.5g} 직전봉Low={low:.5g} "
                        f"이전봉Close>4B상단={float(rates['close'][idx_1]) > (u4_0 or 0)} 이전봉Close>20B상단={float(rates['close'][idx_1]) > (u20_0 or 0)} "
                        f"is_breakout_new(직전O/이전X/그이전X)={is_breakout_new} → 충족."
                    ) if matched else (
                        f"[돌파더블비 10분봉] 직전 봉 이전봉55%선={prev_mid_0:.5g} 직전봉Low={low:.5g} "
                        f"is_breakout={is_breakout_list[2]} is_breakout_new={is_breakout_new} → 미충족."
                    )
                    return matched, msg, None
        # 기존 로직: 양봉 + 마감이 4비 상단 돌파 + 윗꼬리 20% 미만 → Long (1시간봉 등)
        open_prev = float(bar_from_db.get("open", rates["open"][idx_prev])) if (bar_from_db and symbol in ("XAUUSD+", "NAS100+")) else float(rates["open"][idx_prev])
        is_bullish = close > open_prev
        bb4_upper = None
        if bar_from_db is not None and bar_from_db.get("bb4_upper") is not None:
            bb4_upper = float(bar_from_db["bb4_upper"])
        if bb4_upper is None:
            close_4_range = range(idx_prev - 3, idx_prev + 1) if t0 < t_end else range(idx_prev, min(idx_prev + 4, len(rates)))
            closes_4 = [float(rates["close"][i]) for i in close_4_range if 0 <= i < len(rates)]
            if len(closes_4) >= 4:
                bb4_upper = _bb_upper(closes_4, 4, 4)
        if bb4_upper is None:
            return False, None, None
        bb4_upper = _apply_bb_offset(float(bb4_upper), bb_offset_pct, True)
        close_above_4b = close > bb4_upper
        range_ = high - low
        if range_ <= 0:
            upper_wick_ratio = 0.0
        else:
            upper_wick = high - close  # 양봉이면 윗꼬리 = high - close
            upper_wick_ratio = upper_wick / range_
        upper_wick_under_20pct = upper_wick_ratio < 0.20
        matched = is_bullish and close_above_4b and upper_wick_under_20pct
        msg = (
            f"[돌파더블비] 직전 봉({bar_kst_str}) 양봉={is_bullish} Close({close:.5g})>4B상단({bb4_upper:.5g})={close_above_4b} "
            f"윗꼬리비율={upper_wick_ratio:.2%}(<20%)={upper_wick_under_20pct} → 충족."
        ) if matched else (
            f"[돌파더블비] 직전 봉 양봉={is_bullish} 4B상단돌파={close_above_4b} 윗꼬리<20%={upper_wick_under_20pct}({upper_wick_ratio:.2%}) → 미충족."
        )
        return matched, msg, None

    if condition == "20B상단":
        # 매도 전용: 직전 봉 음봉 + 이전 봉이 20B상단 터치 & 윗꼬리가 캔들 전체 길이의 20% 이상. ADX(직전봉)≥35.
        t0, t_end = int(rates["time"][0]), int(rates["time"][-1])
        db_order = t0 > t_end
        chronological = t0 < t_end
        idx_prev_1 = (idx_prev + 1) if db_order else (idx_prev - 1)
        if idx_prev_1 < 0 or idx_prev_1 >= len(rates):
            return False, None, None
        # 직전 봉: 음봉
        open_prev = float(rates["open"][idx_prev])
        close_prev = float(rates["close"][idx_prev])
        is_bearish = close_prev < open_prev
        # 이전 봉: 20B 상단 터치 + 윗꼬리 >= 20% of range
        if db_order:
            close_20_range = range(idx_prev_1, min(idx_prev_1 + 20, len(rates)))
        else:
            close_20_range = range(max(0, idx_prev_1 - 19), idx_prev_1 + 1)
        closes_20 = [float(rates["close"][i]) for i in close_20_range if 0 <= i < len(rates)]
        if len(closes_20) < 20:
            return False, None, None
        upper_20 = _bb_upper(closes_20, 20, 2)
        if upper_20 is None:
            return False, None, None
        upper_20 = _apply_bb_offset(float(upper_20), bb_offset_pct, True)
        high_1 = float(rates["high"][idx_prev_1])
        low_1 = float(rates["low"][idx_prev_1])
        open_1 = float(rates["open"][idx_prev_1])
        close_1 = float(rates["close"][idx_prev_1])
        touch_20b_upper = high_1 >= upper_20
        body_top = max(open_1, close_1)
        upper_wick = high_1 - body_top
        range_1 = high_1 - low_1
        upper_wick_ratio = (upper_wick / range_1) if range_1 > 0 else 0.0
        is_upper_wick_20pct = upper_wick_ratio >= 0.2
        matched = is_bearish and touch_20b_upper and is_upper_wick_20pct
        # ADX 35 미만이면 매도 오더 금지
        adx_ok = True
        adx_val = None
        if len(rates) >= 28:
            adx_series = _compute_adx_series(rates, 14, 14, chronological)
            if adx_series and idx_prev < len(adx_series) and adx_series[idx_prev] is not None:
                adx_val = adx_series[idx_prev]
                if adx_val < 35:
                    adx_ok = False
        if not adx_ok and adx_val is not None:
            msg = (
                f"[20B상단] 직전 봉 음봉={is_bearish} | 20B상단터치={touch_20b_upper} 윗꼬리≥20%={is_upper_wick_20pct} | "
                f"ADX(직전봉)={adx_val:.1f} < 35 → 매도 금지."
            )
            return False, msg, None
        if not matched:
            msg = (
                f"[20B상단] 직전 봉 음봉={is_bearish} | 이전 봉 20B상단터치={touch_20b_upper} 윗꼬리≥20%={is_upper_wick_20pct}({upper_wick_ratio:.2%}) → 미충족."
            )
            return False, msg, None
        msg = (
            f"[20B상단] 직전 봉 음봉={is_bearish} | 이전 봉 20B상단터치={touch_20b_upper}(High={high_1:.5g}>={upper_20:.5g}) 윗꼬리비율={upper_wick_ratio:.2%} → 충족."
        )
        return True, msg, None

    return False, None, None


def check_any_entry_condition(
    symbol: str, mt5_timeframe: int, conditions: List[str]
) -> bool:
    """등록된 진입 조건 중 하나라도 만족하면 True."""
    if not conditions:
        return False
    for c in conditions:
        if _check_entry_condition_one(symbol, mt5_timeframe, c):
            return True
    return False


def check_entry_condition_with_detail(
    symbol: str, mt5_timeframe: int, conditions: List[str]
) -> tuple:
    """진입 조건 검사 + 충족 시 해당 조건명과 상세 메시지 반환. (충족여부, 조건명, 상세메시지)."""
    if not conditions:
        return False, None, None
    for c in conditions:
        matched, detail, _ = _check_entry_condition_one_with_detail(symbol, mt5_timeframe, c)
        if matched:
            return True, c, detail
    return False, None, None


def _calc_max_weight_pct_for_margin_target(
    balance: float,
    equity: float,
    margin_used: float,
    symbol: str,
    side: str,
    n_value: float,
    ktr_value: float,
    num_positions: int,
    entry_price: float,
    target_margin_level_pct: float = 200.0,
) -> float:
    """마진레벨이 target_margin_level_pct 이상 유지되도록 사용 가능한 최대 비중(%)을 이진 탐색. 반환: 0~100."""
    # 사용 가능 증거금: equity / (target/100) - margin_used = equity * 100/target - margin_used
    if equity <= 0 or target_margin_level_pct <= 0:
        return 0.0
    max_total_margin = (equity * 100.0 / target_margin_level_pct) - margin_used
    if max_total_margin <= 0:
        return 0.0
    sym_db = symbol_for_db(symbol)
    is_buy = side.upper() == "BUY"
    order_type_market = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
    order_type_limit = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT
    ordinals = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th"]

    def total_margin_for_weight(w_pct: float) -> float:
        if w_pct <= 0:
            return 0.0
        lots_map = get_ktrlots_lots(balance, w_pct, n_value, ktr_value, sym_db, headless=True)
        if not lots_map:
            return float("inf")
        lots_list = [lots_map.get(k, 0) for k in ordinals[:num_positions]]
        total = 0.0
        for i, lot in enumerate(lots_list):
            if lot is None or lot <= 0:
                continue
            if i == 0:
                m = mt5.order_calc_margin(order_type_market, symbol, lot, entry_price)
            else:
                limit_price = (entry_price - ktr_value * i) if is_buy else (entry_price + ktr_value * i)
                m = mt5.order_calc_margin(order_type_limit, symbol, lot, limit_price)
            if m is not None and m > 0:
                total += float(m)
        return total

    lo, hi = 0.1, 100.0
    for _ in range(40):
        mid = (lo + hi) / 2.0
        m = total_margin_for_weight(mid)
        if m <= max_total_margin:
            lo = mid
        else:
            hi = mid
    return round(lo, 2)


def _get_contract_size(symbol: str) -> float:
    """심볼의 계약 규모(USD/랏 기준 가격 1단위 이동 시 손익). MT5 trade_contract_size 우선, 없으면 CONTRACT_BY_SYMBOL."""
    sym = (symbol or "").strip().upper().rstrip("+")
    if tr.init_mt5():
        if mt5.symbol_select(symbol or sym, True):
            info = mt5.symbol_info(symbol or sym)
            if info is not None:
                size = getattr(info, "trade_contract_size", None)
                if size is not None and size > 0:
                    return float(size)
    if sym in CONTRACT_BY_SYMBOL:
        return float(CONTRACT_BY_SYMBOL[sym])
    return 1.0


def _calc_max_loss_usd_ktr(
    use_prev_low: bool,
    lots_list: List[float],
    num_positions: int,
    ktr_price: float,
    contract: float,
    step: Optional[float] = None,
) -> float:
    """전부 S/L 터치 시 최대 손실(USD). 전저점 모드: 12.5*L*step*contract. 일반 N모드: sum lot_i * (N-i-0.5)*KTR * contract."""
    if not lots_list or contract <= 0:
        return 0.0
    if use_prev_low and step is not None and step > 0:
        L = lots_list[0] if lots_list else 0.01
        return 12.5 * L * step * contract
    total = 0.0
    for i in range(min(num_positions, len(lots_list))):
        dist = (num_positions - i - 0.5) * ktr_price
        if dist > 0:
            total += lots_list[i] * contract * dist
    return total


def _calc_lots_and_step_prev_low(
    balance: float,
    entry: float,
    prev_low: float,
    symbol: str,
    side: str,
    target_margin_level_pct: float = 200.0,
) -> Tuple[List[float], float]:
    """전저점 모드: 진입~전저점을 4.5N으로 나눈 스텝과, 전저점까지 가도 청산되지 않도록 역산한 동일 랏수(5개).
    반환: (lots_list 5개, step). BUY면 entry > prev_low, step = (entry - prev_low) / 4.5."""
    is_buy = side.upper() == "BUY"
    num_positions = 5
    if is_buy and prev_low >= entry:
        return [0.01] * num_positions, 0.0
    if not is_buy and prev_low <= entry:
        return [0.01] * num_positions, 0.0
    step = (entry - prev_low) / 4.5 if is_buy else (prev_low - entry) / 4.5
    if step <= 0:
        return [0.01] * num_positions, 0.0
    sym = (symbol or "").strip().upper().rstrip("+")
    contract = CONTRACT_BY_SYMBOL.get(sym, 1.0) or 1.0
    order_type_market = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
    order_type_limit = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT
    total_margin_1 = 0.0
    for i in range(num_positions):
        price = (entry - step * i) if is_buy else (entry + step * i)
        m = mt5.order_calc_margin(order_type_market if i == 0 else order_type_limit, symbol, 1.0, price)
        if m is not None and m > 0:
            total_margin_1 += float(m)
    if total_margin_1 <= 0:
        total_margin_1 = 1.0
    # 전저점 도달 시 손실 = L*contract*step*(4.5+3.5+2.5+1.5+0.5) = 12.5*L*step*contract.
    denom = 12.5 * step * contract + total_margin_1 * (target_margin_level_pct / 100.0)
    if denom <= 0:
        L = 0.01
    else:
        L = balance / denom
    L = max(0.01, min(L, 100.0))
    L = round(L, 2)
    lots_list = [L] * num_positions
    return lots_list, step


def _get_closest_sl_tp_from_other_ktr_positions(
    symbol: str,
    exclude_ticket: int,
    entry_price: float,
    is_buy: bool,
) -> Tuple[Optional[float], Optional[float]]:
    """동일 심볼 다른 KTR 포지션의 S/L·T/P 중 진입가에 가장 가까운 값 반환. (sl, tp) 또는 (None, None)."""
    sym_clean = (symbol or "").rstrip("+")
    all_pos = mt5.positions_get(symbol=symbol) or []
    if not all_pos and sym_clean:
        all_pos = mt5.positions_get(symbol=sym_clean) or []
    others = [
        p for p in all_pos
        if getattr(p, "magic", 0) == MAGIC_KTR and getattr(p, "ticket", 0) != exclude_ticket
    ]
    if not others:
        return None, None
    sl_vals = []
    tp_vals = []
    for p in others:
        sl = getattr(p, "sl", 0) or 0
        tp = getattr(p, "tp", 0) or 0
        if sl > 0:
            sl_vals.append(float(sl))
        if tp > 0:
            tp_vals.append(float(tp))
    sl_best = None
    tp_best = None
    if is_buy:
        # 매수: S/L은 진입가보다 아래, 그중 진입가에 가장 가까운(가장 큰 값)
        below = [s for s in sl_vals if s < entry_price]
        sl_best = max(below) if below else None
        # T/P는 진입가보다 위, 그중 진입가에 가장 가까운(가장 작은 값)
        above = [t for t in tp_vals if t > entry_price]
        tp_best = min(above) if above else None
    else:
        # 매도: S/L은 진입가보다 위, 그중 진입가에 가장 가까운(가장 작은 값)
        above = [s for s in sl_vals if s > entry_price]
        sl_best = min(above) if above else None
        # T/P는 진입가보다 아래, 그중 진입가에 가장 가까운(가장 큰 값)
        below = [t for t in tp_vals if t < entry_price]
        tp_best = max(below) if below else None
    return sl_best, tp_best


def _execute_ktr_entry(
    symbol: str,
    side: str,
    weight_pct: Any,
    n_value: float,
    num_positions: int,
    sl_from_n: bool,
    session: str,
    tf: str,
    tp_option: str,
    sl_option: str,
    log_fn,
    comment_tf: Optional[str] = None,
    order_id: Optional[str] = None,
    entry_conditions: Optional[List[str]] = None,
    source: str = "reservation",
    tp_ktr_multiplier: Optional[float] = None,
    tf_label: Optional[str] = None,
    use_other_positions_sltp: bool = False,
    ktr_multiplier: float = 1.0,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """KTR 1차 시장가 + 2~N차 예약 주문 실행. 반환: (성공 여부, 진입가, 1차 랏수). weight_pct<=0 이면 마진레벨 500%까지 최대 비중 자동 계산."""
    log_fn("--- KTR 진입 시작 ---")
    # tf = KTR 조회용 타임프레임(10M/1H), tf_label = 표시용(10분봉 등), comment_tf = 주문 코멘트용 진입봉
    tf_display = (tf_label or "").strip() or tf
    # 10분봉 진입 시: 1H 직전 봉이 20B 상단 터치면 매수 차단, 20B 하단 터치면 매도 차단
    if (tf or "").strip() in ("10M", "10분봉") or (tf_label or "").strip() == "10분봉":
        touched_upper, touched_lower = _1h_last_closed_bar_touched_20b_upper_or_lower(symbol)
        if (side or "").strip().upper() == "BUY" and touched_upper:
            log_fn("❌ 오더 미실행 사유: 10분봉 매수 진입 시 1H 직전 봉 20B 상단 터치 → 매수 보류.")
            return False, None, None
        if (side or "").strip().upper() == "SELL" and touched_lower:
            log_fn("❌ 오더 미실행 사유: 10분봉 매도 진입 시 1H 직전 봉 20B 하단 터치 → 매도 보류.")
            return False, None, None
    # RSI 시그널 하향 추세(10분봉 직전 봉 RSI < 그 이전 봉 RSI)면 매수 주문 차단
    if (side or "").strip().upper() == "BUY" and _is_rsi_downtrend(symbol):
        log_fn("❌ 오더 미실행 사유: RSI 하향 추세(10분봉 직전 봉 RSI 하락) → 매수 보류.")
        return False, None, None
    # 자동: 진입 시점에 DB에서 가장 최근 기록된 KTR 사용. 그 외: 해당 세션(또는 이전 세션 폴백)
    ktr_value, resolved_session, session_used, tf_used = get_ktr_from_db_with_fallback(symbol, session, tf)
    if session == "자동":
        log_fn(f"심볼: {symbol} | 방향: {side} | 세션: 자동(최근 기록) | TF: {tf_display} | 진입수: {num_positions}")
        if session_used:
            log_fn(f"KTR 세션 자동 → 사용: {session_used} {tf_used} (DB 최근 기록)")
    else:
        resolved_for_log = resolve_ktr_session(symbol, session, tf)
        log_fn(f"심볼: {symbol} | 방향: {side} | 세션: {resolved_for_log} | TF: {tf_display} | 진입수: {num_positions}")
    if ktr_value <= 0:
        log_fn(f"❌ 오더 미실행 사유: KTR DB에 값 없음 ({symbol}/{resolved_session or session}/{tf}). KTR 수동 입력 후 재시도.")
        return False, None, None
    if session_used != resolved_session:
        log_fn(f"⚠️ 해당 세션({resolved_session}) KTR 없음 → 이전 세션({session_used}) KTR 사용: {ktr_value} pt")
    if tf_used != tf:
        log_fn(f"⚠️ {tf} KTR 없음 → {tf_used} KTR 사용: {ktr_value} pt")
    log_fn(f"KTR 값: {ktr_value} pt ({session_used})")
    if ktr_multiplier != 1.0:
        ktr_value = ktr_value * ktr_multiplier
        log_fn(f"KTR 배수 적용: ×{ktr_multiplier} → {ktr_value} pt")

    if not tr.init_mt5():
        log_fn("❌ 오더 미실행 사유: MT5 연결 실패. 터미널 실행 및 로그인 확인.")
        return False, None, None
    balance = mt5.account_info().balance
    acc = tr.get_account_info()
    equity = (acc.get("equity") or balance) if acc else balance
    margin_used = (acc.get("margin") or 0) if acc else 0

    current_weight = _current_ktr_weight_pct(balance)
    if current_weight >= WEIGHT_PCT_MAX_FOR_NEW_ORDER:
        log_fn(
            f"❌ 오더 미실행 사유: 기존 진입 오더 비중 {current_weight:.1f}% (≥{WEIGHT_PCT_MAX_FOR_NEW_ORDER}%) → 추가 오더 생성 금지"
        )
        return False, None, None

    ask, bid = tr.get_market_price(symbol)
    if ask is None:
        log_fn("❌ 오더 미실행 사유: 가격 조회 실패 (종목 선택/연결 상태 확인).")
        return False, None, None
    is_buy = side == "BUY"
    entry = ask if is_buy else bid
    log_fn(f"진입가(시장가): {entry:.5g} (Ask={ask:.5g} Bid={bid:.5g})")

    # KTR 값을 가격에 그대로 반영 (pt × point 변환 없음)
    ktr_price = float(ktr_value)

    # 실시간 오더: 진입가 ±0.5 KTR 안에 이미 KTR 포지션이 있으면 추가 진입 불가
    symbol_clean = (symbol or "").rstrip("+")
    existing = mt5.positions_get(symbol=symbol) or []
    if not existing and symbol_clean:
        existing = mt5.positions_get(symbol=symbol_clean) or []
    ktr_existing = [p for p in existing if getattr(p, "magic", 0) == MAGIC_KTR]
    half_ktr = 0.5 * ktr_price
    for p in ktr_existing:
        po = getattr(p, "price_open", None)
        if po is not None and abs(float(po) - entry) <= half_ktr:
            log_fn(
                f"❌ 오더 미실행 사유: 진입가 {entry:.5g} 기준 ±0.5 KTR({half_ktr:.5g}) 안에 기존 포지션 존재 "
                f"(#{getattr(p, 'ticket', '?')} 진입가 {po:.5g}). 추가 진입 불가."
            )
            return False, None, None

    use_prev_low = isinstance(weight_pct, str) and (weight_pct or "").strip() == "전저점"
    price_step = None  # 전저점일 때 (entry~전저점)/4.5 스텝

    if use_prev_low:
        if _pm_db is None:
            log_fn("❌ 전저점 모드: position_monitor_db를 불러올 수 없습니다.")
            return False, None, None
        try:
            conn = _pm_db.get_connection()
            prev_low = _pm_db.get_min_low_past_4_sessions(conn, symbol)
            conn.close()
        except Exception as e:
            log_fn(f"❌ 전저점 조회 실패: {e}")
            return False, None, None
        if prev_low is None:
            log_fn("❌ 전저점 모드: 과거 4세션 High/Low 데이터 없음. High/Low 버튼으로 먼저 갱신하세요.")
            return False, None, None
        if is_buy and prev_low >= entry:
            log_fn(f"❌ 전저점 모드: 전저점({prev_low:.2f})이 진입가({entry:.2f}) 이상입니다. (매수만 지원)")
            return False, None, None
        if not is_buy and prev_low <= entry:
            log_fn("❌ 전저점 모드: 매도에서는 전저점이 진입가 이하일 수 없습니다.")
            return False, None, None
        num_positions = 5
        lots_list, price_step = _calc_lots_and_step_prev_low(
            balance, entry, prev_low, symbol, side, MARGIN_LEVEL_MIN_PCT
        )
        log_fn(f"전저점 모드(N=4.5): 전저점={prev_low:.2f} | 스텝={(entry - prev_low) / 4.5:.2f} | 5차례 동일 랏수: {lots_list[0]:.2f}")
    else:
        if weight_pct <= 0:
            weight_pct = _calc_max_weight_pct_for_margin_target(
                balance, equity, margin_used, symbol, side, n_value, ktr_value, num_positions, entry, MARGIN_LEVEL_MIN_PCT
            )
            if weight_pct <= 0:
                log_fn(f"❌ 오더 미실행 사유: 마진레벨 {MARGIN_LEVEL_MIN_PCT}%까지 사용 가능한 비중을 계산할 수 없습니다.")
                return False, None, None
            log_fn(f"비중(최대): {weight_pct}% (마진레벨 {MARGIN_LEVEL_MIN_PCT}% 유지)")
        log_fn(f"잔고: ${balance:,.2f} | 리스크: {weight_pct}% | N값: {n_value}")

        n_for_lots = n_value
        lots_map = get_ktrlots_lots(
            balance, float(weight_pct), n_for_lots, ktr_value, symbol_for_db(symbol), headless=True
        )
        if not lots_map:
            log_fn("❌ 오더 미실행 사유: ktrlots 랏수 조회 실패 (balance/KTR/종목 확인).")
            return False, None, None
        ordinals = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"]
        lots_list = [lots_map.get(k, 0) for k in ordinals[:num_positions]]
        MIN_LOT_FALLBACK = 0.01
        lots_list = [MIN_LOT_FALLBACK if (lot is None or lot <= 0) else lot for lot in lots_list]
    ordinals = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"]
    log_fn(f"포지션 수: {num_positions} | 랏수: {lots_list}")

    # 전부 S/L 터치 시 최대 손실이 잔액의 50%를 넘으면 오더 생성 불가 (N값·비중 점검)
    contract = _get_contract_size(symbol)
    max_loss_usd = _calc_max_loss_usd_ktr(
        use_prev_low, lots_list, num_positions, ktr_price, contract,
        step=price_step if use_prev_low else None,
    )
    max_allowed_loss = 0.5 * balance
    if max_loss_usd > max_allowed_loss:
        log_fn(
            f"❌ 오더 미실행 사유: 전 포지션 S/L 시 예상 최대 손실 ${max_loss_usd:,.2f}가 "
            f"잔액의 50%(${max_allowed_loss:,.2f})를 초과합니다. N값 또는 리스크 비중을 낮춰 주세요."
        )
        return False, None, None
    log_fn(f"최대 손실(전부 S/L 시): ${max_loss_usd:,.2f} (잔액 50% 한도 ${max_allowed_loss:,.2f} 이내)")

    tf_for_comment = comment_tf if comment_tf else tf
    if comment_tf and comment_tf != tf:
        log_fn(f"진입(코멘트) TF: {tf_for_comment} (KTR TF {tf}와 다름)")
    comment = _build_order_comment("KTR1", resolved_session, tf_for_comment, tp_option, sl_option, sl_from_n)
    log_fn(f"1차 시장가 주문 요청: {lots_list[0]}랏 @ {entry:.5g} | 코멘트: {comment[:50]}...")
    ok, msg = tr.execute_market_order(
        symbol, side, lots_list[0], magic=MAGIC_KTR, comment=comment
    )
    if not ok:
        log_fn(f"❌ 1차 진입 실패. 사유: {msg}")
        return False, None, None
    log_fn(f"1차 시장가 진입 성공: {lots_list[0]}랏 @ {entry:.5g}")

    # 포지션 반영 대기 후 조회 (MT5 반영 지연 고려)
    symbol_clean = symbol.rstrip("+") if symbol else ""
    for _ in range(5):
        time.sleep(0.4)
        positions = mt5.positions_get(symbol=symbol)
        if not positions and symbol_clean:
            positions = mt5.positions_get(symbol=symbol_clean)
        if positions:
            positions = [p for p in positions if p.magic == MAGIC_KTR]
        if positions:
            break
    if not positions:
        all_pos = mt5.positions_get()
        if all_pos:
            positions = [
                p for p in all_pos
                if p.magic == MAGIC_KTR and (getattr(p, "symbol", "") == symbol or getattr(p, "symbol", "") == symbol_clean)
            ]
    if positions:
        last_pos = max(positions, key=lambda p: p.time)
        pos_symbol = getattr(last_pos, "symbol", symbol)
        sl_1: float = 0.0
        tp_1: float = 0.0
        # N값 없음(실시간 1포지션): 다른 KTR 포지션의 S/L·T/P 중 진입가에 가장 가까운 값 사용.
        # use_other_positions_sltp 플래그 또는 (num_positions==1 and n_value==1.0) 이면 N값 없음으로 간주.
        is_n_value_none_mode = use_other_positions_sltp or (num_positions == 1 and n_value == 1.0)
        if num_positions == 1 and is_n_value_none_mode:
            sl_from_others, tp_from_others = _get_closest_sl_tp_from_other_ktr_positions(
                symbol, last_pos.ticket, entry, is_buy
            )
            if sl_from_others is not None and sl_from_others > 0:
                sl_1 = sl_from_others
                log_fn(f"  1차 S/L 설정: 다른 KTR 포지션 중 진입가에 가장 가까운 S/L = {sl_1:.5g}")
            if tp_from_others is not None and tp_from_others > 0:
                tp_1 = tp_from_others
                log_fn(f"  T/P 설정: 다른 KTR 포지션 중 진입가에 가장 가까운 T/P = {tp_1:.5g}")
            if (sl_from_others is None or sl_from_others <= 0) and (tp_from_others is None or tp_from_others <= 0):
                log_fn("  다른 KTR 포지션 없음 → S/L·T/P 미설정")
            if sl_1 > 0 or tp_1 > 0:
                ok_sltp, msg_sltp = tr.modify_position_sltp(last_pos.ticket, pos_symbol, sl_1, tp_1)
                if not ok_sltp:
                    log_fn(f"  ⚠️ 1차 S/L·T/P 설정 실패: {msg_sltp}")
        else:
            # 1차 주문 손절: 진입가 ± (N-0.5)KTR (항상 1 KTR 단위 오더 기준)
            sl_1 = (entry - (num_positions - 0.5) * ktr_price) if is_buy else (entry + (num_positions - 0.5) * ktr_price)
            if tp_ktr_multiplier is not None and tp_ktr_multiplier > 0:
                tp_1 = entry + (ktr_price * tp_ktr_multiplier) if is_buy else entry - (ktr_price * tp_ktr_multiplier)
            ok_sltp, msg_sltp = tr.modify_position_sltp(last_pos.ticket, pos_symbol, sl_1, tp_1)
            if ok_sltp:
                log_fn(f"  1차 S/L 설정: 진입가 ± (N-0.5)KTR = {sl_1:.5g}")
                if tp_1 > 0:
                    log_fn(f"  T/P 설정: 진입가 {'+' if is_buy else '-'} (KTR×{tp_ktr_multiplier}) = {tp_1:.5g}")
            else:
                log_fn(f"  ⚠️ 1차 S/L (및 T/P) 설정 실패: {msg_sltp}")
    step_for_limit = (price_step if use_prev_low and price_step is not None else ktr_price)
    for i in range(1, num_positions):
        lot = lots_list[i] if i < len(lots_list) else 0
        if lot <= 0:
            log_fn(f"{ordinals[i]} 예약 생략: 랏수 0")
            continue
        limit_price = (entry - step_for_limit * i) if is_buy else (entry + step_for_limit * i)
        j = i + 1
        if num_positions == 2 and j == 2:
            mult_j = 1.5
        else:
            mult_j = num_positions - j + 0.5
        # 손절 = 해당 포지션 체결가(limit_price) 기준 - mult_j KTR (1차는 시장가 기준 (N-0.5)KTR)
        sl_j = (limit_price - mult_j * ktr_price) if is_buy else (limit_price + mult_j * ktr_price)
        pending_comment = _build_order_comment(
            f"KTR{i+1}", resolved_session, tf_for_comment, tp_option, sl_option, sl_from_n
        )
        log_fn(f"{ordinals[i]} 예약 주문 요청: {lot}랏 @ {limit_price:.2f} S/L={sl_j:.5g} (체결가 기준 -{mult_j} KTR)")
        ok, msg = tr.place_pending_limit(
            symbol, side, lot, limit_price, sl=sl_j, tp=0.0, magic=MAGIC_KTR, comment=pending_comment
        )
        if ok:
            log_fn(f"{ordinals[i]} 예약 주문 성공: {lot}랏 @ {limit_price:.2f} S/L={sl_j:.5g}")
        else:
            log_fn(f"❌ {ordinals[i]} 예약 실패. 사유: {msg}")
        # 연속 예약 시 조정된 가격이 겹치지 않도록 다음 주문 전 짧은 대기 (시세 갱신)
        if i < num_positions - 1:
            time.sleep(0.4)
        # +1 KTR 예약은 실시간 오더에서 "+KTR" 체크 시에만 예약 목록으로 추가되며, 가격 도달 시 Stop 주문으로 실행됨 (중복·Invalid price 방지)

    log_fn("--- KTR 진입 완료 ---")
    return True, entry, lots_list[0]


def _normalize_loaded_reservation(r: Dict[str, Any]) -> Dict[str, Any]:
    """로드된 예약 1건 정규화. conditions를 항상 리스트로, 기타 필드 호환."""
    if not isinstance(r, dict):
        return r
    cond = r.get("conditions")
    if isinstance(cond, str):
        r = {**r, "conditions": [cond] if cond else [ENTRY_CONDITIONS[0]]}
    elif not isinstance(cond, list):
        r = {**r, "conditions": []}
    return r


def load_reservations(path: Optional[str] = None) -> List[Dict[str, Any]]:
    """저장된 예약 목록 로드. path 미지정 시 RESERVATIONS_PATH 사용. 파일 없거나 오류 시 빈 목록."""
    p = path if path else RESERVATIONS_PATH
    if not p or not os.path.isfile(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        return [_normalize_loaded_reservation(r) for r in data]
    except Exception:
        return []


def save_reservations(items: List[Dict[str, Any]], path: Optional[str] = None) -> None:
    """예약 목록을 ktr_reservations.json에 저장. path 미지정 시 RESERVATIONS_PATH 사용. 폴더 없으면 생성."""
    p = path if path else RESERVATIONS_PATH
    if not p:
        return
    dir_path = os.path.dirname(p)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


# ---------- 포지션 SL/TP 수정 서브 창 (실시간 오더 탭에서 사용) ----------
class PositionSltpEditorWindow:
    """오픈 포지션 중 하나를 선택해 차익실현/손절 옵션 적용 및 override 저장."""

    TP_OPTIONS = ("20이평", "120이평", "20B상단", "4B상단")
    SL_OPTIONS = ("N기준", "잔액비 -10%", "잔액비 -20%", "잔액비 50%", "사용하지 않음")
    SESSIONS = ("자동",)
    TFS = ("10M", "1H")

    def __init__(self, parent: tk.Toplevel, log_fn=None):
        self.win = parent
        self.win.title("포지션 SL/TP 수정")
        self.win.geometry("620x480")
        self.log_fn = log_fn or (lambda msg: None)
        self.positions = []
        self._build_ui()
        self._refresh_positions()

    def _build_ui(self):
        f = ttk.Frame(self.win, padding=10)
        f.pack(fill=tk.BOTH, expand=True)
        ttk.Label(f, text="오픈 포지션 (KTR)").grid(row=0, column=0, sticky=tk.W, pady=(0, 4))
        cols = ("ticket", "symbol", "type", "volume", "comment")
        self.tree = ttk.Treeview(f, columns=cols, show="headings", height=6, selectmode="browse")
        self.tree.heading("ticket", text="티켓")
        self.tree.heading("symbol", text="심볼")
        self.tree.heading("type", text="매수/매도")
        self.tree.heading("volume", text="랏")
        self.tree.heading("comment", text="코멘트")
        for c in cols:
            self.tree.column(c, width=90)
        self.tree.column("comment", width=220)
        self.tree.grid(row=1, column=0, columnspan=2, sticky=tk.NSEW, pady=(0, 8))
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        ttk.Button(f, text="목록 새로고침", command=self._refresh_positions).grid(row=2, column=0, sticky=tk.W, pady=(0, 8))
        row = 3
        ttk.Separator(f, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=tk.EW, pady=8)
        row += 1
        ttk.Label(f, text="차익실현").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_tp = tk.StringVar(value=self.TP_OPTIONS[0])
        tp_f = ttk.Frame(f)
        tp_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in self.TP_OPTIONS:
            ttk.Radiobutton(tp_f, text=val, variable=self.var_tp, value=val).pack(side=tk.LEFT, padx=(0, 8))
        row += 1
        ttk.Label(f, text="손절").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_sl = tk.StringVar(value=self.SL_OPTIONS[1])
        sl_f = ttk.Frame(f)
        sl_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in self.SL_OPTIONS:
            ttk.Radiobutton(sl_f, text=val, variable=self.var_sl, value=val).pack(side=tk.LEFT, padx=(0, 8))
        row += 1
        ttk.Label(f, text="KTR 세션/타임프레임 (N기준 손절 시)").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_session = tk.StringVar(value="자동")
        self.var_tf = tk.StringVar(value="1H")
        st_f = ttk.Frame(f)
        st_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        ttk.Combobox(st_f, textvariable=self.var_session, values=list(self.SESSIONS), width=8, state="readonly").pack(side=tk.LEFT, padx=(0, 8))
        ttk.Combobox(st_f, textvariable=self.var_tf, values=list(self.TFS), width=6, state="readonly").pack(side=tk.LEFT)
        row += 1
        ttk.Button(f, text="적용 (SL/TP 반영 + 옵션 저장)", command=self._on_apply).grid(row=row, column=0, columnspan=2, pady=10)
        f.grid_rowconfigure(1, weight=1)
        f.grid_columnconfigure(1, weight=1)

    def _refresh_positions(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        self.positions = []
        if not tr.init_mt5():
            return
        positions = mt5.positions_get()
        positions = [p for p in positions if getattr(p, "magic", 0) == MAGIC_KTR]
        overrides = load_sltp_overrides()
        for p in positions:
            comment = getattr(p, "comment", "") or ""
            ov = overrides.get(str(p.ticket))
            if ov:
                comment = f"[적용] TP:{ov.get('tp_option','')} SL:{ov.get('sl_option','')}"
            side = "매수" if p.type == mt5.ORDER_TYPE_BUY else "매도"
            self.tree.insert("", tk.END, values=(p.ticket, p.symbol, side, p.volume, comment[:40]))
            self.positions.append(p)
        if not self.positions:
            self.tree.insert("", tk.END, values=("—", "—", "—", "—", "KTR 오픈 포지션 없음"))

    def _on_select(self, event):
        sel = self.tree.selection()
        if not sel or not self.positions:
            return
        idx = self.tree.index(sel[0])
        if idx >= len(self.positions):
            return
        pos = self.positions[idx]
        parsed = _parse_comment(getattr(pos, "comment", "") or "")
        ov = load_sltp_overrides().get(str(pos.ticket))
        if ov:
            self.var_tp.set(ov.get("tp_option", self.TP_OPTIONS[0]))
            self.var_sl.set(ov.get("sl_option", self.SL_OPTIONS[1]))
            self.var_session.set(ov.get("session", "자동"))
            self.var_tf.set(ov.get("timeframe", "1H"))
        elif parsed:
            tp_option, sl_option, session, timeframe = parsed
            self.var_tp.set(tp_option)
            self.var_sl.set("N기준" if sl_option == "N" else sl_option)
            self.var_session.set(session)
            self.var_tf.set(timeframe)

    def _on_apply(self):
        sel = self.tree.selection()
        if not sel or not self.positions:
            messagebox.showwarning("선택 필요", "포지션을 선택하세요.")
            return
        idx = self.tree.index(sel[0])
        if idx >= len(self.positions):
            return
        pos = self.positions[idx]
        tp_option = self.var_tp.get()
        sl_option = self.var_sl.get()
        session = self.var_session.get()
        timeframe = self.var_tf.get()
        sym = pos.symbol
        resolved_session = resolve_ktr_session(sym, session, timeframe)
        sl_option_for_calc = "N" if sl_option == "N기준" else sl_option
        is_buy = pos.type == mt5.ORDER_TYPE_BUY
        balance = mt5.account_info().balance
        tp_level = get_tp_level(sym, tp_option)
        if sl_option == "N기준":
            sl_price = _compute_sl_for_n(sym, is_buy, resolved_session, timeframe)
        else:
            sl_price = get_sl_price(sym, is_buy, pos.price_open, pos.volume, balance, sl_option_for_calc)
        sl_f = sl_price if sl_price and sl_price > 0 else 0.0
        tp_f = tp_level if tp_level and tp_level > 0 else 0.0
        if sl_f <= 0 and tp_f <= 0:
            messagebox.showinfo("적용", "적용할 SL/TP 값이 없습니다.")
            return
        ok, msg = tr.modify_position_sltp(pos.ticket, sym, sl_f, tp_f)
        if not ok:
            messagebox.showerror("오류", f"SL/TP 수정 실패: {msg}")
            return
        save_sltp_override(pos.ticket, tp_option, sl_option_for_calc, resolved_session, timeframe)
        self.log_fn(f"포지션 #{pos.ticket} SL/TP 적용 및 옵션 저장 완료 (TP:{tp_option} SL:{sl_option_for_calc})")
        messagebox.showinfo("적용", f"#{pos.ticket} SL/TP 반영 및 적용 옵션 저장 완료.")
        self._refresh_positions()


class KTRReservationApp:
    def __init__(self):
        # 저장 파일 경로 일치를 위해 작업 디렉터리를 스크립트 폴더로 고정 (다른 위치에서 실행해도 동일 경로 사용)
        try:
            os.chdir(_SCRIPT_DIR)
        except Exception:
            pass
        # chdir 직후 기준 경로 사용 → 실행 위치와 관계없이 항상 같은 파일 로드/저장
        self._reservations_path = RESERVATIONS_PATH
        self._reservations_lock = threading.Lock()
        # +KTR 되돌림: 가격이 +KTR 초과 후 다시 +KTR로 돌아왔을 때 시장가 실행 대기 목록
        self._retrace_lock = threading.Lock()
        self._realtime_ktr_retrace_list: List[Dict[str, Any]] = []
        self.root = tk.Tk()
        self.root.title("KTR 예약·실시간 오더")
        self.root.minsize(650, 646)
        self.root.geometry("650x646")
        self.reservations: List[Dict[str, Any]] = []
        self._editing_index: Optional[int] = None  # 수정 시 선택된 예약 인덱스
        self.monitor_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        # 봉 마감 텔레그램 중복 전송 방지: tf_label -> "YYYY-MM-DD HH:MM" (마지막 전송한 봉 시각)
        self._last_bar_telegram_sent: Dict[str, str] = {}
        self._build_ui()
        # 저장된 예약 오더 로드 (ktr_reservations.json) → 재실행 시 목록 복원
        self._load_and_refresh_list()
        # 실행 시 모니터링 자동 시작
        self.root.after(100, self._on_start_monitor)
        # KTR 테이블 누락 레코드 확인 후 입력 다이얼로그 (약간 지연하여 창이 뜬 뒤 표시)
        self.root.after(600, self._check_and_show_missing_ktr)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        f = ttk.Frame(self.root, padding=6)
        f.pack(fill=tk.BOTH, expand=True)

        notebook = ttk.Notebook(f)
        notebook.grid(row=0, column=0, columnspan=3, sticky=tk.NSEW)

        # ----- 탭 1: 예약 오더 -----
        tab_res = ttk.Frame(notebook, padding=4)
        notebook.add(tab_res, text="예약 오더")
        res_f = ttk.Frame(tab_res)
        res_f.pack(fill=tk.BOTH, expand=True)

        # 상단: 예약 입력
        top_row_f = ttk.Frame(res_f)
        top_row_f.grid(row=0, column=0, sticky=tk.NSEW, pady=(0, 4))
        res_f.grid_columnconfigure(0, weight=1)

        left_f = ttk.Frame(top_row_f)
        left_f.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        row = 0
        mon_f = ttk.Frame(left_f)
        mon_f.grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=(0, 8))
        self.monitor_status_var = tk.StringVar(value="모니터링: 실행 중")
        ttk.Label(mon_f, textvariable=self.monitor_status_var).pack(side=tk.LEFT, padx=(0, 0))
        row += 1

        ttk.Label(left_f, text="심볼").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_symbol = tk.StringVar(value="NAS100+")
        sym_f = ttk.Frame(left_f)
        sym_f.grid(row=row, column=1, columnspan=2, sticky=tk.W, pady=2)
        for val in ("NAS100+", "XAUUSD+"):
            ttk.Radiobutton(
                sym_f, text=val, variable=self.var_symbol, value=val,
                command=self._refresh_res_ktr,
            ).pack(side=tk.LEFT, padx=(0, 12))
        row += 1

        ttk.Label(left_f, text="매수/매도").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_side = tk.StringVar(value="BUY")
        side_f = ttk.Frame(left_f)
        side_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        ttk.Radiobutton(side_f, text="매수", variable=self.var_side, value="BUY").pack(
            side=tk.LEFT, padx=(0, 12)
        )
        ttk.Radiobutton(side_f, text="매도", variable=self.var_side, value="SELL").pack(
            side=tk.LEFT
        )
        row += 1

        ttk.Label(left_f, text="타임프레임").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_tf_monitor = tk.StringVar(value="1시간봉")
        tf_f = ttk.Frame(left_f)
        tf_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in RESERVATION_TF_OPTIONS:
            if val in TF_MAP:
                ttk.Radiobutton(
                    tf_f, text=val, variable=self.var_tf_monitor, value=val
                ).pack(side=tk.LEFT, padx=(0, 8))
        row += 1

        ttk.Label(left_f, text="진입 조건").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_entry_condition = tk.StringVar(value=ENTRY_CONDITIONS[0])
        cond_f = ttk.Frame(left_f)
        cond_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in ENTRY_CONDITIONS:
            ttk.Radiobutton(cond_f, text=val, variable=self.var_entry_condition, value=val).pack(
                side=tk.LEFT, padx=(0, 12)
            )
        ttk.Label(left_f, text="진입시간").grid(row=row, column=2, sticky=tk.W, padx=(24, 0), pady=2)
        self.var_entry_time = tk.StringVar(value="")
        self.entry_time_entry = ttk.Entry(left_f, textvariable=self.var_entry_time, width=12)
        self.entry_time_entry.grid(row=row, column=3, sticky=tk.W, pady=2)
        row += 1

        ttk.Label(left_f, text="비중").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_weight = tk.StringVar(value="1%")
        weight_f = ttk.Frame(left_f)
        weight_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in ("1%", "2.5%", "5%", "10%"):
            ttk.Radiobutton(
                weight_f, text=val, variable=self.var_weight, value=val
            ).pack(side=tk.LEFT, padx=(0, 12))
        row += 1

        ttk.Label(left_f, text="N값").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_n = tk.StringVar(value="2.5")
        n_f = ttk.Frame(left_f)
        n_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in ("1.5", "2.5", "3.5", "4.5", "없음"):
            ttk.Radiobutton(n_f, text=val, variable=self.var_n, value=val).pack(
                side=tk.LEFT, padx=(0, 8)
            )
        row += 1

        ttk.Label(left_f, text="KTR 타임프레임").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.var_ktr_tf = tk.StringVar(value="10M")
        ktr_tf_f = ttk.Frame(left_f)
        ktr_tf_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        for val in ("10M", "1H"):
            ttk.Radiobutton(
                ktr_tf_f, text=val, variable=self.var_ktr_tf, value=val,
                command=self._refresh_res_ktr,
            ).pack(side=tk.LEFT, padx=(0, 12))
        row += 1

        ttk.Label(left_f, text="KTR 값 (선택)").grid(row=row, column=0, sticky=tk.W, pady=2)
        res_ktr_f = ttk.Frame(left_f)
        res_ktr_f.grid(row=row, column=1, sticky=tk.W, pady=2)
        self.lbl_res_ktr = ttk.Label(res_ktr_f, text="—", font=("", 10, "bold"))
        self.lbl_res_ktr.pack(side=tk.LEFT)
        self.var_res_ktr_x2 = tk.BooleanVar(value=False)
        ttk.Checkbutton(res_ktr_f, text="KTR X2", variable=self.var_res_ktr_x2, command=self._refresh_res_ktr).pack(side=tk.LEFT, padx=(12, 0))
        ttk.Label(left_f, text="KTR 세션").grid(row=row, column=2, sticky=tk.W, padx=(24, 0), pady=2)
        self.var_session = tk.StringVar(value="자동")
        session_f = ttk.Frame(left_f)
        session_f.grid(row=row, column=3, sticky=tk.W, pady=2)
        ttk.Radiobutton(session_f, text="자동", variable=self.var_session, value="자동", command=self._refresh_res_ktr).pack(side=tk.LEFT, padx=(0, 12))
        row += 1

        # 볼린저 밴드 오프셋 % (심볼별, 저장 시 파일 덮어쓰기). 지수(나스닥)는 절대가격이 커서 오프셋 %를 더 크게 설정 권장.
        bb_offset_f = ttk.LabelFrame(left_f, text="볼린저 밴드 오프셋 % (골드 0.5% / 지수 2.5% 권장)")
        bb_offset_f.grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=4)
        row += 1
        loaded_bb = _load_bb_offset()
        self.var_bb_offset = {}
        for c, sym in enumerate(BB_OFFSET_SYMBOLS):
            ttk.Label(bb_offset_f, text=sym).grid(row=0, column=c * 2, sticky=tk.W, padx=(8, 4), pady=4)
            v = tk.StringVar(value=str(loaded_bb.get(sym, "")) if loaded_bb.get(sym) not in (None, "") else "0")
            self.var_bb_offset[sym] = v
            ttk.Entry(bb_offset_f, textvariable=v, width=8).grid(row=0, column=c * 2 + 1, sticky=tk.W, padx=(0, 16), pady=4)
        ttk.Button(bb_offset_f, text="BB 오프셋 저장", command=self._on_save_bb_offset).grid(row=0, column=4, padx=8, pady=4)
        ttk.Button(bb_offset_f, text="오프셋 그래프", command=self._on_show_offset_graph).grid(row=0, column=5, padx=4, pady=4)
        row += 1

        # 차익실현/손절 설정 기능 제거 → 오더 시 T/P·S/L 미설정. 코멘트용 기본값만 유지
        self.var_tp = tk.StringVar(value="사용하지 않음")
        self.var_sl = tk.StringVar(value="사용하지 않음")

        # 예약 목록: 상단 절반 (list_section_f)
        list_section_f = ttk.Frame(res_f)
        list_section_f.grid(row=1, column=0, columnspan=3, sticky=tk.NSEW, pady=(8, 4))
        res_f.grid_rowconfigure(1, weight=1)
        row += 1
        list_header_f = ttk.Frame(list_section_f)
        list_header_f.grid(row=0, column=0, sticky=tk.W, pady=(0, 2))
        cols = (
            "symbol",
            "side",
            "tf",
            "conditions",
            "entry_time",
            "weight",
            "n",
            "session",
            "active",
            "created",
        )
        self.tree = ttk.Treeview(
            list_section_f, columns=cols, show="headings", height=4, selectmode="browse"
        )
        self.tree.heading("symbol", text="심볼")
        self.tree.heading("side", text="매수/매도")
        self.tree.heading("tf", text="타임프레임")
        self.tree.heading("conditions", text="진입조건")
        self.tree.heading("entry_time", text="진입시간")
        self.tree.heading("weight", text="비중")
        self.tree.heading("n", text="N")
        self.tree.heading("session", text="KTR세션")
        self.tree.heading("active", text="활성")
        self.tree.heading("created", text="등록일시")
        for c in cols:
            self.tree.column(c, width=72)
        self.tree.column("conditions", width=140)
        self.tree.column("entry_time", width=72)
        self.tree.column("created", width=100)
        self.tree.grid(row=1, column=0, sticky=tk.NSEW, pady=2)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        list_section_f.grid_rowconfigure(1, weight=1)
        list_section_f.grid_columnconfigure(0, weight=1)
        list_btn_f = ttk.Frame(list_section_f)
        list_btn_f.grid(row=2, column=0, sticky=tk.W, pady=2)
        self.btn_add_reservation = ttk.Button(list_btn_f, text="예약 추가", command=self._on_add_reservation)
        self.btn_add_reservation.pack(side=tk.LEFT, padx=4)
        ttk.Button(list_btn_f, text="선택 예약 활성화", command=lambda: self._on_set_active(True)).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(list_btn_f, text="선택 예약 비활성화", command=lambda: self._on_set_active(False)).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(list_btn_f, text="선택 예약 수정", command=self._on_edit_reservation).pack(
            side=tk.LEFT, padx=4
        )
        self.btn_save_edit = ttk.Button(
            list_btn_f, text="수정 반영 (저장)", command=self._on_save_edited_reservation
        )
        self.btn_cancel_edit = ttk.Button(
            list_btn_f, text="수정 취소", command=self._cancel_edit_mode
        )
        ttk.Button(list_btn_f, text="선택 예약 삭제", command=self._on_delete).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(list_btn_f, text="새로고침", command=self._load_and_refresh_list).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(list_btn_f, text="시뮬레이터 실행", command=self._on_launch_simulator).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(list_btn_f, text="AI 시뮬레이터", command=self._on_launch_ai_simulator).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(list_btn_f, text="프로그램 종료", command=self._on_close).pack(
            side=tk.LEFT, padx=(12, 4)
        )
        res_f.grid_columnconfigure(1, weight=1)

        # ----- 탭 2: 실시간 오더 (스크롤 가능, 좁은 창에서도 전부 보이도록) -----
        tab_rt = ttk.Frame(notebook, padding=4)
        notebook.add(tab_rt, text="실시간 오더")
        notebook.select(tab_rt)  # 실행 시 실시간 오더 탭이 기본 선택
        rt_canvas = tk.Canvas(tab_rt, highlightthickness=0)
        rt_vscroll = ttk.Scrollbar(tab_rt, orient=tk.VERTICAL, command=rt_canvas.yview)
        rt_hscroll = ttk.Scrollbar(tab_rt, orient=tk.HORIZONTAL, command=rt_canvas.xview)
        rt_canvas.configure(yscrollcommand=rt_vscroll.set, xscrollcommand=rt_hscroll.set)
        rt_vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        rt_hscroll.pack(side=tk.BOTTOM, fill=tk.X)
        rt_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        rt_f = ttk.Frame(rt_canvas)
        rt_win = rt_canvas.create_window((0, 0), window=rt_f, anchor=tk.NW)
        MIN_RT_WIDTH = 520
        rt_f.update_idletasks()
        rt_canvas.configure(scrollregion=(0, 0, max(MIN_RT_WIDTH, rt_f.winfo_reqwidth()), rt_f.winfo_reqheight()))

        def _rt_on_canvas_configure(evt):
            w = max(MIN_RT_WIDTH, evt.width)
            rt_canvas.itemconfig(rt_win, width=w)
            rt_canvas.configure(scrollregion=(0, 0, w, rt_f.winfo_reqheight()))

        def _rt_on_frame_configure(evt):
            rt_canvas.configure(scrollregion=rt_canvas.bbox("all"))

        def _rt_bind_mousewheel(evt):
            rt_canvas.yview_scroll(int(-1 * (evt.delta / 120)), "units")

        rt_f.bind("<Configure>", _rt_on_frame_configure)
        rt_canvas.bind("<Configure>", _rt_on_canvas_configure)
        rt_canvas.bind("<MouseWheel>", _rt_bind_mousewheel)

        r = 0
        ttk.Label(rt_f, text="심볼").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_symbol = tk.StringVar(value="XAUUSD+")
        sym_rt = ttk.Frame(rt_f)
        sym_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        for val in ("NAS100+", "XAUUSD+"):
            ttk.Radiobutton(sym_rt, text=val, variable=self.var_rt_symbol, value=val, command=self._refresh_rt_ktr).pack(side=tk.LEFT, padx=(0, 12))
        r += 1
        ttk.Label(rt_f, text="매수/매도").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_side = tk.StringVar(value="BUY")
        side_rt = ttk.Frame(rt_f)
        side_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        ttk.Radiobutton(side_rt, text="매수", variable=self.var_rt_side, value="BUY").pack(side=tk.LEFT, padx=(0, 12))
        ttk.Radiobutton(side_rt, text="매도", variable=self.var_rt_side, value="SELL").pack(side=tk.LEFT)
        r += 1
        ttk.Label(rt_f, text="타임프레임").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_entry_tf = tk.StringVar(value="10분봉")
        entry_tf_rt = ttk.Frame(rt_f)
        entry_tf_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        for val in RESERVATION_TF_OPTIONS:
            if val in TF_MAP:
                ttk.Radiobutton(entry_tf_rt, text=val, variable=self.var_rt_entry_tf, value=val).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Separator(entry_tf_rt, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=12)
        self.var_rt_m10_bb_auto = tk.BooleanVar(value=False)
        ttk.Checkbutton(entry_tf_rt, text="10분봉 4B/20B 자동오더", variable=self.var_rt_m10_bb_auto).pack(side=tk.LEFT, padx=(0, 8))
        self.var_rt_m10_bb_weight = tk.StringVar(value="1%")
        ttk.Radiobutton(entry_tf_rt, text="1%", variable=self.var_rt_m10_bb_weight, value="1%").pack(side=tk.LEFT, padx=(0, 4))
        ttk.Radiobutton(entry_tf_rt, text="2.5%", variable=self.var_rt_m10_bb_weight, value="2.5%").pack(side=tk.LEFT, padx=(0, 4))
        ttk.Radiobutton(entry_tf_rt, text="5%", variable=self.var_rt_m10_bb_weight, value="5%").pack(side=tk.LEFT, padx=(0, 4))
        ttk.Radiobutton(entry_tf_rt, text="10%", variable=self.var_rt_m10_bb_weight, value="10%").pack(side=tk.LEFT)
        r += 1
        ttk.Label(rt_f, text="진입 조건").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_entry_condition = tk.StringVar(value="돌파더블비")
        cond_rt_f = ttk.Frame(rt_f)
        cond_rt_f.grid(row=r, column=1, sticky=tk.W, pady=2)
        for val in ENTRY_CONDITIONS:
            ttk.Radiobutton(cond_rt_f, text=val, variable=self.var_rt_entry_condition, value=val).pack(
                side=tk.LEFT, padx=(0, 8)
            )
        r += 1
        ttk.Label(rt_f, text="비중").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_weight = tk.StringVar(value="1%")
        w_rt = ttk.Frame(rt_f)
        w_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        for val in ("1%", "2.5%", "5%", "10%"):
            ttk.Radiobutton(w_rt, text=val, variable=self.var_rt_weight, value=val).pack(side=tk.LEFT, padx=(0, 12))
        r += 1
        ttk.Label(rt_f, text="N값").grid(row=r, column=0, sticky=tk.W, pady=2)
        n_rt = ttk.Frame(rt_f)
        n_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        self.var_rt_n = tk.StringVar(value="1.5")
        for val in ("1.5", "2.5", "3.5", "4.5", "없음"):
            ttk.Radiobutton(n_rt, text=val, variable=self.var_rt_n, value=val, command=self._on_rt_n_changed).pack(side=tk.LEFT, padx=(0, 8))
        r += 1
        ttk.Label(rt_f, text="KTR 타임프레임").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_tf = tk.StringVar(value="10M")
        tf_rt = ttk.Frame(rt_f)
        tf_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        for val in ("10M", "1H"):
            ttk.Radiobutton(tf_rt, text=val, variable=self.var_rt_tf, value=val, command=self._refresh_rt_ktr).pack(side=tk.LEFT, padx=(0, 12))
        r += 1
        ttk.Label(rt_f, text="KTR 세션 / +KTR").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_session = tk.StringVar(value="자동")
        sess_rt = ttk.Frame(rt_f)
        sess_rt.grid(row=r, column=1, sticky=tk.W, pady=2)
        ttk.Radiobutton(sess_rt, text="자동", variable=self.var_rt_session, value="자동", command=self._refresh_rt_ktr).pack(side=tk.LEFT, padx=(0, 12))
        self.var_rt_add_ktr = tk.BooleanVar(value=True)
        ttk.Checkbutton(sess_rt, text="+KTR", variable=self.var_rt_add_ktr).pack(side=tk.LEFT, padx=(16, 4))
        self.var_rt_add_ktr_mult = tk.StringVar(value="1")
        ttk.Radiobutton(sess_rt, text="1", variable=self.var_rt_add_ktr_mult, value="1").pack(side=tk.LEFT, padx=(0, 4))
        ttk.Radiobutton(sess_rt, text="2", variable=self.var_rt_add_ktr_mult, value="2").pack(side=tk.LEFT, padx=(0, 0))
        r += 1
        ttk.Label(rt_f, text="KTR 값 (선택)").grid(row=r, column=0, sticky=tk.W, pady=2)
        rt_ktr_f = ttk.Frame(rt_f)
        rt_ktr_f.grid(row=r, column=1, sticky=tk.W, pady=2)
        self.lbl_rt_ktr = ttk.Label(rt_ktr_f, text="—", font=("", 10, "bold"))
        self.lbl_rt_ktr.pack(side=tk.LEFT)
        self.var_rt_ktr_x2 = tk.BooleanVar(value=False)
        ttk.Checkbutton(rt_ktr_f, text="KTR X2", variable=self.var_rt_ktr_x2, command=self._refresh_rt_ktr).pack(side=tk.LEFT, padx=(12, 0))
        r += 1
        ttk.Label(rt_f, text="차익실현 (KTR 기준)").grid(row=r, column=0, sticky=tk.W, pady=2)
        self.var_rt_tp_ktr = tk.StringVar(value="1")
        tp_ktr_f = ttk.Frame(rt_f)
        tp_ktr_f.grid(row=r, column=1, sticky=tk.W, pady=2)
        for val in ("0.5", "1", "1.5", "2", "2.5", "3", "없음"):
            ttk.Radiobutton(tp_ktr_f, text=val, variable=self.var_rt_tp_ktr, value=val).pack(side=tk.LEFT, padx=(0, 6))
        r += 1
        ttk.Label(rt_f, text="진입 예약 시간").grid(row=r, column=0, sticky=tk.W, pady=2)
        sch_f = ttk.Frame(rt_f)
        sch_f.grid(row=r, column=1, sticky=tk.W, pady=2)
        self.var_rt_scheduled_time = tk.StringVar(value="")
        ttk.Entry(sch_f, textvariable=self.var_rt_scheduled_time, width=22).pack(side=tk.LEFT)
        ttk.Label(sch_f, text=" 비우면 즉시 | 예: 2025-02-25 14:30", font=("", 8)).pack(side=tk.LEFT)
        r += 1
        # ----- 현재 포지션 (선택 후 KTR 기준 T/P 업데이트) -----
        ttk.Separator(rt_f, orient=tk.HORIZONTAL).grid(row=r, column=0, columnspan=2, sticky=tk.EW, pady=8)
        r += 1
        rt_pos_f = ttk.Frame(rt_f)
        rt_pos_f.grid(row=r, column=0, columnspan=2, sticky=tk.NSEW, pady=(0, 2))
        cols_rt = ("ticket", "symbol", "type", "volume", "진입가", "현재TP")
        self.tree_rt_positions = ttk.Treeview(rt_pos_f, columns=cols_rt, show="headings", height=3, selectmode="browse")
        for col, (heading, w) in zip(cols_rt, [("티켓", 52), ("심볼", 72), ("매수/매도", 64), ("랏", 44), ("진입가", 72), ("현재TP", 72)]):
            self.tree_rt_positions.heading(col, text=heading)
            self.tree_rt_positions.column(col, width=w)
        scroll_rt_pos = ttk.Scrollbar(rt_pos_f, orient=tk.VERTICAL, command=self.tree_rt_positions.yview)
        self.tree_rt_positions.configure(yscrollcommand=scroll_rt_pos.set)
        self.tree_rt_positions.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_rt_pos.pack(side=tk.RIGHT, fill=tk.Y)
        self.rt_positions: List[Any] = []
        r += 1
        btn_rt = ttk.Frame(rt_f)
        btn_rt.grid(row=r, column=0, columnspan=2, sticky=tk.W, pady=(8, 4))
        ttk.Button(btn_rt, text="진입 실행", command=self._on_execute_realtime).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btn_rt, text="목록 새로고침", command=self._refresh_rt_positions).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btn_rt, text="T/P 업데이트", command=self._on_update_selected_position_tp).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btn_rt, text="포지션 SL/TP 수정…", command=self._open_sltp_editor).pack(side=tk.LEFT)
        self.var_rt_trailing_stop = tk.BooleanVar(value=False)
        ttk.Checkbutton(btn_rt, text="트레일링 스탑 (이익 50% 보전)", variable=self.var_rt_trailing_stop).pack(side=tk.LEFT, padx=(12, 0))
        r += 1
        self.var_rt_tp = tk.StringVar(value="사용하지 않음")
        self.var_rt_sl = tk.StringVar(value="사용하지 않음")
        rt_f.grid_columnconfigure(1, weight=1)

        # 공통 로그 (노트북 아래)
        f.grid_rowconfigure(0, weight=1)
        f.grid_columnconfigure(0, weight=1)
        self.log = scrolledtext.ScrolledText(f, height=6, width=72, state=tk.DISABLED)
        self.log.grid(row=1, column=0, columnspan=3, sticky=tk.NSEW, pady=2)
        f.grid_rowconfigure(1, weight=1)
        self._on_rt_n_changed()
        self._refresh_rt_ktr()
        self._refresh_rt_positions()
        self._refresh_res_ktr()

    def _refresh_rt_positions(self):
        """실시간 탭: KTR 포지션 목록 갱신."""
        for i in self.tree_rt_positions.get_children():
            self.tree_rt_positions.delete(i)
        self.rt_positions.clear()
        if not tr.init_mt5():
            self.tree_rt_positions.insert("", tk.END, values=("—", "—", "—", "—", "—", "MT5 연결 실패"))
            return
        positions = mt5.positions_get()
        positions = list(positions or [])
        for p in positions:
            side = "매수" if p.type == mt5.ORDER_TYPE_BUY else "매도"
            entry = getattr(p, "price_open", 0) or 0
            tp = getattr(p, "tp", 0) or 0
            self.tree_rt_positions.insert(
                "", tk.END,
                values=(p.ticket, p.symbol, side, p.volume, f"{entry:.5g}", f"{tp:.5g}" if tp else "—")
            )
            self.rt_positions.append(p)
        if not self.rt_positions:
            self.tree_rt_positions.insert("", tk.END, values=("—", "—", "—", "—", "—", "오픈 포지션 없음"))

    def _on_update_selected_position_tp(self):
        """선택한 포지션에 대해 현재가 ± (KTR×배수)로 차익실현 가격 업데이트. 손절은 유지."""
        sel = self.tree_rt_positions.selection()
        if not sel or not self.rt_positions:
            messagebox.showwarning("선택 필요", "포지션을 선택하세요.")
            return
        idx = self.tree_rt_positions.index(sel[0])
        if idx >= len(self.rt_positions):
            return
        pos = self.rt_positions[idx]
        tp_ktr_str = self.var_rt_tp_ktr.get().strip()
        if tp_ktr_str == "없음":
            messagebox.showinfo("KTR 배수 선택", "차익실현(KTR 기준)에서 0.5~3 중 하나를 선택하세요.")
            return
        try:
            tp_ktr_mult = float(tp_ktr_str)
        except ValueError:
            messagebox.showerror("오류", "KTR 배수 값을 숫자로 선택하세요.")
            return
        sym = pos.symbol
        comment = getattr(pos, "comment", "") or ""
        parsed = _parse_comment(comment)
        if parsed:
            _, _sl, session, timeframe = parsed
        else:
            session = self.var_rt_session.get().strip()
            timeframe = self.var_rt_tf.get().strip()
        ktr_value, _, _, _ = get_ktr_from_db_with_fallback(sym, session, timeframe)
        if not ktr_value or ktr_value <= 0:
            self._log(f"⚠️ {sym} KTR 조회 실패 (세션:{session} TF:{timeframe}) → T/P 업데이트 스킵")
            messagebox.showwarning("KTR 없음", f"{sym} 해당 세션/타임프레임 KTR이 없습니다. DB를 확인하세요.")
            return
        tick = mt5.symbol_info_tick(sym)
        if tick is None:
            self._log(f"⚠️ {sym} 호가 조회 실패")
            messagebox.showerror("오류", f"{sym} 현재가 조회 실패")
            return
        is_buy = pos.type == mt5.ORDER_TYPE_BUY
        current_price = tick.bid if is_buy else tick.ask
        new_tp = current_price + (ktr_value * tp_ktr_mult) if is_buy else current_price - (ktr_value * tp_ktr_mult)
        current_sl = getattr(pos, "sl", 0) or 0.0
        ok, msg = tr.modify_position_sltp(pos.ticket, sym, current_sl, new_tp)
        if ok:
            _add_realtime_tp_ticket(pos.ticket)
            self._log(f"  ✓ #{pos.ticket} {sym} T/P 업데이트: 현재가 {current_price:.5g} ± (KTR {ktr_value}×{tp_ktr_mult}) = {new_tp:.5g}")
            messagebox.showinfo("적용 완료", f"#{pos.ticket} 차익실현가 {new_tp:.5g} 로 설정되었습니다.")
            self._refresh_rt_positions()
        else:
            self._log(f"  ⚠️ T/P 수정 실패: {msg}")
            messagebox.showerror("수정 실패", msg)

    def _log(self, msg: str):
        self.log.configure(state=tk.NORMAL)
        self.log.insert(
            tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"
        )
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def _refresh_rt_ktr(self):
        """실시간 탭: 선택 심볼·세션·타임프레임으로 KTR 표시. 자동 선택 시 DB에 가장 최근 기록된 데이터 표시."""
        try:
            sym = self.var_rt_symbol.get()
            sess = self.var_rt_session.get()
            tf = self.var_rt_tf.get()
            if (sess or "").strip() == "자동":
                v, session_used = get_ktr_from_db_auto(sym, tf)
                if v and v > 0 and session_used:
                    self.lbl_rt_ktr.config(text=f"{v:.2f} pt ({session_used} {tf})")
                elif v and v > 0:
                    self.lbl_rt_ktr.config(text=f"{v:.2f} pt")
                else:
                    self.lbl_rt_ktr.config(text="(없음)")
            else:
                v = get_ktr_from_db(sym, sess, tf)
                if v and v > 0:
                    self.lbl_rt_ktr.config(text=f"{v:.2f} pt")
                else:
                    self.lbl_rt_ktr.config(text="(없음)")
        except Exception:
            self.lbl_rt_ktr.config(text="(오류)")

    def _refresh_res_ktr(self):
        """예약 오더 탭: 선택 심볼·세션·KTR 타임프레임으로 KTR 값 표시."""
        if not hasattr(self, "lbl_res_ktr"):
            return
        try:
            sym = self.var_symbol.get()
            sess = self.var_session.get()
            tf = self.var_ktr_tf.get()
            if (sess or "").strip() == "자동":
                v, session_used = get_ktr_from_db_auto(sym, tf)
                if v and v > 0 and session_used:
                    self.lbl_res_ktr.config(text=f"{v:.2f} pt ({session_used} {tf})")
                elif v and v > 0:
                    self.lbl_res_ktr.config(text=f"{v:.2f} pt")
                else:
                    self.lbl_res_ktr.config(text="(없음)")
            else:
                v = get_ktr_from_db(sym, sess, tf)
                if v and v > 0:
                    self.lbl_res_ktr.config(text=f"{v:.2f} pt")
                else:
                    self.lbl_res_ktr.config(text="(없음)")
        except Exception:
            self.lbl_res_ktr.config(text="(오류)")

    def _on_rt_n_changed(self):
        """실시간 탭: N값 변경 시 KTR 등만 갱신. 손절율은 항상 선택 가능 (N 사용 시에는 실행 시 N기준 손절 적용)."""
        self._refresh_rt_ktr()

    def _on_execute_realtime(self):
        """실시간 오더: 즉시 또는 지정 시간에 1차 시장가 + 2~N차 예약 주문. 진입 예약 시간이 미래면 그 시각에 실행."""
        symbol = self.var_rt_symbol.get().strip()
        tf = self.var_rt_tf.get().strip()
        entry_tf_label = self.var_rt_entry_tf.get().strip()
        side = self.var_rt_side.get().strip().upper()
        weight_s = self.var_rt_weight.get().strip().replace("%", "").strip()
        if weight_s == "전저점":
            risk = "전저점"
            num_positions_override = 5
        elif weight_s == "최대":
            risk = 0.0
            num_positions_override = None
        else:
            try:
                risk = float(weight_s)
            except ValueError:
                risk = 10.0
            num_positions_override = None
        n_str = self.var_rt_n.get().strip()
        # 비중 5%, 10%는 N값 없음으로 진입 불가
        if weight_s in ("5", "10") and n_str == "없음":
            messagebox.showwarning(
                "비중 / N값",
                "비중 5%, 10%는 N값 없음으로 오더를 생성할 수 없습니다.\nN값(1.5, 2.5, 3.5, 4.5 중 하나)을 선택하세요.",
            )
            return
        if n_str == "없음":
            num_positions = 1
            n_for_ktrlots = 1.0
        else:
            try:
                n_val = float(n_str)
            except ValueError:
                n_val = 2.5
            if num_positions_override is not None:
                num_positions = num_positions_override
            else:
                # 항상 1 KTR 단위로만 오더 생성 (10분봉 0.5 KTR 방식 제거)
                num_positions = int(n_val + 0.5)
            n_for_ktrlots = 4.5 if weight_s == "전저점" else n_val
        sl_from_n = True
        session = self.var_rt_session.get().strip()
        ENTRY_TF_TO_COMMENT = {"10분봉": "10M", "1시간봉": "1H"}
        comment_tf = ENTRY_TF_TO_COMMENT.get(entry_tf_label) or "1H"
        tp_ktr_str = self.var_rt_tp_ktr.get().strip()
        if tp_ktr_str == "없음":
            tp_ktr_multiplier = None
            tp_option = "사용하지 않음"
        else:
            try:
                tp_ktr_multiplier = float(tp_ktr_str)
                tp_option = f"KTR×{tp_ktr_str}"
            except ValueError:
                tp_ktr_multiplier = None
                tp_option = "사용하지 않음"
        sl_option = self.var_rt_sl.get().strip()

        cond_rt = self.var_rt_entry_condition.get().strip()
        conditions_rt = [cond_rt] if cond_rt in ENTRY_CONDITIONS else [ENTRY_CONDITIONS[0]]
        # 기본더블비는 N값 2.5 이상 또는 '없음'만 허용 (N=1.5 오더 생성 차단)
        if cond_rt == "기본더블비" and n_str == "1.5":
            messagebox.showwarning(
                "진입 조건 / N값",
                "기본더블비는 N값 2.5 이상 또는 '없음'만 선택할 수 있습니다.\nN값 1.5로는 진입 실행이 되지 않습니다.",
            )
            return
        # 돌파더블비 선택 시: 직전 봉에서 4B 상단 돌파 마감이 있었는지 확인
        if cond_rt == "돌파더블비":
            mt5_tf_rt = int(TF_MAP.get(entry_tf_label, mt5.TIMEFRAME_H1))
            matched_breakout, detail_breakout, _ = _check_entry_condition_one_with_detail(symbol, mt5_tf_rt, "돌파더블비")
            if not matched_breakout:
                messagebox.showwarning(
                    "돌파더블비 조건 미충족",
                    "돌파더블비는 직전 봉에서 4B 상단 돌파 마감이 확인된 경우에만 실행할 수 있습니다.\n\n"
                    + (f"상세: {detail_breakout}" if detail_breakout else "현재 직전 봉이 조건을 만족하지 않습니다."),
                )
                return
        add_ktr = getattr(self, "var_rt_add_ktr", None) and self.var_rt_add_ktr.get()
        if n_str == "없음":
            add_ktr = False  # N값 없음: 1포지션만 진입, +KTR 예약 추가 안 함
        add_ktr_mult_str = (getattr(self, "var_rt_add_ktr_mult", None) or tk.StringVar(value="1")).get().strip()
        if add_ktr_mult_str not in ("1", "2"):
            add_ktr_mult_str = "1"
        add_m10_bb_auto = bool(getattr(self, "var_rt_m10_bb_auto", None) and self.var_rt_m10_bb_auto.get())
        m10_bb_weight_s = (getattr(self, "var_rt_m10_bb_weight", None) or tk.StringVar(value="1%")).get().strip().replace("%", "").strip()
        try:
            m10_bb_weight_pct = float(m10_bb_weight_s) if m10_bb_weight_s else 1.0
        except ValueError:
            m10_bb_weight_pct = 1.0
        # M10 BB 자동 추가 비중 5%, 10%는 N값 없음으로 예약 생성 불가
        if add_m10_bb_auto and m10_bb_weight_s in ("5", "10") and n_str == "없음":
            messagebox.showwarning(
                "비중 / N값",
                "비중 5%, 10%는 N값 없음으로 오더를 생성할 수 없습니다.\nM10 BB 자동 추가를 쓰려면 N값을 선택하거나, 비중을 1%/2.5%로 설정하세요.",
            )
            add_m10_bb_auto = False
        # 시스템 주문(10분봉 4B/20B 자동오더)은 매시 정시~10분에만 입력 가능
        if add_m10_bb_auto and not _is_system_order_time_window_kst():
            messagebox.showwarning(
                "시스템 주문",
                "시스템 주문(10분봉 4B/20B 자동오더)은 매시 정시~10분에만 입력 가능합니다.\n(현재 11~59분 구간은 입력 불가)",
            )
            add_m10_bb_auto = False

        params = {
            "symbol": symbol,
            "tf": tf,
            "entry_tf_label": entry_tf_label,
            "side": side,
            "risk": risk,
            "n_for_ktrlots": n_for_ktrlots,
            "num_positions": num_positions,
            "sl_from_n": sl_from_n,
            "session": session,
            "comment_tf": comment_tf,
            "tp_ktr_multiplier": tp_ktr_multiplier,
            "tp_option": tp_option,
            "sl_option": sl_option,
            "conditions": conditions_rt,
            "add_ktr": bool(add_ktr),
            "add_ktr_mult": add_ktr_mult_str,
            "add_m10_bb_auto": add_m10_bb_auto,
            "m10_bb_weight_pct": m10_bb_weight_pct,
            "use_other_positions_sltp": (n_str == "없음"),
            "trailing_stop": bool(getattr(self, "var_rt_trailing_stop", None) and self.var_rt_trailing_stop.get()),
            "ktr_x2": bool(getattr(self, "var_rt_ktr_x2", None) and self.var_rt_ktr_x2.get()),
        }

        scheduled_str = self.var_rt_scheduled_time.get().strip()
        scheduled_dt = _parse_scheduled_time_kst(scheduled_str)
        now_kst = datetime.now(KST)

        if scheduled_str and scheduled_dt is None:
            messagebox.showwarning("진입 예약 시간", "예약 시간 형식이 올바르지 않습니다.\n예: 2025-02-25 14:30 또는 14:30")
            return
        if scheduled_dt is not None and scheduled_dt <= now_kst:
            messagebox.showinfo("진입 예약 시간", "입력한 시간이 과거입니다. 즉시 실행합니다.")
            scheduled_dt = None

        if scheduled_dt is not None:
            delay_sec = (scheduled_dt - now_kst).total_seconds()
            if delay_sec <= 0:
                self._log(f"⚠️ 예약 시간({scheduled_dt.strftime('%Y-%m-%d %H:%M')})이 이미 지났거나 같습니다. 즉시 실행하지 않습니다. 예약 시간을 비우면 즉시 실행됩니다.")
                messagebox.showwarning(
                    "진입 예약 시간",
                    "입력한 시간이 이미 지났거나 같습니다.\n즉시 실행하려면 '진입 예약 시간'을 비우고 다시 실행하세요.",
                )
                return
            self._log(f"⏰ 진입 예약: {scheduled_dt.strftime('%Y-%m-%d %H:%M')} (KST) 에 실행됩니다. 대기 시간: {int(delay_sec)}초. 프로그램을 종료하면 실행되지 않습니다.")
            root = self.root
            target_dt = scheduled_dt

            def run_at_scheduled():
                # 대기: 예약 시각이 될 때까지 (실행 직전에 다시 확인하여 조기 실행 방지)
                while True:
                    now = datetime.now(KST)
                    if now >= target_dt:
                        break
                    remain = (target_dt - now).total_seconds()
                    time.sleep(min(1.0, max(0.1, remain)))
                try:
                    if root.winfo_exists():
                        root.after(0, lambda: self._do_execute_realtime_with_params(params))
                except Exception:
                    pass
            threading.Thread(target=run_at_scheduled, daemon=True).start()
            return

        self._do_execute_realtime_with_params(params)

    def _do_execute_realtime_with_params(self, params: dict):
        """실시간 진입을 현재 폼이 아닌 전달된 params로 실행 (즉시 또는 예약 도달 시)."""
        if not self.root.winfo_exists():
            return
        symbol = params["symbol"]
        side = params["side"]
        risk = params["risk"]
        n_for_ktrlots = params["n_for_ktrlots"]
        num_positions = params["num_positions"]
        sl_from_n = params["sl_from_n"]
        session = params["session"]
        tf = params["tf"]
        entry_tf_label = params.get("entry_tf_label", "")
        comment_tf = params["comment_tf"]
        tp_option = params["tp_option"]
        sl_option = params["sl_option"]
        tp_ktr_multiplier = params["tp_ktr_multiplier"]

        conditions_rt = params.get("conditions", ["실시간"])
        cond_str = ", ".join(conditions_rt)
        telegram_lines = [
            "📋 KTR 실시간 오더 실행 로그",
            f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"심볼: {symbol} | {side} | 진입조건: {cond_str}",
            "",
        ]

        def log_fn(msg):
            self._log(msg)
            telegram_lines.append(msg)

        if not tr.init_mt5():
            self._log("❌ 실시간 오더 미실행: MT5 연결 실패.")
            messagebox.showerror("실행 불가", "MT5 연결에 실패했습니다. 터미널 실행 및 로그인을 확인하세요.")
            return
        acc = tr.get_account_info()
        if acc is None:
            self._log("❌ 실시간 오더 미실행: 계정 정보 조회 실패.")
            messagebox.showerror("실행 불가", "계정 정보 조회에 실패했습니다.")
            return
        margin_level = acc.get("margin_level") or 0
        if margin_level > 0 and margin_level <= MARGIN_LEVEL_MIN_PCT:
            self._log(f"❌ 실시간 오더 미실행: 마진 레벨 {margin_level:.1f}% (실행 조건: {MARGIN_LEVEL_MIN_PCT}% 초과)")
            messagebox.showwarning(
                "주문 미실행 (마진 부족)",
                f"현재 마진 레벨: {margin_level:.1f}%\n"
                f"실행 조건: {MARGIN_LEVEL_MIN_PCT}% 초과 시에만 주문 가능합니다.\n마진 확보 후 다시 시도하세요.",
            )
            return
        try:
            ok, entry, _ = _execute_ktr_entry(
                symbol=symbol,
                side=side,
                weight_pct=risk,
                n_value=n_for_ktrlots,
                num_positions=num_positions,
                sl_from_n=sl_from_n,
                session=session,
                tf=tf,
                tp_option=tp_option,
                sl_option=sl_option,
                log_fn=log_fn,
                comment_tf=comment_tf,
                order_id=f"realtime_{symbol}_{int(datetime.now().timestamp())}",
                entry_conditions=conditions_rt,
                source="realtime",
                tp_ktr_multiplier=tp_ktr_multiplier,
                tf_label=entry_tf_label,
                use_other_positions_sltp=bool(params.get("use_other_positions_sltp", False)),
                ktr_multiplier=2.0 if params.get("ktr_x2") else 1.0,
            )
        except Exception as e:
            import traceback
            self._log(f"❌ 진입 실행 중 오류: {e}")
            self._log(traceback.format_exc())
            telegram_lines.append(f"❌ 예외: {e}")
            ok = False
            entry = None
        if ok:
            self._log("실시간 진입 절차 완료.")
            telegram_lines.insert(3, "✅ 실행 결과: 성공")
            if params.get("trailing_stop"):
                sym_clean = (symbol or "").rstrip("+")
                positions = mt5.positions_get(symbol=symbol) or []
                if not positions and sym_clean:
                    positions = mt5.positions_get(symbol=sym_clean) or []
                ktr_pos = [p for p in positions if getattr(p, "magic", 0) == MAGIC_KTR]
                if ktr_pos:
                    latest = max(ktr_pos, key=lambda p: getattr(p, "time", 0))
                    _add_trailing_stop_ticket(latest.ticket)
                    self._log(f"  [트레일링 스탑] 1차 포지션 #{latest.ticket} 등록 (캔들가 기준 이익 50%% S/L 갱신)")
            # +KTR 체크 시 예약 오더 없이 즉시 시장가+KTR 1건 실행 (시장가 체결 후 N×KTR S/L·T/P 설정). 실패 시 로그만 남김.
            # 진입수>=2(1차 시장가+2차 예약)인 경우 +KTR 추가 시장가는 생략 → 동일가(24829) 시장가 중복 방지
            add_ktr = params.get("add_ktr", False)
            add_ktr_mult_str = params.get("add_ktr_mult", "1")
            num_positions = params.get("num_positions", 1)
            if not add_ktr:
                self._log("  +KTR 미실행: +KTR 체크 해제됨 또는 N값 없음(1포지션만 진입 시 +KTR 비활성)")
            elif num_positions >= 2:
                self._log("  +KTR 미실행: 진입수 2이상(1차 시장가+예약) 시 동일가 중복 시장가 방지를 위해 +KTR 추가 진입 생략")
            elif entry is None:
                self._log("  +KTR 미실행: 1차 진입가 조회 불가(진입 실패 시 추가 불가)")
            elif add_ktr_mult_str not in ("1", "2"):
                self._log(f"  +KTR 미실행: 배수 값이 1/2가 아님 (현재: {add_ktr_mult_str})")
            else:
                try:
                    ktr_value, _, _, _ = get_ktr_from_db_with_fallback(symbol, session, tf)
                    if not ktr_value or ktr_value <= 0:
                        self._log(f"  +KTR 미실행: 해당 심볼/세션/타임프레임 KTR 값 없음 ({symbol} / {session} / {tf}). KTR 탭에서 입력 후 재시도.")
                    else:
                        ktr_mult = 2.0 if params.get("ktr_x2") else 1.0
                        ktr_f = float(ktr_value) * ktr_mult
                        entry_tf = (params.get("entry_tf_label") or "").strip()
                        tp_ktr = params.get("tp_ktr_multiplier")
                        is_buy = (side or "").strip().upper() == "BUY"
                        tp_ktr_f = float(tp_ktr) if tp_ktr is not None else None
                        # 등록할 배수 목록: 항상 1 KTR 단위만 (1 또는 2 중 선택한 값 하나)
                        cond_label = f"실시간+KTR{add_ktr_mult_str}"
                        mults_to_register = [(add_ktr_mult_str, cond_label)]
                        for mult_str, cond_label in mults_to_register:
                            mult_f = float(mult_str)
                            trigger_price = (entry + ktr_f * mult_f) if not is_buy else (entry - ktr_f * mult_f)
                            item = {
                                "symbol": symbol,
                                "side": side,
                                "entry": entry,
                                "trigger_price": trigger_price,
                                "ktr_value": ktr_f,
                                "n_value": n_for_ktrlots,
                                "weight_pct": risk,
                                "session": session,
                                "tf": tf,
                                "tp_option": tp_option or "사용하지 않음",
                                "sl_option": sl_option or "사용하지 않음",
                                "entry_tf_label": entry_tf,
                                "add_ktr_mult_str": mult_str,
                                "tp_ktr_multiplier": tp_ktr_f,
                                "cond_label": cond_label,
                                "exceeded": False,
                            }
                            with self._retrace_lock:
                                self._realtime_ktr_retrace_list.append(item)
                            self._log(f"  +KTR 되돌림 대기 등록: {cond_label} 트리거={trigger_price:.5g} (가격이 트리거를 초과했다가 다시 돌아오면 시장가 실행)")
                except Exception as e_add:
                    self._log(f"  ⚠️ +KTR 되돌림 등록 실패: {e_add}")
                    import traceback
                    self._log(traceback.format_exc())
        else:
            telegram_lines.insert(3, "❌ 실행 결과: 실패 (사유는 아래 로그 참고)")
        if params.get("add_m10_bb_auto"):
            self._ensure_m10_bb_auto_orders(
                params["symbol"],
                params.get("m10_bb_weight_pct", 1.0),
                params.get("session", "자동"),
                params.get("tp_option", "사용하지 않음"),
                params.get("sl_option", "사용하지 않음"),
                log_fn,
            )
        try:
            send_telegram_msg("\n".join(telegram_lines), parse_mode="")
        except Exception:
            pass

    def _open_sltp_editor(self):
        """포지션 SL/TP 수정 창 열기."""
        win = tk.Toplevel(self.root)
        win.transient(self.root)
        PositionSltpEditorWindow(win, log_fn=self._log)

    def _ensure_m10_bb_auto_orders(
        self,
        symbol: str,
        weight_pct: float,
        session: str,
        tp_option: str,
        sl_option: str,
        log_fn,
    ) -> None:
        """10분봉 4B/20B 자동오더: 오프셋 적용 20B 하단·4B 하단에 BUY_STOP 예약 주문 생성 또는 가격 갱신. 정시~10분에만 입력 가능."""
        if not _is_system_order_time_window_kst():
            log_fn("  [10M 4B/20B 자동오더] 매시 정시~10분에만 입력 가능합니다. (현재 11~59분 → 스킵)")
            return
        if not tr.init_mt5():
            log_fn("  [10M 4B/20B 자동오더] MT5 미연결 → 스킵")
            return
        # 1H 직전 봉이 20B 상단 터치 시 매수 진입 차단 (상단 터치 후 바로 매수 방지)
        touched_upper, _ = _1h_last_closed_bar_touched_20b_upper_or_lower(symbol)
        if touched_upper:
            log_fn("  [10M 4B/20B 자동오더] 1H 직전 봉 20B 상단 터치 → 매수 오더 생성 보류")
            return
        l20, l4 = _get_m10_bb_lower_levels_with_offset(symbol)
        if l20 is None and l4 is None:
            log_fn("  [10M 4B/20B 자동오더] 10분봉 20B/4B 하단 계산 불가 → 스킵")
            return
        if l20 is None:
            l20 = l4
        if l4 is None:
            l4 = l20
        acc = tr.get_account_info()
        balance = (acc.get("balance") or 0) if acc else 0
        if balance <= 0:
            log_fn("  [10M 4B/20B 자동오더] 잔고 조회 실패 → 스킵")
            return
        ktr_value, resolved_session, _, _ = get_ktr_from_db_with_fallback(symbol, session, "10M")
        if not ktr_value or ktr_value <= 0:
            ktr_value, _, _, _ = get_ktr_from_db_with_fallback(symbol, session, "1H")
        if not ktr_value or ktr_value <= 0:
            ktr_value = 1.0
        lots_map = get_ktrlots_lots(
            balance, weight_pct, 2.5, ktr_value, symbol_for_db(symbol), headless=True
        )
        lot = (lots_map or {}).get("1st") or 0
        if not lot or lot <= 0:
            log_fn("  [10M 4B/20B 자동오더] 랏수 계산 실패 → 스킵")
            return
        path = getattr(self, "_reservations_path", None) or RESERVATIONS_PATH
        script_dir = os.path.dirname(os.path.abspath(path))
        m10_path = M10_BB_AUTO_ORDERS_PATH
        try:
            with open(m10_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = []
        if not isinstance(data, list):
            data = []
        entry = next((e for e in data if isinstance(e, dict) and e.get("symbol") == symbol), None)
        comment_20b = _build_order_comment("10M20B", resolved_session or session, "10M", tp_option, sl_option, True)
        comment_4b = _build_order_comment("10M4B", resolved_session or session, "10M", tp_option, sl_option, True)
        ticket_20b = entry.get("ticket_20b") if entry else None
        ticket_4b = entry.get("ticket_4b") if entry else None
        if ticket_20b:
            orders = mt5.orders_get(ticket=int(ticket_20b))
            if not orders or len(orders) == 0:
                ticket_20b = None
        if ticket_4b:
            orders = mt5.orders_get(ticket=int(ticket_4b))
            if not orders or len(orders) == 0:
                ticket_4b = None
        if ticket_20b:
            ok, msg = tr.modify_pending_order_price(int(ticket_20b), l20)
            if ok:
                log_fn(f"  [10M 4B/20B 자동오더] 20B 하단 예약 가격 갱신: {l20:.5g}")
            else:
                log_fn(f"  [10M 4B/20B 자동오더] 20B 하단 가격 수정 실패: {msg}")
        else:
            ok, msg = tr.place_pending_stop(
                symbol, "BUY", lot, l20, magic=MAGIC_KTR, comment=comment_20b
            )
            if ok:
                orders = mt5.orders_get(symbol=symbol)
                for o in (orders or []):
                    if getattr(o, "magic", 0) == MAGIC_KTR and getattr(o, "type", 0) == mt5.ORDER_TYPE_BUY_STOP:
                        if abs(getattr(o, "price_open", 0) - l20) < 1e-6 * (1 + abs(l20)):
                            ticket_20b = o.ticket
                            break
                log_fn(f"  [10M 4B/20B 자동오더] 20B 하단 예약 주문: {lot}랏 @ {l20:.5g}")
            else:
                log_fn(f"  [10M 4B/20B 자동오더] 20B 하단 주문 실패: {msg}")
        if ticket_4b:
            ok, msg = tr.modify_pending_order_price(int(ticket_4b), l4)
            if ok:
                log_fn(f"  [10M 4B/20B 자동오더] 4B 하단 예약 가격 갱신: {l4:.5g}")
            else:
                log_fn(f"  [10M 4B/20B 자동오더] 4B 하단 가격 수정 실패: {msg}")
        else:
            ok, msg = tr.place_pending_stop(
                symbol, "BUY", lot, l4, magic=MAGIC_KTR, comment=comment_4b
            )
            if ok:
                orders = mt5.orders_get(symbol=symbol)
                for o in (orders or []):
                    if getattr(o, "magic", 0) == MAGIC_KTR and getattr(o, "type", 0) == mt5.ORDER_TYPE_BUY_STOP:
                        if abs(getattr(o, "price_open", 0) - l4) < 1e-6 * (1 + abs(l4)) and o.ticket != ticket_20b:
                            ticket_4b = o.ticket
                            break
                log_fn(f"  [10M 4B/20B 자동오더] 4B 하단 예약 주문: {lot}랏 @ {l4:.5g}")
            else:
                log_fn(f"  [10M 4B/20B 자동오더] 4B 하단 주문 실패: {msg}")
        if entry is None:
            entry = {"symbol": symbol, "weight_pct": weight_pct, "ticket_20b": None, "ticket_4b": None}
            data.append(entry)
        entry["ticket_20b"] = ticket_20b
        entry["ticket_4b"] = ticket_4b
        entry["weight_pct"] = weight_pct
        try:
            with open(m10_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_fn(f"  [10M 4B/20B 자동오더] 설정 저장 실패: {e}")

    def _get_conditions_from_form(self) -> List[str]:
        """진입 조건 1개만 선택(라디오)."""
        c = self.var_entry_condition.get().strip()
        return [c] if c in ENTRY_CONDITIONS else []

    def _on_add_reservation(self):
        conditions = self._get_conditions_from_form()
        if not conditions:
            messagebox.showwarning("진입 조건", "진입 조건을 하나 선택하세요.")
            return
        symbol = self.var_symbol.get().strip()
        tf_label = self.var_tf_monitor.get().strip()
        side = self.var_side.get().strip().upper()
        weight_s = self.var_weight.get().strip().replace("%", "").strip()
        if weight_s == "전저점":
            risk = "전저점"
            num_positions_override = 5
        elif weight_s == "최대":
            risk = 0.0
            num_positions_override = None
        else:
            try:
                risk = float(weight_s)
            except ValueError:
                risk = 10.0
            num_positions_override = None
        n_str = self.var_n.get().strip()
        # 비중 5%, 10%는 N값 없음으로 오더 생성 불가
        if weight_s in ("5", "10") and n_str == "없음":
            messagebox.showwarning(
                "비중 / N값",
                "비중 5%, 10%는 N값 없음으로 오더를 생성할 수 없습니다.\nN값(1.5, 2.5, 3.5, 4.5 중 하나)을 선택하세요.",
            )
            return
        # 기본더블비는 N값 2.5 이상 또는 '없음'만 허용 (N=1.5 오더 생성 차단)
        if "기본더블비" in conditions and n_str == "1.5":
            messagebox.showwarning(
                "진입 조건 / N값",
                "기본더블비는 N값 2.5 이상 또는 '없음'만 선택할 수 있습니다.\nN값 1.5로는 오더가 생성되지 않습니다.",
            )
            return
        # 돌파더블비 선택 시: 직전 봉에서 4B 상단 돌파 마감이 있었는지 확인
        if "돌파더블비" in conditions:
            mt5_tf = int(TF_MAP.get(tf_label, mt5.TIMEFRAME_H1))
            matched, detail, _ = _check_entry_condition_one_with_detail(symbol, mt5_tf, "돌파더블비")
            if not matched:
                messagebox.showwarning(
                    "돌파더블비 조건 미충족",
                    "돌파더블비는 직전 봉에서 4B 상단 돌파 마감이 확인된 경우에만 등록할 수 있습니다.\n\n"
                    + (f"상세: {detail}" if detail else "현재 직전 봉이 조건을 만족하지 않습니다."),
                )
                return
        if n_str == "없음":
            num_positions = 1
            n_for_ktrlots = 1.0
            n_display_str = "없음"
        else:
            try:
                n_val = float(n_str)
            except ValueError:
                n_val = 2.5
            num_positions = int(n_val + 0.5) if num_positions_override is None else num_positions_override
            n_for_ktrlots = 4.5 if weight_s == "전저점" else n_val
            n_display_str = "4.5" if weight_s == "전저점" else n_str

        item = {
            "id": datetime.now().strftime("%Y%m%d%H%M%S") + "_" + symbol,
            "symbol": symbol,
            "side": side,
            "timeframe_label": tf_label,
            "mt5_timeframe": int(TF_MAP.get(tf_label, mt5.TIMEFRAME_H1)),
            "conditions": conditions,
            "entry_time": (self.var_entry_time.get() or "").strip(),
            "weight_pct": risk,
            "n_value": n_for_ktrlots,
            "n_display": n_display_str,
            "num_positions": num_positions,
            "sl_from_n": True,
            "session": self.var_session.get().strip(),
            "ktr_tf": self.var_ktr_tf.get().strip(),
            "ktr_x2": bool(getattr(self, "var_res_ktr_x2", None) and self.var_res_ktr_x2.get()),
            "tp_option": self.var_tp.get().strip(),
            "sl_option": self.var_sl.get().strip(),
            "active": True,
            "created_at": datetime.now().isoformat(),
        }
        if self._editing_index is not None:
            idx = self._editing_index
            updated = False
            with self._reservations_lock:
                if 0 <= idx < len(self.reservations):
                    old = self.reservations[idx]
                    item["id"] = old.get("id", item["id"])
                    item["created_at"] = old.get("created_at", item["created_at"])
                    item["active"] = old.get("active", True)
                    self.reservations[idx] = item
                    updated = True
            if updated:
                save_reservations(self.reservations, getattr(self, "_reservations_path", None))
                self._refresh_tree()
                self._update_reservation_count_title()
                self._clear_edit_mode_ui()
                self._log(f"예약 수정 반영: {symbol} {side} {tf_label} {conditions} (파일에 저장됨)")
                messagebox.showinfo("수정 반영", "예약 수정 내용이 파일에 저장되었습니다. 재시작 후에도 반영됩니다.")
            return
        with self._reservations_lock:
            self.reservations.append(item)
            save_reservations(self.reservations, getattr(self, "_reservations_path", None))
        self._refresh_tree()
        self._update_reservation_count_title()
        self._log(f"예약 추가: {symbol} {side} {tf_label} {conditions} (파일에 저장됨)")

    def _clear_edit_mode_ui(self):
        """수정 모드 해제: 버튼 문구 복구 및 수정 반영/수정 취소 버튼 숨김."""
        self._editing_index = None
        self.btn_add_reservation.configure(text="매매예약 추가")
        self.btn_save_edit.pack_forget()
        self.btn_cancel_edit.pack_forget()

    def _on_save_edited_reservation(self):
        """수정 모드일 때 폼 내용을 선택 예약에 반영·저장. (수정 반영 버튼용)."""
        if self._editing_index is None:
            messagebox.showinfo("저장", "수정할 예약을 선택한 뒤 '선택 예약 수정'을 누르세요.")
            return
        self._on_add_reservation()

    def _cancel_edit_mode(self):
        """수정 모드 취소 (저장하지 않음)."""
        self._clear_edit_mode_ui()
        self._log("예약 수정 취소 (저장하지 않음)")
        messagebox.showinfo("수정 취소", "수정 모드를 취소했습니다. 저장되지 않았습니다.")

    def _on_edit_reservation(self):
        """선택한 예약을 폼에 불러와 수정할 수 있게 함. 수정 후 '수정 반영 (저장)' 버튼으로 저장."""
        sel = self._get_selected_or_focused_reservation_item()
        if not sel:
            messagebox.showinfo("수정", "목록에서 수정할 예약을 선택하세요.")
            return
        idx = self.tree.index(sel[0])
        with self._reservations_lock:
            if idx < 0 or idx >= len(self.reservations):
                return
            r = self.reservations[idx].copy()
        self.var_symbol.set(r.get("symbol", "NAS100+"))
        self.var_side.set((r.get("side") or "BUY").upper())
        self.var_tf_monitor.set(r.get("timeframe_label", "1시간봉"))
        conditions = r.get("conditions", [])
        cond0 = conditions[0] if conditions else ENTRY_CONDITIONS[0]
        self.var_entry_condition.set(cond0 if cond0 in ENTRY_CONDITIONS else ENTRY_CONDITIONS[0])
        self.var_entry_time.set(r.get("entry_time", "") or "")
        w = r.get("weight_pct", 10)
        if w == "전저점":
            weight_str = "전저점"
        elif w == 0:
            weight_str = "최대"
        else:
            weight_str = f"{int(w)}%" if isinstance(w, (int, float)) and w == int(w) else f"{w}%"
        if weight_str not in ("1%", "2.5%", "5%", "10%", "전저점"):
            weight_str = "1%"
        self.var_weight.set(weight_str)
        n_disp = r.get("n_display", str(r.get("n_value", "2.5")))
        self.var_n.set(
            n_disp
            if n_disp in ("1.5", "2.5", "3.5", "4.5", "없음")
            else "2.5"
        )
        self.var_session.set("자동")
        ktr_tf = (r.get("ktr_tf") or "10M").strip().upper()
        self.var_ktr_tf.set(ktr_tf if ktr_tf in ("10M", "1H") else "10M")
        if getattr(self, "var_res_ktr_x2", None) is not None:
            self.var_res_ktr_x2.set(bool(r.get("ktr_x2", False)))
        self._refresh_res_ktr()
        # 차익실현/손절 설정 기능 제거 → 수정 시에도 사용하지 않음으로 유지
        self.var_tp.set("사용하지 않음")
        self.var_sl.set("사용하지 않음")
        self._editing_index = idx
        self.btn_add_reservation.configure(text="수정 반영")
        self.btn_save_edit.pack(side=tk.LEFT, padx=4)
        self.btn_cancel_edit.pack(side=tk.LEFT, padx=2)
        self._log(f"예약 수정 모드: {r.get('symbol')} (폼 수정 후 '수정 반영 (저장)' 클릭)")
        messagebox.showinfo(
            "예약 수정",
            "위 폼에서 내용을 수정한 뒤,\n아래 [수정 반영 (저장)] 버튼을 눌러 저장하세요.\n저장하지 않으려면 [수정 취소]를 누르세요.",
        )

    def _refresh_tree(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        with self._reservations_lock:
            snapshot = list(self.reservations)
        for r in snapshot:
            cond_str = ",".join(r.get("conditions", []))
            entry_time_str = (r.get("entry_time") or "").strip() or "—"
            self.tree.insert(
                "",
                tk.END,
                values=(
                    r.get("symbol", ""),
                    r.get("side", ""),
                    r.get("timeframe_label", ""),
                    cond_str,
                    entry_time_str,
                    "전저점" if r.get("weight_pct") == "전저점" else ("최대" if r.get("weight_pct", 0) == 0 else str(r.get("weight_pct", 0)) + "%"),
                    r.get("n_display", r.get("n_value", "")),
                    r.get("session", "자동"),
                    "Y" if r.get("active", True) else "N",
                    (r.get("created_at", "") or "")[:19].replace("T", " "),
                ),
            )

    def _update_reservation_count_title(self):
        """창 제목에 현재 예약 건수 반영."""
        with self._reservations_lock:
            n = len(self.reservations)
        self.root.title(f"KTR 예약·실시간 오더 (예약 {n}건)")

    def _load_and_refresh_list(self):
        """ktr_reservations.json에서 예약 목록 로드 후 트리 갱신. 재실행 시 저장된 예약이 자동 복원됨."""
        path = getattr(self, "_reservations_path", None) or RESERVATIONS_PATH
        with self._reservations_lock:
            self.reservations = load_reservations(path)
            # 다른 경로에 기존 파일이 있으면 한 번만 이전 경로에서 읽어 현재 경로에 저장 (이관)
            if not self.reservations and path != RESERVATIONS_PATH and os.path.isfile(RESERVATIONS_PATH):
                migrated = load_reservations(RESERVATIONS_PATH)
                if migrated:
                    save_reservations(migrated, path)
                    self.reservations = migrated
                    self._log(f"기존 예약 파일을 이 프로그램 폴더로 이관했습니다. ({len(migrated)}건)")
        self._refresh_tree()
        if self._editing_index is not None:
            self._clear_edit_mode_ui()
        self._update_reservation_count_title()
        with self._reservations_lock:
            n = len(self.reservations)
        if n > 0:
            self._log(f"저장된 예약 {n}건을 불러왔습니다. (파일: {os.path.basename(path)})")
        else:
            self._log(f"예약 목록 로드됨 (0건). 추가한 예약은 프로그램 재실행 후에도 유지됩니다. (저장: {os.path.basename(path)})")

    def _check_and_show_missing_ktr(self):
        """시작 시 오늘자 현재 세션 KTR만 자동 측정(MT5 등). 수동 입력 창은 띄우지 않음."""
        def _do_fill():
            try:
                from ktr_measure_calculator import run_fill_missing_ktr_for_today
                try:
                    from db_config import UNIFIED_DB_PATH
                    db_path = UNIFIED_DB_PATH
                except ImportError:
                    db_path = os.path.join(_SCRIPT_DIR, "scheduler.db")
                filled = run_fill_missing_ktr_for_today(ktr_db_path=db_path, quiet=True)
                if filled > 0:
                    self.root.after(0, lambda: self._log(f"KTR 누락 자동 입력 완료: 오늘자 현재 세션 {filled}개 슬롯.\n", "stdout"))
            except Exception as e:
                self.root.after(0, lambda: self._log(f"KTR 누락 자동 입력 실패: {e}\n", "stderr"))

        try:
            db = KTRDatabase()
            today_str = datetime.now(KST).strftime("%Y-%m-%d")
            missing = db.get_missing_ktr_slots([today_str])
            db.conn.close()
        except Exception as e:
            self._log(f"KTR 누락 조회 실패: {e}\n", "stderr")
            return
        if not missing:
            return
        threading.Thread(target=_do_fill, daemon=True).start()

    def _open_missing_ktr_dialog(self, missing: List[Tuple[str, str, str]]):
        """누락된 KTR 슬롯 목록을 보여주고, NAS100/XAUUSD 값을 입력해 DB에 저장할 수 있는 창을 연다."""
        win = tk.Toplevel(self.root)
        win.title("KTR 누락 레코드 입력")
        win.geometry("720x400")
        win.minsize(560, 300)
        f = ttk.Frame(win, padding=10)
        f.pack(fill=tk.BOTH, expand=True)
        ttk.Label(f, text="아래 슬롯은 KTR 테이블에 레코드가 없습니다. 값을 입력한 뒤 [행 저장] 또는 [일괄 저장]을 눌러 주세요.", font=("", 9)).grid(row=0, column=0, columnspan=6, sticky=tk.W, pady=(0, 8))
        # 헤더
        ttk.Label(f, text="날짜").grid(row=1, column=0, sticky=tk.W, padx=2, pady=2)
        ttk.Label(f, text="세션").grid(row=1, column=1, sticky=tk.W, padx=2, pady=2)
        ttk.Label(f, text="TF").grid(row=1, column=2, sticky=tk.W, padx=2, pady=2)
        ttk.Label(f, text="NAS100 KTR").grid(row=1, column=3, sticky=tk.W, padx=2, pady=2)
        ttk.Label(f, text="XAUUSD KTR").grid(row=1, column=4, sticky=tk.W, padx=2, pady=2)
        ttk.Label(f, text="").grid(row=1, column=5, sticky=tk.W, padx=2, pady=2)
        entries: Dict[Tuple[str, str, str], Tuple[tk.Entry, tk.Entry]] = {}
        for i, (session, tf, record_date) in enumerate(missing):
            row = 2 + i
            ttk.Label(f, text=record_date).grid(row=row, column=0, sticky=tk.W, padx=2, pady=2)
            ttk.Label(f, text=session).grid(row=row, column=1, sticky=tk.W, padx=2, pady=2)
            ttk.Label(f, text=tf).grid(row=row, column=2, sticky=tk.W, padx=2, pady=2)
            nas_var = tk.StringVar(value="")
            xau_var = tk.StringVar(value="")
            en_nas = ttk.Entry(f, textvariable=nas_var, width=10)
            en_xau = ttk.Entry(f, textvariable=xau_var, width=10)
            en_nas.grid(row=row, column=3, sticky=tk.W, padx=2, pady=2)
            en_xau.grid(row=row, column=4, sticky=tk.W, padx=2, pady=2)
            key = (session, tf, record_date)
            entries[key] = (en_nas, en_xau)

            def _save_row(s=session, t=tf, d=record_date, en_n=en_nas, en_x=en_xau):
                try:
                    v_nas = en_n.get().strip()
                    v_xau = en_x.get().strip()
                    if not v_nas and not v_xau:
                        messagebox.showwarning("입력 필요", "NAS100 또는 XAUUSD KTR 값을 입력하세요.", parent=win)
                        return
                    ktr_db = KTRDatabase()
                    if v_nas:
                        ktr_db.update_ktr("NAS100", s, t, float(v_nas.replace(",", ".")), record_date=d)
                    if v_xau:
                        ktr_db.update_ktr("XAUUSD", s, t, float(v_xau.replace(",", ".")), record_date=d)
                    ktr_db.conn.close()
                    self._log(f"KTR 저장: {d} {s} {t} NAS100={v_nas or '-'} XAUUSD={v_xau or '-'}")
                    messagebox.showinfo("저장 완료", "해당 슬롯이 저장되었습니다.", parent=win)
                except ValueError as ve:
                    messagebox.showerror("입력 오류", "KTR 값은 숫자로 입력하세요.", parent=win)
                except Exception as ex:
                    messagebox.showerror("저장 실패", str(ex), parent=win)

            ttk.Button(f, text="행 저장", command=_save_row).grid(row=row, column=5, sticky=tk.W, padx=2, pady=2)

        def _save_all():
            ktr_db = KTRDatabase()
            saved = 0
            for (session, tf, record_date), (en_nas, en_xau) in entries.items():
                v_nas = en_nas.get().strip()
                v_xau = en_xau.get().strip()
                if not v_nas and not v_xau:
                    continue
                try:
                    if v_nas:
                        ktr_db.update_ktr("NAS100", session, tf, float(v_nas.replace(",", ".")), record_date=record_date)
                    if v_xau:
                        ktr_db.update_ktr("XAUUSD", session, tf, float(v_xau.replace(",", ".")), record_date=record_date)
                    saved += 1
                except ValueError:
                    pass
            ktr_db.conn.close()
            if saved > 0:
                self._log(f"KTR 일괄 저장: {saved}개 슬롯")
                messagebox.showinfo("일괄 저장 완료", f"{saved}개 슬롯을 저장했습니다.", parent=win)
            else:
                messagebox.showwarning("저장할 값 없음", "입력된 KTR 값이 없습니다.", parent=win)

        btn_row = 2 + len(missing)
        ttk.Button(f, text="일괄 저장", command=_save_all).grid(row=btn_row, column=3, sticky=tk.W, padx=4, pady=12)
        ttk.Button(f, text="닫기", command=win.destroy).grid(row=btn_row, column=4, sticky=tk.W, padx=4, pady=12)
        f.grid_columnconfigure(3, weight=1)
        f.grid_columnconfigure(4, weight=1)

    def _on_launch_simulator(self):
        """진입 조건별 승률 시뮬레이터 GUI를 별도 프로세스로 실행."""
        gui_path = os.path.join(_SCRIPT_DIR, "Others", "entry_simulator_gui.py")
        if not os.path.isfile(gui_path):
            messagebox.showerror("시뮬레이터", f"파일을 찾을 수 없습니다: {gui_path}")
            return
        try:
            kwargs = {"cwd": _SCRIPT_DIR}
            if sys.platform == "win32":
                kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            subprocess.Popen([sys.executable, gui_path], **kwargs)
            self._log("시뮬레이터 창을 실행했습니다.")
        except Exception as e:
            messagebox.showerror("시뮬레이터", f"실행 실패: {e}")

    def _on_launch_ai_simulator(self):
        """AI 시뮬레이터(entry_simulator_gui_v3.py)를 별도 프로세스로 실행."""
        gui_path = os.path.join(_SCRIPT_DIR, "Others", "entry_simulator_gui_v3.py")
        if not os.path.isfile(gui_path):
            messagebox.showerror("AI 시뮬레이터", f"파일을 찾을 수 없습니다: {gui_path}")
            return
        try:
            kwargs = {"cwd": _SCRIPT_DIR}
            if sys.platform == "win32":
                kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            subprocess.Popen([sys.executable, gui_path], **kwargs)
            self._log("AI 시뮬레이터 창을 실행했습니다.")
        except Exception as e:
            messagebox.showerror("AI 시뮬레이터", f"실행 실패: {e}")

    def _on_save_bb_offset(self):
        """심볼별 BB 오프셋 %를 파일에 저장 (기존 파일이 있으면 덮어씀)."""
        if not hasattr(self, "var_bb_offset"):
            return
        try:
            data = {}
            for sym in BB_OFFSET_SYMBOLS:
                v = self.var_bb_offset[sym].get() if sym in self.var_bb_offset else "0"
                try:
                    data[sym] = float((v or "0").strip().replace(",", ""))
                except ValueError:
                    data[sym] = 0.0
            _save_bb_offset(data)
            messagebox.showinfo("저장됨", "볼린저 밴드 오프셋 %가 저장되었습니다.")
        except Exception as e:
            messagebox.showerror("저장 오류", str(e))

    def _on_show_offset_graph(self):
        """오프셋 그래프 창: 과거 3일 캔들 + BB(오프셋 적용) + 타임프레임 전환·확대."""
        try:
            import matplotlib
            matplotlib.use("TkAgg")
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
        except ImportError:
            messagebox.showerror("오프셋 그래프", "matplotlib가 필요합니다. pip install matplotlib")
            return
        win = tk.Toplevel(self.root)
        win.title("오프셋 그래프 (과거 3일 캔들·볼린저 밴드)")
        win.geometry("1000x650")
        win.minsize(800, 500)
        try:
            plt.rcParams["font.family"] = "Malgun Gothic"
            plt.rcParams["axes.unicode_minus"] = False
        except Exception:
            pass
        offset_map = _load_bb_offset()
        tf_to_key = {"5분봉": "M5", "10분봉": "M10", "1시간봉": "H1"}
        bars_3d = {"M5": 3 * 24 * 12, "M10": 3 * 24 * 6, "H1": 3 * 24}

        def get_bars(symbol: str, tf: str):
            if _pm_db is None:
                return []
            bars = _pm_db.get_bars_from_db(symbol, tf, limit=900)
            if not bars:
                return []
            n = min(len(bars), bars_3d.get(tf, 500))
            chronological = list(reversed(bars[:n]))
            return chronological

        current_symbol = tk.StringVar(value="XAUUSD+")
        current_tf = tk.StringVar(value="5분봉")
        fig = None
        canvas_widget = None
        top_f = ttk.Frame(win)
        top_f.pack(fill=tk.X, padx=6, pady=6)
        chart_frame = ttk.Frame(win)
        chart_frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)

        def draw():
            nonlocal fig, canvas_widget
            sym = current_symbol.get().strip()
            tf_label = current_tf.get().strip()
            tf = tf_to_key.get(tf_label, "M5")
            bars = get_bars(sym, tf)
            for c in list(chart_frame.winfo_children()):
                try:
                    c.destroy()
                except Exception:
                    pass
            if fig is not None:
                try:
                    plt.close(fig)
                except Exception:
                    pass
                fig = None
            fig, ax = plt.subplots(figsize=(10, 5.5), dpi=100)
            ax.set_facecolor("#f8f8f8")
            fig.patch.set_facecolor("#f0f0f0")
            if not bars:
                ax.set_title(f"{sym} {tf_label} — 데이터 없음", fontsize=11)
                canvas = FigureCanvasTkAgg(fig, master=chart_frame)
                canvas_widget = canvas.get_tk_widget()
                canvas_widget.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
                toolbar_frame = ttk.Frame(chart_frame)
                toolbar_frame.pack(fill=tk.X, padx=2, pady=2)
                toolbar_frame._is_toolbar_frame = True
                NavigationToolbar2Tk(canvas, toolbar_frame)
                canvas.draw()
                return
            n = len(bars)
            x = list(range(n))
            opens = [float(b["open"]) for b in bars]
            highs = [float(b["high"]) for b in bars]
            lows = [float(b["low"]) for b in bars]
            closes = [float(b["close"]) for b in bars]
            for i in range(n):
                color = "#d32f2f" if closes[i] < opens[i] else "#1976d2"
                ax.vlines(i, lows[i], highs[i], colors=color, linewidth=0.8)
                lo, hi = min(opens[i], closes[i]), max(opens[i], closes[i])
                ax.vlines(i, lo, hi, colors=color, linewidth=3.5)
            offset_pct = offset_map.get(sym, 0) or 0
            bb20_u, bb20_l, bb4_u, bb4_l = [], [], [], []
            bb20_u_off, bb20_l_off, bb4_u_off, bb4_l_off = [], [], [], []
            for b in bars:
                u20, l20 = b.get("bb20_upper"), b.get("bb20_lower")
                u4, l4 = b.get("bb4_upper"), b.get("bb4_lower")
                bb20_u.append(float(u20) if u20 is not None else float("nan"))
                bb20_l.append(float(l20) if l20 is not None else float("nan"))
                bb4_u.append(float(u4) if u4 is not None else float("nan"))
                bb4_l.append(float(l4) if l4 is not None else float("nan"))
                bb20_u_off.append(_apply_bb_offset(float(u20), offset_pct, True) if u20 is not None else float("nan"))
                bb20_l_off.append(_apply_bb_offset(float(l20), offset_pct, False) if l20 is not None else float("nan"))
                bb4_u_off.append(_apply_bb_offset(float(u4), offset_pct, True) if u4 is not None else float("nan"))
                bb4_l_off.append(_apply_bb_offset(float(l4), offset_pct, False) if l4 is not None else float("nan"))
            ax.plot(x, bb20_u, color="gray", linewidth=0.8, alpha=0.7, label="20B상단")
            ax.plot(x, bb20_l, color="gray", linewidth=0.8, alpha=0.7, label="20B하단")
            ax.plot(x, bb4_u, color="orange", linewidth=0.7, alpha=0.6, label="4B상단")
            ax.plot(x, bb4_l, color="orange", linewidth=0.7, alpha=0.6, label="4B하단")
            ax.plot(x, bb20_u_off, color="green", linewidth=1.2, linestyle="--", label=f"20B상단(오프셋{offset_pct}%)")
            ax.plot(x, bb20_l_off, color="green", linewidth=1.2, linestyle="--", label=f"20B하단(오프셋{offset_pct}%)")
            ax.plot(x, bb4_u_off, color="purple", linewidth=1.0, linestyle=":", label=f"4B상단(오프셋{offset_pct}%)")
            ax.plot(x, bb4_l_off, color="purple", linewidth=1.0, linestyle=":", label=f"4B하단(오프셋{offset_pct}%)")
            ax.legend(loc="upper left", fontsize=8)
            ax.set_title(f"{sym}  {tf_label}  (과거 3일, 오프셋 적용)", fontsize=11)
            ax.set_xlabel("봉 인덱스", fontsize=9)
            ax.set_ylabel("가격", fontsize=9)
            ax.grid(True, alpha=0.3)
            ax.autoscale(enable=True, axis="both")
            plt.tight_layout()
            canvas = FigureCanvasTkAgg(fig, master=chart_frame)
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
            toolbar_frame = ttk.Frame(chart_frame)
            toolbar_frame.pack(fill=tk.X, padx=2, pady=2)
            toolbar_frame._is_toolbar_frame = True
            NavigationToolbar2Tk(canvas, toolbar_frame)
            canvas.draw()

        ttk.Label(top_f, text="심볼").pack(side=tk.LEFT, padx=(0, 4))
        for s in ("XAUUSD+", "NAS100+"):
            ttk.Radiobutton(top_f, text=s, variable=current_symbol, value=s, command=draw).pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(top_f, text="  타임프레임").pack(side=tk.LEFT, padx=(12, 4))
        for label in ("5분봉", "10분봉", "1시간봉"):
            ttk.Radiobutton(top_f, text=label, variable=current_tf, value=label, command=draw).pack(side=tk.LEFT, padx=(0, 8))
        draw()

    def _on_select(self, ev):
        pass

    def _get_selected_or_focused_reservation_item(self):
        """선택된 트리 항목 반환. 버튼 클릭 시 선택이 비워질 수 있으므로 포커스된 항목을 폴백으로 사용."""
        sel = self.tree.selection()
        if not sel and self.tree.focus():
            sel = (self.tree.focus(),)
        return sel

    def _on_set_active(self, active: bool):
        """선택한 예약을 활성(True) 또는 비활성(False)으로 설정."""
        sel = self._get_selected_or_focused_reservation_item()
        if not sel:
            messagebox.showinfo("활성/비활성", "목록에서 예약을 선택하세요.")
            return
        idx = self.tree.index(sel[0])
        with self._reservations_lock:
            if 0 <= idx < len(self.reservations):
                self.reservations[idx]["active"] = active
        self._update_reservation_count_title()
        self._refresh_tree()
        label = "활성" if active else "비활성"
        self._log(f"선택 예약을 {label}으로 변경했습니다. (메모리만 반영, 파일 미저장)")

    def _on_delete(self):
        sel = self._get_selected_or_focused_reservation_item()
        if not sel:
            messagebox.showinfo("삭제", "목록에서 삭제할 예약을 선택하세요.")
            return
        idx = self.tree.index(sel[0])
        save_ok = False
        with self._reservations_lock:
            if 0 <= idx < len(self.reservations):
                self.reservations.pop(idx)
                path = getattr(self, "_reservations_path", None) or RESERVATIONS_PATH
                try:
                    save_reservations(self.reservations, path)
                    save_ok = True
                except Exception as e:
                    self._log(f"⚠️ 삭제 후 파일 저장 실패: {e} (목록은 메모리에서만 삭제됨, 재시작/새로고침 시 다시 나타날 수 있음)")
                    messagebox.showerror(
                        "저장 실패",
                        f"예약은 목록에서 제거되었지만 파일에 저장하지 못했습니다.\n\n{e}\n\n"
                        "프로그램을 재시작하거나 '새로고침'을 누르면 삭제한 예약이 다시 나타날 수 있습니다.\n"
                        "파일 경로·권한을 확인하세요.",
                    )
        self._refresh_tree()
        self._update_reservation_count_title()
        if save_ok:
            self._log("선택 예약을 삭제했습니다. (파일에 저장됨 — 재시작 후에도 반영)")

    def _on_start_monitor(self):
        if self.monitor_running:
            return
        self.monitor_running = True
        self.monitor_status_var.set("모니터링: 실행 중")
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.monitor_thread = threading.Thread(target=self._run_monitor, daemon=True)
            self.monitor_thread.start()
        self._log("예약 모니터링 실행 중 (봉 마감 시점에만 점검·진입)")

    def _on_stop_monitor(self):
        self.monitor_running = False
        self.monitor_status_var.set("모니터링: 중지됨")
        self._log("예약 모니터링 중지됨")

    def _run_monitor(self):
        while self.monitor_running:
            try:
                # 예약 체크·봉 마감 텔레그램은 현재 메모리의 예약 목록만 사용 (파일 재로드 안 함 → GUI에서 삭제한 내용이 그대로 반영)
                with self._reservations_lock:
                    active_list = [r for r in self.reservations if r.get("active", True)]
                now_kst = datetime.now(KST)

                # 토 07:00 ~ 월 07:30(KST): 예약 오더 점검·실시간+KTR·봉 마감 텔레그램 미실행
                if _is_weekend_off_window(now_kst):
                    time.sleep(60.0)
                    continue

                # [우선 실행] 실시간+KTR 예약: 가격 도달 시 1건 예약 주문 실행 (매 사이클 점검)
                if now_kst.hour != 7 and tr.init_mt5():
                    for r in active_list:
                        conditions = r.get("conditions", [])
                        if not conditions or conditions[0] not in REALTIME_KTR_CONDITIONS:
                            continue
                        trigger_price = r.get("realtime_ktr_trigger_price")
                        if trigger_price is None:
                            continue
                        symbol = r.get("symbol", "")
                        side = (r.get("side") or "BUY").strip().upper()
                        ask, bid = tr.get_market_price(symbol) if hasattr(tr, "get_market_price") else (None, None)
                        if bid is None or ask is None:
                            tick = mt5.symbol_info_tick(symbol) if mt5.symbol_select(symbol, True) else None
                            if tick is None:
                                continue
                            ask = getattr(tick, "ask", None)
                            bid = getattr(tick, "bid", None)
                        if bid is None or ask is None:
                            continue
                        trigger_price_f = float(trigger_price)
                        reached = (side == "BUY" and bid >= trigger_price_f) or (side == "SELL" and ask <= trigger_price_f)
                        if reached:
                            self.root.after(0, lambda res=r: self._execute_realtime_ktr_single(res))

                # [우선 실행] +KTR 되돌림: 캔들이 트리거를 초과했다가 다시 트리거로 돌아왔을 때 시장가 실행
                if now_kst.hour != 7 and tr.init_mt5():
                    with self._retrace_lock:
                        retrace_list = list(self._realtime_ktr_retrace_list)
                    to_remove = []
                    for item in retrace_list:
                        symbol_r = item.get("symbol", "")
                        if not symbol_r:
                            continue
                        sym_mt5 = (symbol_r.strip() + "+") if symbol_r.strip() and not symbol_r.strip().endswith("+") else symbol_r.strip()
                        if not mt5.symbol_select(sym_mt5, True):
                            continue
                        # 5분봉: 테이블(DB) 우선, 없으면 MT5 (get_rates_for_timeframe)
                        mt5_m5 = getattr(mt5, "TIMEFRAME_M5", 5)
                        rates = get_rates_for_timeframe(sym_mt5, mt5_m5, 3)
                        tick = mt5.symbol_info_tick(sym_mt5)
                        if rates is None or len(rates) < 2 or tick is None:
                            continue
                        ask = getattr(tick, "ask", None)
                        bid = getattr(tick, "bid", None)
                        if ask is None or bid is None:
                            continue
                        trigger_f = float(item["trigger_price"])
                        is_buy = (item.get("side") or "BUY").strip().upper() == "BUY"
                        # DB는 [0]=최신, MT5는 [-1]=최신 → 최근 2봉 high/low 통일 계산
                        if int(rates["time"][0]) > int(rates["time"][-1]):
                            h1, l1 = float(rates["high"][0]), float(rates["low"][0])
                            h2, l2 = float(rates["high"][1]), float(rates["low"][1])
                        else:
                            h1, l1 = float(rates["high"][-1]), float(rates["low"][-1])
                            h2, l2 = float(rates["high"][-2]), float(rates["low"][-2])
                        candle_high = max(h1, h2)
                        candle_low = min(l1, l2)
                        if not item.get("exceeded"):
                            if is_buy:
                                if candle_low <= trigger_f:
                                    item["exceeded"] = True
                            else:
                                if candle_high >= trigger_f:
                                    item["exceeded"] = True
                        if not item.get("exceeded"):
                            continue
                        retraced = (is_buy and bid >= trigger_f) or (not is_buy and ask <= trigger_f)
                        if not retraced:
                            continue
                        to_remove.append(item)
                        acc = tr.get_account_info()
                        balance = (acc.get("balance") or 0) if acc else 0
                        self.root.after(
                            0,
                            lambda it=item, bal=balance: self._execute_realtime_ktr_retrace_order(it, bal),
                        )
                    if to_remove:
                        with self._retrace_lock:
                            for it in to_remove:
                                if it in self._realtime_ktr_retrace_list:
                                    self._realtime_ktr_retrace_list.remove(it)

                # 트레일링 스탑: 등록된 티켓에 대해 10분봉 캔들가 기준 이익 50% 보전 S/L 갱신
                if now_kst.hour != 7 and tr.init_mt5():
                    def _trailing_log_fn(msg):
                        try:
                            self.root.after(0, lambda m=msg: self._log(m))
                        except Exception:
                            pass
                    _update_trailing_stop_50pct(_trailing_log_fn)

                # [우선 실행] 예약 오더 진입 점검·실행 — 해당 타임프레임 봉 마감 시점에만 점검·진입
                reservation_check_log_lines = []  # 미충족 시 텔레그램 전송용 (맨 윗줄 "[예약 체크]"는 전송 시에만 추가)
                if now_kst.hour != 7:
                    matched_by_symbol: Dict[str, List[tuple]] = {}
                    check_index = 0
                    for r in active_list:
                        symbol = r.get("symbol", "")
                        mt5_tf = r.get("mt5_timeframe", mt5.TIMEFRAME_H1)
                        conditions = r.get("conditions", [])
                        if not conditions:
                            continue
                        if conditions[0] in REALTIME_KTR_CONDITIONS:
                            continue  # 실시간+KTR은 위에서 별도 처리
                        # 모든 진입조건: 해당 TF 봉 직전 마감 시점에만 점검 (봉 마감 후 진입)
                        is_bar_closed = _is_bar_just_closed_for_timeframe_kst(mt5_tf, now_kst)
                        if not is_bar_closed:
                            continue
                        matched, matched_cond, detail_msg = check_entry_condition_with_detail(
                            symbol, mt5_tf, conditions
                        )
                        if matched:
                            side_r = (r.get("side") or "BUY").strip().upper()
                            tf_str_r = _MT5_TF_TO_STR.get(mt5_tf, "H1")
                            entry_time_s = (r.get("entry_time") or "").strip()
                            if entry_time_s:
                                entry_dt = _parse_scheduled_time_kst(entry_time_s)
                                if entry_dt is not None and now_kst < entry_dt:
                                    self.root.after(
                                        0,
                                        lambda sym=symbol, et=entry_time_s: self._log(
                                            f"⏭️ {sym} 진입조건 충족 but 진입시간({et}) 미도달 → 대기"
                                        ),
                                    )
                                    continue
                            if _allowed_by_sma20_filter(symbol, matched_cond or "", side_r, tf_str_r):
                                matched_by_symbol.setdefault(symbol, []).append((r, matched_cond, detail_msg))
                            else:
                                pos_tf = _tf_sma20_position(symbol, tf_str_r) if side_r == "BUY" else _1h_sma20_position(symbol)
                                req = f"{tf_str_r} 20이평 아래" if side_r == "BUY" else "1H 20이평 위"
                                self.root.after(
                                    0,
                                    lambda s=symbol, c=matched_cond, p=pos_tf, r=req: self._log(
                                        f"⏭️ {s} [{c}] 조건 충족 but 20이평 필터 미충족 (현재={p}, 요구={r}) → 오더 생성 안 함"
                                    ),
                                )
                        else:
                            tf_str = _MT5_TF_TO_STR.get(mt5_tf, "?")
                            for c in conditions:
                                _, why_msg, brief = _check_entry_condition_one_with_detail(symbol, mt5_tf, c)
                                if why_msg:
                                    check_index += 1
                                    bar_time_kst = _last_closed_bar_display_kst(tf_str).strftime("%Y-%m-%d %H:%M")
                                    log_lines = _format_reservation_check_log(
                                        check_index, symbol, tf_str, c, why_msg,
                                        bar_time_kst=bar_time_kst, brief_second_line=brief
                                    )
                                    # 타임프레임 봉 마감 시점에 점검한 경우만 텔레그램 전송
                                    reservation_check_log_lines.extend(log_lines)
                                    reservation_check_log_lines.append("")  # 블록 구분용 빈 줄
                                    # GUI: 첫 번째 점검일 때만 "[예약 체크 - 점검 시각]" 헤더 출력
                                    if check_index == 1:
                                        header = f"[예약 체크 - 점검 시각 {now_kst.strftime('%Y-%m-%d %H:%M')} KST]"
                                        self.root.after(0, lambda h=header: self._log(h))
                                    for log_line in log_lines:
                                        self.root.after(0, lambda msg=log_line: self._log(msg))
                                    self.root.after(0, lambda: self._log(""))
                    for symbol, candidates in matched_by_symbol.items():
                        best = max(
                            candidates,
                            key=lambda x: _TF_EXECUTION_ORDER.get((x[0].get("timeframe_label") or "").strip(), -1),
                        )
                        res, matched_cond, detail_msg = best
                        self.root.after(
                            0,
                            lambda res=res, cond=matched_cond, msg=detail_msg: self._trigger_entry(
                                res, detail_msg=msg, matched_condition=cond
                            ),
                        )
                        if len(candidates) > 1:
                            tf_label = (res.get("timeframe_label") or "").strip()
                            self.root.after(
                                0,
                                lambda s=symbol, tf=tf_label, n=len(candidates): self._log(
                                    f"[예약 실행] {s} 동시 {n}건 충족 → 가장 큰 TF({tf}) 1건만 실행"
                                ),
                            )
                    if reservation_check_log_lines:
                        try:
                            telegram_header = f"[예약 체크 - 점검 시각 {now_kst.strftime('%Y-%m-%d %H:%M')} KST]"
                            telegram_body = telegram_header + "\n" + "\n".join(reservation_check_log_lines)
                            send_telegram_msg(telegram_body, parse_mode="")
                        except Exception:
                            pass

                # 10분/1시간/2시간/4시간봉 마감 시점에 심볼별·진입조건별 충족 여부를 텔레그램으로 전송 (한 줄이 길어지지 않도록 여러 줄로 구분)
                # 동일 (tf_label, 봉 시각)에 대해 한 번만 전송: DB(telegram_bar_sent 테이블)로 프로세스 간 중복 방지, 같은 프로세스 내에는 _last_bar_telegram_sent
                _clean_old_telegram_bar_sent_locks()  # 오늘 이전 DB 이력 삭제 + 레거시 .telegram_bar_sent 폴더 삭제
                bar_key = now_kst.strftime("%Y-%m-%d %H:%M")
                for tf_label, mt5_tf in TF_MAP.items():
                    if not _is_bar_just_closed_for_timeframe_kst(mt5_tf, now_kst):
                        continue
                    last_sent = getattr(self, "_last_bar_telegram_sent", None) or {}
                    if last_sent.get(tf_label) == bar_key:
                        _bar_telegram_log(tf_label, bar_key, "스킵_같은프로세스")
                        self.root.after(
                            0,
                            lambda tf=tf_label, bk=bar_key: self._log(
                                f"[봉 마감 텔레그램] {tf} {bk} — 스킵(같은 프로세스에서 이미 전송)"
                            ),
                        )
                        continue
                    lock_path = _bar_telegram_lock_path(tf_label, bar_key)
                    _bar_telegram_log(tf_label, bar_key, "시도")
                    self.root.after(
                        0,
                        lambda tf=tf_label, bk=bar_key, lp=lock_path: self._log(
                            f"[봉 마감 텔레그램] {tf} {bk} 전송 시도 (락: {lp})"
                        ),
                    )
                    if not _try_acquire_bar_telegram_sent(tf_label, bar_key):
                        _bar_telegram_log(tf_label, bar_key, "스킵_락실패")
                        self.root.after(
                            0,
                            lambda tf=tf_label, bk=bar_key, lp=lock_path: self._log(
                                f"[봉 마감 텔레그램] {tf} {bk} — 스킵(락 실패, 이미 전송됨) 락={lp}"
                            ),
                        )
                        continue
                    try:
                        pid = os.getpid()
                        lines = [
                            f"📊 **[{tf_label} 마감]** {bar_key} KST",
                            f"트리거: ktr_order_reservation_gui  PID={pid}",
                            "",
                        ]
                        for symbol in ("XAUUSD+", "NAS100+"):
                            lines.append(f"**{symbol}**")
                            for cond in ENTRY_CONDITIONS:
                                matched, _, _ = _check_entry_condition_one_with_detail(symbol, mt5_tf, cond)
                                lines.append(f"  • {cond}: {'충족' if matched else '미충족'}")
                            lines.append("")
                        # 예약 오더: 이번 봉 타임프레임과 일치하는 예약만 해당 TF 기준으로 조건 점검 후 텔레그램에 기록
                        reservations_for_tf = [
                            r for r in active_list
                            if (r.get("timeframe_label") or "").strip() == tf_label
                        ]
                        if reservations_for_tf:
                            lines.append("**예약 오더 (해당 TF)**")
                            for r in reservations_for_tf:
                                symbol = r.get("symbol", "").strip()
                                side = (r.get("side") or "BUY").strip().upper()
                                side_label = "매수" if side == "BUY" else "매도"
                                conditions = r.get("conditions", [])
                                if not symbol or not conditions:
                                    continue
                                matched, matched_cond, detail_msg = check_entry_condition_with_detail(
                                    symbol, mt5_tf, conditions
                                )
                                cond_str = ", ".join(conditions)
                                if matched:
                                    lines.append(f"  • {symbol} {side_label} {cond_str} → **충족** ({matched_cond})")
                                else:
                                    brief = (detail_msg or "미충족")[:60]
                                    if len((detail_msg or "")) > 60:
                                        brief += "..."
                                    lines.append(f"  • {symbol} {side_label} {cond_str} → 미충족")
                                    lines.append(f"    └ {brief}")
                            lines.append("")
                        send_telegram_msg("\n".join(lines).strip(), parse_mode="Markdown")
                        self._last_bar_telegram_sent[tf_label] = bar_key
                        _bar_telegram_log(tf_label, bar_key, "전송완료")
                        self.root.after(
                            0,
                            lambda tf=tf_label, bk=bar_key: self._log(f"[봉 마감 텔레그램] {tf} {bk} — 전송 완료"),
                        )
                    except Exception as e:
                        _bar_telegram_log(tf_label, bar_key, "전송실패_" + str(e).replace("\n", " ")[:80])
                        self.root.after(0, lambda err=e: self._log(f"[진입조건 텔레그램] {tf_label} 전송 실패: {err}"))

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                self.root.after(0, lambda: self._log(f"모니터 오류: {e}"))
                self.root.after(0, lambda t=tb: self._log(t))
            # 타임프레임별 다음 봉 마감 시각까지 대기(최대 60초). 1분 단위로 예약 체크하지 않음.
            try:
                _al = active_list
            except NameError:
                _al = []
            next_close = _next_bar_close_kst(_al) if _al else None
            if next_close is not None:
                remain = (next_close - datetime.now(KST)).total_seconds()
                sleep_sec = max(5.0, min(60.0, remain)) if remain > 0 else 60.0
            else:
                sleep_sec = 60.0
            time.sleep(sleep_sec)

    def _trigger_entry(
        self,
        res: Dict[str, Any],
        detail_msg: Optional[str] = None,
        matched_condition: Optional[str] = None,
    ):
        """조건 충족 시 마진 레벨 확인 후, 500% 초과일 때만 KTR 진입 실행.
        같은 심볼: 미청산이 있으면 더 큰 TF(1H>10M)일 때만 기존 포지션 청산 후 신규 실행."""
        symbol = res.get("symbol", "")
        conditions = res.get("conditions", [])
        cond_desc = matched_condition or (", ".join(conditions) if conditions else "—")
        tf_label = (res.get("timeframe_label") or "").strip() or "—"
        side_label = "매수" if (res.get("side", "BUY").strip().upper() == "BUY") else "매도"
        self._log(
            f"[예약 실행 검토] {symbol} ({tf_label} {side_label} [{cond_desc}]) 조건 충족 → 스킵 시 원인: 동일 1시간봉 이미 실행 / 미청산 예약 / KTR 포지션 있음 / 마진≤{MARGIN_LEVEL_MIN_PCT}% / KTR DB 없음"
        )
        new_tf = (res.get("ktr_tf") or "1H").strip().upper()
        if not new_tf.endswith("M") and new_tf != "1H":
            new_tf = "1H"

        bar_key = _current_1h_bar_key_kst()
        exec_data = _load_execution_1h_bar()
        # 같은 봉에서 이미 실행된 적 있어도, 해당 심볼에 KTR 포지션/대기오더가 없으면 재실행 허용
        if exec_data.get(symbol) == bar_key:
            has_ktr_position_or_pending = False
            if tr.init_mt5():
                positions = mt5.positions_get(symbol=symbol)
                ktr_positions = [p for p in (positions or []) if getattr(p, "magic", 0) == MAGIC_KTR]
                orders = mt5.orders_get(symbol=symbol) or []
                ktr_pending = [o for o in orders if getattr(o, "magic", 0) == MAGIC_KTR]
                has_ktr_position_or_pending = bool(ktr_positions or ktr_pending)
            if has_ktr_position_or_pending:
                self._log(
                    f"⏭️ {symbol}: 동일 1시간봉({bar_key})에서 이미 예약 실행됨 + KTR 포지션/대기오더 있음 → 추가 실행 생략 "
                    f"(10M 조건이라도 같은 1H 봉 안에서는 1회만 실행)"
                )
                return
            self._log(
                f"▶️ {symbol}: 동일 1시간봉({bar_key}) 실행 이력 있으나 포지션/대기오더 없음 → 재실행 허용"
            )

        # 해당 심볼에 KTR 포지션이 있고 대기 중인 KTR 오더도 있으면: 1차만 500% 마진 유지 비중으로 진입, 2~N차 오더는 생성 안 함
        # 포지션만 있고 대기 오더 없으면: 기존처럼 신규 실행 보류
        only_first_at_200_margin = False
        if tr.init_mt5():
            positions = mt5.positions_get(symbol=symbol)
            ktr_positions = [p for p in (positions or []) if getattr(p, "magic", 0) == MAGIC_KTR]
            orders = mt5.orders_get(symbol=symbol) or []
            ktr_pending = [o for o in orders if getattr(o, "magic", 0) == MAGIC_KTR]
            if ktr_positions and ktr_pending:
                only_first_at_200_margin = True
                self._log(
                    f"[예약 실행] {symbol}: 포지션 {len(ktr_positions)}건 + 대기 KTR 오더 {len(ktr_pending)}건 있음 → "
                    "1차만 500% 마진 유지 비중으로 진입, 2~N차 오더 미생성"
                )
            elif ktr_positions:
                self._log(f"⏭️ {symbol}: 이미 해당 심볼 KTR 포지션 {len(ktr_positions)}건 있음 → 신규 실행 보류 (청산 후 실행 가능)")
                return

        side = res.get("side", "BUY")
        conditions = res.get("conditions", [])
        cond = matched_condition or (conditions[0] if conditions else "")
        # 기본더블비 + N값 1.5 조합은 허용하지 않음 (오더 생성 안 함)
        if "기본더블비" in (conditions or []):
            n_val_res = res.get("n_value")
            n_disp_res = res.get("n_display", "")
            if n_val_res == 1.5 or n_disp_res == "1.5":
                self._log(
                    "⏭️ 기본더블비는 N값 2.5 이상 또는 '없음'만 허용됩니다. 해당 예약(N=1.5)은 실행하지 않습니다."
                )
                return
        self._log(f"진입 조건 충족: {symbol} {side} [{cond}] → KTR 진입 시도")
        if detail_msg:
            self._log(f"  상세: {detail_msg}")

        # 수집한 로그를 텔레그램으로 보낼 목록 (진입 시도 헤더 포함)
        tf_label = (res.get("timeframe_label") or "").strip() or "—"
        telegram_lines = [
            f"📋 KTR 예약 실행 로그",
            f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"심볼: {symbol} | {side} | 타임프레임: {tf_label} | 진입조건: {cond}",
        ]
        if detail_msg:
            telegram_lines.append(f"상세: {detail_msg}")
        telegram_lines.append("")

        if not tr.init_mt5():
            self._log("MT5 연결 실패로 진입 보류")
            telegram_lines.append("❌ 오더 미실행: MT5 연결 실패. 터미널 실행 및 로그인 확인.")
            try:
                send_telegram_msg("\n".join(telegram_lines), parse_mode="")
            except Exception:
                pass
            return
        acc = tr.get_account_info()
        if acc is None:
            self._log("계정 정보 조회 실패로 진입 보류")
            telegram_lines.append("❌ 오더 미실행: 계정 정보 조회 실패.")
            try:
                send_telegram_msg("\n".join(telegram_lines), parse_mode="")
            except Exception:
                pass
            return
        margin_level = acc.get("margin_level") or 0
        if margin_level > 0 and margin_level <= MARGIN_LEVEL_MIN_PCT:
            msg = (
                f"⚠️ KTR 예약 주문 보류 (마진 레벨 부족)\n"
                f"심볼: {symbol} / {res.get('side', '')} / 진입조건: {', '.join(conditions)}\n"
                f"현재 마진 레벨: {margin_level:.1f}% (실행 조건: {MARGIN_LEVEL_MIN_PCT}% 초과)\n"
                f"주문은 실행하지 않았습니다. 마진 확보 후 다음 주기에 재시도됩니다."
            )
            self._log(f"마진 레벨 {margin_level:.1f}% (≤{MARGIN_LEVEL_MIN_PCT}%) → 주문 미실행, 텔레그램 발송")
            send_telegram_msg(msg, parse_mode="")
            return

        def log_fn(msg):
            self._log(msg)
            telegram_lines.append(msg)

        # 기존 KTR 포지션이 있으면, 진입 예정가가 기존 포지션 진입가로부터 위·아래 1 KTR 이내면 추가 진입하지 않음
        positions_now = mt5.positions_get(symbol=symbol) or []
        ktr_positions_now = [p for p in positions_now if getattr(p, "magic", 0) == MAGIC_KTR]
        if ktr_positions_now:
            ktr_value, _, _, _ = get_ktr_from_db_with_fallback(
                symbol, res.get("session", "자동"), res.get("ktr_tf", "1H")
            )
            if ktr_value and ktr_value > 0:
                one_ktr_price = float(ktr_value)
                tick = mt5.symbol_info_tick(symbol)
                if tick is None:
                    tick = mt5.symbol_info_tick(symbol.replace(" ", ""))
                if tick is not None:
                    is_buy = (res.get("side", "BUY").strip().upper() == "BUY")
                    entry_price = tick.ask if is_buy else tick.bid
                    for pos in ktr_positions_now:
                        if abs(entry_price - pos.price_open) <= one_ktr_price:
                            self._log(
                                f"⏭️ {symbol}: 기존 포지션(진입가 {pos.price_open:.5g})으로부터 1 KTR 이내(진입 예정가 {entry_price:.5g}) → 추가 진입 생략"
                            )
                            telegram_lines.append("⏭️ 기존 포지션으로부터 1 KTR 이내라 추가 진입하지 않음.")
                            try:
                                send_telegram_msg("\n".join(telegram_lines), parse_mode="")
                            except Exception:
                                pass
                            return

        wp = res.get("weight_pct", 10)
        if wp != "전저점":
            wp = float(wp) if wp is not None else 10.0
        if only_first_at_200_margin:
            wp = 0.0
            num_positions_res = 1
        else:
            num_positions_res = int(res.get("num_positions", 3))
        ok, entry_price, first_lot = _execute_ktr_entry(
            symbol=symbol,
            side=side,
            weight_pct=wp,
            n_value=float(res.get("n_value", 2.5)),
            num_positions=num_positions_res,
            sl_from_n=bool(res.get("sl_from_n", True)),
            session=res.get("session", "자동"),
            tf=res.get("ktr_tf", "1H"),
            tp_option=res.get("tp_option", "20이평"),
            sl_option=res.get("sl_option", "잔액비 -10%"),
            log_fn=log_fn,
            order_id=str(res.get("id", "")),
            entry_conditions=res.get("conditions", []),
            source="reservation",
            tf_label=(res.get("timeframe_label") or "").strip(),
            ktr_multiplier=2.0 if res.get("ktr_x2") else 1.0,
        )
        if ok:
            _save_execution_1h_bar(symbol, bar_key)
            self._log("예약 실행 완료. (활성 상태는 유지되며, 수동으로 비활성화할 수 있습니다.)")
            telegram_lines.insert(3, "✅ 실행 결과: 성공")
        else:
            self._log("진입 실행 실패. 다음 주기에 다시 시도합니다.")
            telegram_lines.insert(3, "❌ 실행 결과: 실패 (사유는 아래 로그 참고)")
        try:
            send_telegram_msg("\n".join(telegram_lines), parse_mode="")
        except Exception:
            pass

    def _execute_realtime_ktr_retrace_order(self, item: Dict[str, Any], balance: float) -> None:
        """+KTR 되돌림 조건 충족 시: 저장된 항목으로 시장가 1건 실행 후 S/L·T/P 설정. GUI 스레드에서 호출."""
        self._execute_realtime_ktr_immediate_market(
            item["symbol"],
            item["side"],
            item["session"],
            item["tf"],
            item["ktr_value"],
            item["n_value"],
            item["weight_pct"],
            item["tp_option"],
            item["sl_option"],
            item["entry_tf_label"],
            item["add_ktr_mult_str"],
            item.get("tp_ktr_multiplier"),
            balance,
            item["cond_label"],
        )
        self._log(f"  [실시간+KTR 되돌림] {item.get('cond_label', '')} 트리거 {item.get('trigger_price', 0):.5g} 되돌림 충족 → 시장가 실행 완료")

    def _execute_realtime_ktr_immediate_market(
        self,
        symbol: str,
        side: str,
        session: str,
        tf: str,
        ktr_value: float,
        n_value: float,
        weight_pct: float,
        tp_option: str,
        sl_option: str,
        entry_tf_label: str,
        add_ktr_mult_str: str,
        tp_ktr_multiplier: Optional[float],
        balance: float,
        cond_label: str,
    ) -> bool:
        """실시간 진입 직후 +KTR: 예약 오더 없이 시장가 1건 체결 후 KTR 기준 S/L·T/P 설정. 실패 시 로그만 남김."""
        sym_mt5 = (symbol or "").strip()
        if sym_mt5 and not sym_mt5.endswith("+"):
            sym_mt5 = sym_mt5 + "+"
        if not tr.init_mt5():
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} MT5 미연결 → 시장가+KTR 스킵")
            return False
        if balance <= 0:
            acc = tr.get_account_info()
            balance = (acc.get("balance") or 0) if acc else 0
        if balance <= 0:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 잔고 없음 → 시장가+KTR 스킵")
            return False
        current_weight = _current_ktr_weight_pct(balance)
        if current_weight >= WEIGHT_PCT_MAX_FOR_NEW_ORDER:
            self._log(
                f"  [실시간+KTR] {sym_mt5 or symbol} 기존 진입 비중 {current_weight:.1f}% (≥{WEIGHT_PCT_MAX_FOR_NEW_ORDER}%) → 시장가+KTR 스킵"
            )
            return False
        lots_map = get_ktrlots_lots(
            balance, weight_pct, n_value, ktr_value, symbol_for_db(symbol or sym_mt5), headless=True
        )
        if not lots_map:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 랏수 조회 실패 → 시장가+KTR 스킵")
            return False
        lot = lots_map.get("1st") or 0
        if not lot or lot <= 0:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 1st 랏 없음 → 시장가+KTR 스킵")
            return False
        ktr_price = float(ktr_value)
        is_buy = (side or "").strip().upper() == "BUY"
        comment = _build_order_comment(
            "KTR+1" if "KTR1" in (cond_label or "") else "KTR+2",
            session, "1H", tp_option, sl_option, True
        )
        ok, msg = tr.execute_market_order(sym_mt5, side, lot, magic=MAGIC_KTR, comment=comment)
        if not ok:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 시장가 진입 실패: {msg}")
            return False
        for _ in range(5):
            time.sleep(0.4)
            positions = mt5.positions_get(symbol=sym_mt5)
            if not positions and (symbol or "").rstrip("+"):
                positions = mt5.positions_get(symbol=(symbol or "").rstrip("+"))
            if positions:
                positions = [p for p in positions if getattr(p, "magic", 0) == MAGIC_KTR]
            if positions:
                break
        if not positions:
            all_pos = mt5.positions_get()
            if all_pos:
                positions = [
                    p for p in all_pos
                    if getattr(p, "magic", 0) == MAGIC_KTR
                    and (getattr(p, "symbol", "") == sym_mt5 or getattr(p, "symbol", "") == (symbol or "").rstrip("+"))
                ]
        if not positions:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 시장가 체결 후 포지션 조회 실패 → S/L 설정 생략")
            return True
        last_pos = max(positions, key=lambda p: getattr(p, "time", 0))
        entry_new = getattr(last_pos, "price_open", None)
        if entry_new is None:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 진입가 조회 실패 → S/L 설정 생략")
            return True
        entry_new = float(entry_new)
        sl_1 = (entry_new - n_value * ktr_price) if is_buy else (entry_new + n_value * ktr_price)
        tp_1 = 0.0
        if tp_ktr_multiplier is not None and tp_ktr_multiplier > 0:
            tp_1 = (entry_new + ktr_price * tp_ktr_multiplier) if is_buy else (entry_new - ktr_price * tp_ktr_multiplier)
        pos_symbol = getattr(last_pos, "symbol", sym_mt5)
        ok_sltp, msg_sltp = tr.modify_position_sltp(last_pos.ticket, pos_symbol, sl_1, tp_1)
        if ok_sltp:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} #{cond_label} 시장가+KTR 성공: {lot}랏 S/L={sl_1:.5g}" + (f" T/P={tp_1:.5g}" if tp_1 > 0 else ""))
        else:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 시장가+KTR S/L 설정 실패: {msg_sltp}")
        return True

    def _execute_realtime_ktr_single(self, res: Dict[str, Any]):
        """실시간+KTR1/2 예약: 가격 도달 시 1건 예약 주문만 실행 후 해당 예약 비활성화."""
        symbol = res.get("symbol", "")
        # MT5 주문용 심볼은 + 접미사 필요 (브로커별)
        sym_mt5 = (symbol or "").strip()
        if sym_mt5 and not sym_mt5.endswith("+"):
            sym_mt5 = sym_mt5 + "+"
        conditions = res.get("conditions", [])
        if not conditions or conditions[0] not in REALTIME_KTR_CONDITIONS:
            return
        trigger_price = res.get("realtime_ktr_trigger_price")
        if trigger_price is None:
            return
        side = (res.get("side") or "BUY").strip().upper()
        session = res.get("session", "자동")
        tf = res.get("ktr_tf", "1H")
        wp = res.get("weight_pct", 10)
        try:
            weight_pct = float(wp) if wp != "전저점" else 10.0
        except (TypeError, ValueError):
            weight_pct = 10.0
        if weight_pct <= 0:
            weight_pct = 10.0
        n_value = float(res.get("n_value", 2.5))
        cond_label = conditions[0]
        if not tr.init_mt5():
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} MT5 미연결 → 스킵")
            return
        info = mt5.account_info()
        balance = getattr(info, "balance", 0) if info else 0
        if balance <= 0:
            acc = tr.get_account_info()
            balance = (acc.get("balance") or 0) if acc else 0
        current_weight = _current_ktr_weight_pct(balance)
        if current_weight >= WEIGHT_PCT_MAX_FOR_NEW_ORDER:
            self._log(
                f"  [실시간+KTR] {sym_mt5 or symbol} 기존 진입 오더 비중 {current_weight:.1f}% (≥{WEIGHT_PCT_MAX_FOR_NEW_ORDER}%) → 추가 오더 생성 금지"
            )
            res["active"] = False
            return
        ktr_value, resolved_session, _, _ = get_ktr_from_db_with_fallback(symbol or sym_mt5, session, tf)
        if not ktr_value or ktr_value <= 0:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} KTR 없음 → 스킵")
            return
        acc = tr.get_account_info()
        balance = (acc.get("balance") or 0) if acc else 0
        lots_map = get_ktrlots_lots(
            balance, weight_pct, n_value, ktr_value, symbol_for_db(symbol or sym_mt5), headless=True
        )
        if not lots_map:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 랏수 조회 실패 → 스킵")
            return
        lot = lots_map.get("1st") or 0
        if not lot or lot <= 0:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 1st 랏 없음 → 스킵")
            return
        tp_option = res.get("tp_option", "사용하지 않음")
        sl_option = res.get("sl_option", "사용하지 않음")
        comment_tf = "1H"
        pending_comment = _build_order_comment(
            "KTR+1" if "KTR1" in (cond_label or "") else "KTR+2",
            resolved_session or session, comment_tf, tp_option, sl_option, True
        )
        trigger_f = float(trigger_price)
        # 매수 시 가격 > 현재가 → BUY_STOP, 매도 시 가격 < 현재가 → SELL_STOP. 그 외는 LIMIT.
        ask, bid = tr.get_market_price(sym_mt5) if hasattr(tr, "get_market_price") else (None, None)
        use_stop = False
        if side == "BUY" and ask is not None and trigger_f > ask:
            use_stop = True
        elif side == "SELL" and bid is not None and trigger_f < bid:
            use_stop = True
        if use_stop:
            ok, msg = tr.place_pending_stop(
                sym_mt5, side, lot, trigger_f, magic=MAGIC_KTR, comment=pending_comment
            )
        else:
            ok, msg = tr.place_pending_limit(
                sym_mt5, side, lot, trigger_f, magic=MAGIC_KTR, comment=pending_comment
            )
        if ok:
            order_type = "Stop" if use_stop else "Limit"
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} #{cond_label} 예약 주문 성공 ({order_type}): {lot}랏 @ {trigger_price:.5g}")
            res["active"] = False
        else:
            self._log(f"  [실시간+KTR] {sym_mt5 or symbol} 예약 주문 실패: {msg}")
        with self._reservations_lock:
            save_reservations(self.reservations, getattr(self, "_reservations_path", None))
        self._refresh_tree()
        self._update_reservation_count_title()

    def run(self):
        self.root.mainloop()
        self.monitor_running = False

    def _on_close(self):
        self.monitor_running = False
        try:
            from single_instance import release_single_instance
            release_single_instance("ktr_order_reservation_gui", _SCRIPT_DIR)
        except Exception:
            pass
        self.root.destroy()
        os._exit(0)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    # 중복 실행 방지
    try:
        from single_instance import (
            try_acquire_single_instance,
            kill_process_forcefully,
            force_remove_lock,
        )
        root = tk.Tk()
        root.withdraw()
        acquired, existing_pid = try_acquire_single_instance("ktr_order_reservation_gui", _SCRIPT_DIR)
        if not acquired:
            if existing_pid is not None:
                if messagebox.askyesno(
                    "중복 실행",
                    "KTR 예약·실시간 오더가 이미 실행 중입니다.\n기존 인스턴스를 종료하고 새로 시작할까요?",
                ):
                    kill_process_forcefully(existing_pid, wait_after_sec=1.5)
                    force_remove_lock("ktr_order_reservation_gui", _SCRIPT_DIR)
                    time.sleep(2)
                    acquired, _ = try_acquire_single_instance("ktr_order_reservation_gui", _SCRIPT_DIR)
                    if not acquired:
                        messagebox.showerror(
                            "오류",
                            "기존 인스턴스를 종료했습니다.\n잠시 후 다시 실행해 주세요.",
                        )
                        root.destroy()
                        sys.exit(1)
                else:
                    root.destroy()
                    sys.exit(0)
            else:
                messagebox.showwarning("중복 실행", "KTR 예약·실시간 오더가 이미 실행 중입니다.\n다른 창을 확인해 주세요.")
                root.destroy()
                sys.exit(1)
        root.destroy()
    except ImportError:
        pass
    app = KTRReservationApp()
    app.run()
