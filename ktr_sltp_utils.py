# -*- coding: utf-8 -*-
"""
KTR 주문의 차익실현(TP)/손절(SL) 계산 공통 로직.
ktr_order_gui, ktr_sltp_updater에서 공통 사용.
포지션별 적용 옵션 override (MT5 코멘트 수정 불가 대안) 저장/로드.
"""
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

import MetaTrader5 as mt5
import pytz

KST = pytz.timezone("Asia/Seoul")

from mt5_trade_utils import init_mt5
from ktr_db_utils import KTRDatabase

try:
    import position_monitor_db as _pm_db
except ImportError:
    _pm_db = None

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RATES_COUNT = 150  # 120이평 등 계산용 1H 봉 수


def _sma(closes: list, period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _bb_upper(closes: list, period: int, num_std: float) -> Optional[float]:
    """BB 상단. Pine Script ta.stdev와 동일하게 표본 표준편차(n-1) 사용."""
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    var = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = var ** 0.5 if var > 0 else 0.0
    return mid + num_std * std


def _bb_lower(closes: list, period: int, num_std: float) -> Optional[float]:
    """BB 하단. Pine Script ta.stdev와 동일하게 표본 표준편차(n-1) 사용."""
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    var = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = var ** 0.5 if var > 0 else 0.0
    return mid - num_std * std


_MT5_TF_TO_STR = {
    getattr(mt5, "TIMEFRAME_M5", 5): "M5",
    getattr(mt5, "TIMEFRAME_M10", 10): "M10",
    getattr(mt5, "TIMEFRAME_H1", 16385): "H1",
}


def get_rates_for_timeframe(symbol: str, mt5_timeframe: int, count: int = 30):
    """지정 타임프레임의 최근 봉 조회. XAUUSD+/NAS100+ 및 M5/M10/H1은 DB 우선."""
    if _pm_db and symbol in ("XAUUSD+", "NAS100+"):
        tf_str = _MT5_TF_TO_STR.get(mt5_timeframe)
        if tf_str:
            rates = _pm_db.get_rates_from_db(symbol, tf_str, limit=count)
            if rates is not None and len(rates) >= 2:
                return rates
    if not init_mt5():
        return None
    if not mt5.symbol_select(symbol, True):
        return None
    return mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, count)


def get_1h_rates(symbol: str):
    """1H 봉 조회. XAUUSD+/NAS100+는 DB 우선, 없거나 부족하면 MT5."""
    if _pm_db and symbol in ("XAUUSD+", "NAS100+"):
        rates = _pm_db.get_rates_from_db(symbol, "H1", limit=RATES_COUNT)
        if rates is not None and len(rates) >= 2:
            return rates
    if not init_mt5():
        return None
    if not mt5.symbol_select(symbol, True):
        return None
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_H1, 0, RATES_COUNT)
    return rates


def get_tp_level(symbol: str, option: str) -> Optional[float]:
    """익절가 옵션에 따른 1H 기준 가격 레벨 반환. '사용하지 않음'이면 None."""
    if option == "사용하지 않음":
        return None
    rates = get_1h_rates(symbol)
    if rates is None or len(rates) < 2:
        return None
    closes = [float(rates["close"][i]) for i in range(len(rates) - 1)]
    opens = [float(rates["open"][i]) for i in range(len(rates) - 1)]
    if option == "20이평":
        return _sma(closes, 20)
    if option == "120이평":
        return _sma(closes, 120)
    if option == "20B상단":
        return _bb_upper(closes, 20, 2)
    if option == "4B상단":
        return _bb_upper(opens, 4, 4)
    return None


def get_sl_price(
    symbol: str, is_buy: bool, entry_price: float, volume: float,
    balance: float, sl_option: str
) -> Optional[float]:
    """손절율 옵션에 따라 손절가 계산."""
    if "사용하지 않음" in sl_option:
        return None
    if "잔액비-10%" in sl_option or "잔액비 -10%" in sl_option:
        max_loss_pct = 10.0
    elif "잔액비-20%" in sl_option or "잔액비 -20%" in sl_option:
        max_loss_pct = 20.0
    elif "50%" in sl_option:
        max_loss_pct = 50.0
    else:
        return None
    max_loss_amount = balance * (max_loss_pct / 100.0)
    info = mt5.symbol_info(symbol)
    contract = getattr(info, "trade_contract_size", 1.0) if info else 1.0
    if contract <= 0:
        contract = 1.0
    price_distance = max_loss_amount / (contract * volume) if (contract * volume) > 0 else 0
    if is_buy:
        return entry_price - price_distance
    return entry_price + price_distance


def symbol_for_db(display_symbol: str) -> str:
    """MT5 티커(NAS100+ 등) → DB 저장용 심볼명(NAS100 등)."""
    s = display_symbol.strip()
    return s.rstrip("+") if s.endswith("+") else s


def _session_by_kst_hour(hour: int) -> str:
    """KST 시각(0~23)에 따라 매매 시점에 해당하는 세션 반환.
    아시아 9~18시, 유럽 18~24시, 미국 0~7시. 7~9시는 아시아로 간주."""
    if 0 <= hour < 7:
        return "US"
    if 7 <= hour < 9:
        return "Asia"
    if 9 <= hour < 18:
        return "Asia"
    if 18 <= hour < 24:
        return "Europe"
    return "Asia"


def resolve_session(symbol: str, session: str, timeframe: str) -> str:
    """세션이 '자동'이면 매매 시점(KST) 기준 가장 가까운 세션을 반환, 아니면 session 그대로.
    아시아 9~18시, 유럽 18~24시, 미국 0~7시."""
    if session and session.strip() != "자동":
        return session.strip()
    now_kst = datetime.now(KST)
    return _session_by_kst_hour(now_kst.hour)


def _previous_session(session: str) -> str:
    """세션 순서 US → Asia → Europe → US 에서 이전 세션 반환."""
    order = ("US", "Asia", "Europe")
    s = (session or "").strip()
    try:
        i = order.index(s)
        return order[(i - 1) % len(order)]
    except ValueError:
        return "Europe"  # 기본 이전 세션


def get_ktr_from_db(symbol: str, session: str, timeframe: str) -> float:
    try:
        from db_config import UNIFIED_DB_PATH
        db_path = UNIFIED_DB_PATH
    except ImportError:
        db_path = os.path.join(_SCRIPT_DIR, "KTR_data.db")
    db = KTRDatabase(db_name=db_path)
    # 자동: 가장 최근에 기록된 (세션 무관) KTR 사용
    if (session or "").strip() == "자동":
        value, _ = db.get_most_recent_ktr(symbol_for_db(symbol), timeframe)
        return float(value) if value else 0.0
    use_session = resolve_session(symbol, session, timeframe)
    return db.get_latest_ktr(symbol_for_db(symbol), use_session, timeframe)


def get_ktr_from_db_auto(symbol: str, timeframe: str) -> Tuple[float, Optional[str]]:
    """자동 모드용: 해당 symbol·timeframe으로 가장 최근 기록된 KTR과 그 세션.
    반환: (ktr_value, session) 예: (12.34, "US"). 없으면 (0.0, None)."""
    try:
        from db_config import UNIFIED_DB_PATH
        db_path = UNIFIED_DB_PATH
    except ImportError:
        db_path = os.path.join(_SCRIPT_DIR, "KTR_data.db")
    db = KTRDatabase(db_name=db_path)
    value, session = db.get_most_recent_ktr(symbol_for_db(symbol), timeframe)
    return (float(value), session) if value and session else (0.0, None)


def get_ktr_from_db_with_fallback(
    symbol: str, session: str, timeframe: str
) -> Tuple[float, str, str, str]:
    """KTR 조회. 해당 세션에 값이 없으면 이전 세션 KTR 사용.
    세션 '자동'이면 해당 symbol·timeframe으로 가장 최근 기록된 데이터 사용.
    10M 요청 시 DB에 10M이 없으면 5M → 1H 순으로 폴백 (KTR 측정은 5M/1H만 저장됨).
    반환: (ktr_value, resolved_session, session_used, tf_used).
    session_used != resolved_session 이면 이전 세션 값, tf_used != timeframe 이면 TF 폴백 사용."""
    try:
        from db_config import UNIFIED_DB_PATH
        db_path = UNIFIED_DB_PATH
    except ImportError:
        db_path = os.path.join(_SCRIPT_DIR, "KTR_data.db")
    db = KTRDatabase(db_name=db_path)
    sym_db = symbol_for_db(symbol)
    if (session or "").strip() == "자동":
        value, session_used = db.get_most_recent_ktr(sym_db, timeframe)
        if value and value > 0 and session_used:
            return (float(value), session_used, session_used, timeframe)
        return (0.0, "자동", "자동", timeframe)
    resolved = resolve_session(symbol, session, timeframe)
    value = db.get_latest_ktr(sym_db, resolved, timeframe)
    if value and value > 0:
        return (float(value), resolved, resolved, timeframe)
    prev = _previous_session(resolved)
    value_prev = db.get_latest_ktr(sym_db, prev, timeframe)
    if value_prev and value_prev > 0:
        return (float(value_prev), resolved, prev, timeframe)
    # 10M은 KTR 측정 스크립트에서 저장하지 않음 → 5M, 1H 순으로 폴백
    if timeframe == "10M":
        for fallback_tf in ("5M", "1H"):
            v = db.get_latest_ktr(sym_db, resolved, fallback_tf)
            if v and v > 0:
                return (float(v), resolved, resolved, fallback_tf)
            v_prev = db.get_latest_ktr(sym_db, prev, fallback_tf)
            if v_prev and v_prev > 0:
                return (float(v_prev), resolved, prev, fallback_tf)
    return (0.0, resolved, resolved, timeframe)


# ---------- 포지션별 TP/SL 옵션 override (코멘트 대체) ----------
SLTP_OVERRIDES_PATH = os.path.join(_SCRIPT_DIR, "ktr_sltp_overrides.json")


def load_sltp_overrides() -> Dict[str, Any]:
    """포지션 티켓별 적용 옵션 로드. { "ticket_id": { tp_option, sl_option, session, timeframe } }"""
    if not os.path.isfile(SLTP_OVERRIDES_PATH):
        return {}
    try:
        with open(SLTP_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_sltp_override(
    ticket: int,
    tp_option: str,
    sl_option: str,
    session: str,
    timeframe: str,
) -> None:
    """해당 포지션의 적용 옵션을 override 파일에 저장 (갱신 프로그램이 우선 사용)."""
    overrides = load_sltp_overrides()
    overrides[str(ticket)] = {
        "tp_option": tp_option,
        "sl_option": sl_option,
        "session": session,
        "timeframe": timeframe,
    }
    with open(SLTP_OVERRIDES_PATH, "w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)
