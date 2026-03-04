# -*- coding: utf-8 -*-
"""
breakout_order_gui.py — 돌파더블비 전용 GUI (다른 진입 조건 없음).

[UI에 있어야 할 것만]
이 파일을 직접 실행했을 때 창에는 다음만 있어야 합니다.
- 제목: "돌파더블비 전용 (Breakout Order)"
- 한 줄 설명: "2/5/10분·1시간봉: 직전 봉이 4/4 시가 BB 상단 돌파 마감 → 다음 봉에서 33% 이상 되돌림 시 매수 진입."
- 심볼: 라디오 (XAUUSD+, NAS100+)
- 타임프레임: 라디오 버튼 (2분, 5분, 10분, 1시간) — 모니터링할 TF 단일 선택
- 비중: 라디오 (1%, 2%)
- KTR 타임프레임: 라디오 (5분봉, 10분봉, 1시간봉)
- KTR: Supabase ktr_records 최신 값 표시 + [새로고침]
- T/P (KTR 배수): 체크박스(사용 여부) + 라디오 (0.5, 1, 2, 3, 없음, 기본 1) + X2 체크
- [모니터 시작] / [모니터 중지] / [프로그램 종료] 버튼
- [실시간 진입]: 선택 타임프레임 직전 봉(방금 마감된 봉)이 돌파더블비인지 확인 후 충족 시 즉시 진입
- [예약 추가]: 현재 설정을 모니터링 예약 목록에 추가
- 예약 목록은 메인 창 안에서 표시, [선택 예약 삭제]로 삭제
- 로그 (스크롤 텍스트)

탭, KTR 설정, 실시간 오더 등 다른 기능은 없습니다.
반드시 아래처럼 단독 실행하세요:  python breakout_order_gui.py

- 2분/5분/10분/1시간봉에서 4/4 시가 볼린저 밴드 상단 돌파 후 마감한 다음,
  다음 봉에서 33% 이상 되돌림 발생 시 비중 1% 또는 2%로 매수 진입만 지원.
- 진입 조건 점검은 봉 마감 1분 뒤(예: 10분봉 15:40 마감 → 15:41에 점검)에 수행하여 직전봉 종가가 MT5/DB에 확정된 뒤 테이블과 동일하게 비교.
"""
import sys
import os
import threading
import time
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timedelta

import pytz

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import MetaTrader5 as mt5

import mt5_trade_utils as tr
from mt5_time_utils import mt5_ts_to_kst
from ktr_sltp_utils import get_ktr_from_db_auto
try:
    from supabase_sync import (
        get_most_recent_ktr_supabase,
        get_breakout_reservations_supabase,
        insert_breakout_reservation_supabase,
        delete_breakout_reservation_supabase,
    )
except ImportError:
    get_most_recent_ktr_supabase = None
    get_breakout_reservations_supabase = None
    insert_breakout_reservation_supabase = None
    delete_breakout_reservation_supabase = None
try:
    from telegram_sender_utils import send_telegram_msg
except ImportError:
    send_telegram_msg = None

KST = pytz.timezone("Asia/Seoul")

# 타임프레임 상수
TIMEFRAME_M2 = getattr(mt5, "TIMEFRAME_M2", 2)
TIMEFRAME_M5 = getattr(mt5, "TIMEFRAME_M5", 5)
TIMEFRAME_M10 = getattr(mt5, "TIMEFRAME_M10", 10)
TIMEFRAME_H1 = getattr(mt5, "TIMEFRAME_H1", 16385)

TF_MAP = {
    "2분": TIMEFRAME_M2,
    "5분": TIMEFRAME_M5,
    "10분": TIMEFRAME_M10,
    "1시간": TIMEFRAME_H1,
}

TF_LABELS = ["2분", "5분", "10분", "1시간"]

# 타임프레임 → KTR DB 조회용 TF (2분은 DB에 없으므로 1H 사용)
TF_TO_KTR_DB = {"2분": "1H", "5분": "5M", "10분": "10M", "1시간": "1H"}
# KTR 표시용 타임프레임 (Supabase ktr_records: 5M, 10M, 1H)
KTR_TF_LABELS = ["5분", "10분", "1시간"]
KTR_TF_TO_DB = {"5분": "5M", "10분": "10M", "1시간": "1H"}

# 이 GUI에서 발생한 시장가 주문 식별용
MAGIC_BREAKOUT_DB = 888001

CONTRACT_BY_SYMBOL = {"XAUUSD": 100.0, "NAS100": 1.0}

# 비중(%) = 잔액의 N%를 마진으로 사용. 1랏당 필요 마진(USD) 조회 실패 시 폴백값.
MARGIN_PER_LOT_FALLBACK = {"XAUUSD": 519.0, "NAS100": 500.0}

# 진입 TF별로 확인할 상위 타임프레임 (정배열: 상위 TF 20이평 > 120이평)
HIGHER_TF_FOR_ALIGNMENT = {"2분": "5분", "5분": "10분", "10분": "1시간", "1시간": None}


def _place_ktr_order_after_breakout_entry(
    symbol: str, side: str, lot: float, entry_price: float, tf_label: str
) -> Tuple[bool, str]:
    """돌파더블비 시장가 진입 후 선택된 KTR로 예약 주문 추가. 매수: 체결가-KTR, 매도: 체결가+KTR.
    반환: (성공 여부, 메시지)."""
    ktr_tf = TF_TO_KTR_DB.get(tf_label, "1H")
    ktr_value, _ = get_ktr_from_db_auto(symbol, ktr_tf)
    if not ktr_value or ktr_value <= 0:
        return False, "KTR 값 없음"
    if side.upper() == "BUY":
        ktr_price = entry_price - ktr_value
    else:
        ktr_price = entry_price + ktr_value
    comment = f"BBdb_KTR_{'sell' if side.upper() == 'SELL' else 'buy'}_{tf_label}"
    ok, msg = tr.place_pending_limit(
        symbol, side, lot, ktr_price, magic=MAGIC_BREAKOUT_DB, comment=comment
    )
    return ok, msg


def _sma(closes: list, period: int) -> Optional[float]:
    if len(closes) < period or period <= 0:
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
    """RSI(period) 시리즈. closes_chron = 과거→현재 순. 반환: 길이 len(closes_chron), 앞 period개는 None, 이후는 RSI 값."""
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


def _is_higher_tf_correct_alignment(symbol: str, entry_tf_label: str) -> Tuple[bool, Optional[str]]:
    """진입 TF보다 큰 타임프레임이 정배열(20이평 > 120이평)인지 확인. 1시간봉 진입 시에는 검사 없음."""
    higher_label = HIGHER_TF_FOR_ALIGNMENT.get(entry_tf_label)
    if higher_label is None:
        return True, None
    mt5_tf = TF_MAP.get(higher_label)
    if mt5_tf is None:
        return True, None
    rates = _get_rates(symbol, mt5_tf, count=125)
    if rates is None or len(rates) < 121:
        return False, f"{higher_label}봉 데이터 부족(120봉 필요)"
    closes = [float(rates["close"][i]) for i in range(len(rates))]
    use_20 = closes[1 : 1 + 20]
    use_120 = closes[1 : 1 + 120]
    if len(use_20) < 20 or len(use_120) < 120:
        return False, f"{higher_label}봉 봉 수 부족"
    sma20 = _sma(use_20, 20)
    sma120 = _sma(use_120, 120)
    if sma20 is None or sma120 is None:
        return False, f"{higher_label}봉 20/120이평 계산 불가"
    if sma20 > sma120:
        return True, None
    return False, f"{higher_label}봉 역배열(20이평 {sma20:.2f} < 120이평 {sma120:.2f}) → 진입 스킵"


def _is_higher_tf_reverse_alignment(symbol: str, entry_tf_label: str) -> Tuple[bool, Optional[str]]:
    """매도 진입 시: 상위 TF가 다음 중 하나면 진입 허용.
    1) 역배열(20이평 < 120이평)
    2) 4이평 < 20이평 역배열이고, RSI(14) 이동평균이 하락 추세
    1시간봉 진입 시에는 검사 없음.
    """
    higher_label = HIGHER_TF_FOR_ALIGNMENT.get(entry_tf_label)
    if higher_label is None:
        return True, None
    mt5_tf = TF_MAP.get(higher_label)
    if mt5_tf is None:
        return True, None
    rates = _get_rates(symbol, mt5_tf, count=125)
    if rates is None or len(rates) < 121:
        return False, f"{higher_label}봉 데이터 부족(120봉 필요)"
    closes = [float(rates["close"][i]) for i in range(len(rates))]
    # 과거→현재 순으로 종가 (MT5: index 0 = 최신 봉)
    closes_chron = [float(rates["close"][i]) for i in range(len(rates) - 1, -1, -1)]
    use_20 = closes[1 : 1 + 20]
    use_120 = closes[1 : 1 + 120]
    if len(use_20) < 20 or len(use_120) < 120:
        return False, f"{higher_label}봉 봉 수 부족"
    sma20 = _sma(use_20, 20)
    sma120 = _sma(use_120, 120)
    if sma20 is None or sma120 is None:
        return False, f"{higher_label}봉 20/120이평 계산 불가"
    # 1) 20 < 120 역배열이면 진입 허용
    if sma20 < sma120:
        return True, None
    # 2) 4이평 < 20이평 역배열 + RSI(14) 이동평균 하락 추세이면 진입 허용
    use_4 = closes[1 : 1 + 4]
    if len(use_4) < 4:
        return False, f"{higher_label}봉 4이평 계산 불가"
    sma4 = _sma(use_4, 4)
    if sma4 is None:
        return False, f"{higher_label}봉 4이평 계산 불가"
    if sma4 >= sma20:
        return False, f"{higher_label}봉 정배열(20≥120)이고 4이평({sma4:.2f})≥20이평({sma20:.2f}) → 매도 진입 스킵"
    # 4 < 20 만족. RSI(14) 이동평균 하락 추세 확인 (최근 3봉 RSI 평균 < 그 이전 3봉 RSI 평균)
    if len(closes_chron) < 14 + 7:  # RSI 14 + 최소 6개 유효 RSI + 1
        return False, f"{higher_label}봉 RSI 계산용 봉 부족"
    rsi_list = _rsi_series(closes_chron, 14)
    if rsi_list is None:
        return False, f"{higher_label}봉 RSI 계산 불가"
    valid_rsi = [v for v in rsi_list if v is not None]
    if len(valid_rsi) < 6:
        return False, f"{higher_label}봉 RSI 값 부족"
    rsi_ma_current = sum(valid_rsi[-3:]) / 3.0
    rsi_ma_prev = sum(valid_rsi[-6:-3]) / 3.0
    if rsi_ma_current < rsi_ma_prev:
        return True, None  # RSI 이동평균 하락 추세 → 진입 허용
    return False, f"{higher_label}봉 4<20 역배열이나 RSI(14)이평 하락 추세 아님(현재 {rsi_ma_current:.1f} ≥ 이전 {rsi_ma_prev:.1f}) → 매도 진입 스킵"


def _bb_upper_series(prices: list, period: int = 4, num_std: float = 4.0) -> Optional[float]:
    """BB 상단. 표본 표준편차(n-1) 사용."""
    if len(prices) < period or period <= 1:
        return None
    use = prices[-period:]
    mid = sum(use) / period
    var = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = var ** 0.5 if var > 0 else 0.0
    return mid + num_std * std


def _bb_lower_series(prices: list, period: int = 4, num_std: float = 4.0) -> Optional[float]:
    """BB 하단. 표본 표준편차(n-1) 사용."""
    if len(prices) < period or period <= 1:
        return None
    use = prices[-period:]
    mid = sum(use) / period
    var = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = var ** 0.5 if var > 0 else 0.0
    return mid - num_std * std


def _format_bar_time_kst(rates: Any, index: int = 1) -> str:
    """rates 테이블에서 index 봉의 Bar Time을 KST 문자열로 반환 (YYYY-MM-DD HH:MM)."""
    if rates is None:
        return ""
    try:
        n_rows = int(len(rates))
        if index < 0 or index >= n_rows:
            return ""
        t = rates["time"][index]
        if hasattr(t, "item"):
            ts = int(t.item())
        else:
            ts = int(t)
        if ts <= 0:
            return ""
        return mt5_ts_to_kst(ts).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OSError, AttributeError):
        return ""


def _bar_duration_minutes(mt5_tf: int) -> int:
    """MT5 타임프레임 상수에 해당하는 봉 길이(분)."""
    if mt5_tf == TIMEFRAME_H1:
        return 60
    if mt5_tf == TIMEFRAME_M10:
        return 10
    if mt5_tf == TIMEFRAME_M5:
        return 5
    if mt5_tf == TIMEFRAME_M2:
        return 2
    return 1


def _format_bar_time_range_kst(rates: Any, index: int, mt5_tf: int, now_kst: Optional[datetime] = None) -> str:
    """rates에서 index 봉의 Bar 구간(시가시각~종가시각)을 KST로 반환. 현재시각 대비 미래봉이면 경고 붙임."""
    if rates is None:
        return ""
    try:
        n_rows = int(len(rates))
        if index < 0 or index >= n_rows:
            return ""
        t = rates["time"][index]
        if hasattr(t, "item"):
            ts = int(t.item())
        else:
            ts = int(t)
        if ts <= 0:
            return ""
        dt_open = mt5_ts_to_kst(ts)
        minutes = _bar_duration_minutes(mt5_tf)
        dt_close = dt_open + timedelta(minutes=minutes)
        out = f"{dt_open.strftime('%Y-%m-%d %H:%M')}~{dt_close.strftime('%H:%M')}"
        if now_kst is not None and dt_open > now_kst:
            out += " ⚠현재시각대비 미래봉"
        return out
    except (TypeError, ValueError, OSError, AttributeError):
        return _format_bar_time_kst(rates, index)


def _get_rates(symbol: str, mt5_tf: int, count: int = 30):
    """지정 타임프레임 봉 조회. M2는 MT5만 사용."""
    if not tr.init_mt5():
        return None
    sym = symbol.strip()
    if not sym.endswith("+"):
        sym = sym + "+"
    if not mt5.symbol_select(sym, True):
        return None
    return mt5.copy_rates_from_pos(sym, mt5_tf, 0, count)


def _is_bar_just_closed(mt5_tf: int, now_kst: Optional[datetime] = None) -> bool:
    """해당 TF 봉이 방금 마감된 시점(새 봉 첫 1분)이면 True."""
    if now_kst is None:
        now_kst = datetime.now(KST)
    m, h = now_kst.minute, now_kst.hour
    if mt5_tf == TIMEFRAME_H1:
        return m == 0
    if mt5_tf == TIMEFRAME_M10:
        return (m % 10) == 0
    if mt5_tf == TIMEFRAME_M5:
        return (m % 5) == 0
    if mt5_tf == TIMEFRAME_M2:
        return (m % 2) == 0
    return True


def _is_bar_closed_at_least_1min_ago(mt5_tf: int, now_kst: Optional[datetime] = None) -> bool:
    """해당 TF 봉이 마감된 지 1분이 지난 시점이면 True. 봉 마감 직후 MT5/DB 미반영으로 직전봉 종가가 달라지는 것을 막기 위해 1분 뒤에 점검."""
    if now_kst is None:
        now_kst = datetime.now(KST)
    m = now_kst.minute
    if mt5_tf == TIMEFRAME_H1:
        return m == 1
    if mt5_tf == TIMEFRAME_M10:
        return (m % 10) == 1
    if mt5_tf == TIMEFRAME_M5:
        return (m % 5) == 1
    if mt5_tf == TIMEFRAME_M2:
        return (m % 2) == 1
    return True


# 돌파더블비: BB 상단 돌파 후 되돌림을 점검할 최대 봉 수 (직전봉만이 아니라 최대 4봉까지)
RETRACE_BARS_AFTER_BREAKOUT = 4


def check_breakout_doublebottom(
    symbol: str,
    mt5_tf: int,
    weight_pct: float,
) -> Tuple[bool, str, Optional[float], Optional[float], Optional[float]]:
    """
    돌파더블비: 직전봉 = 2봉 전(돌파 봉), 그 다음 1봉 전에서 되돌림 점검.
    예: 현재 3:51이면 3:30봉(2봉 전)이 BB 상단 돌파 마감인지 확인, 3:40봉(1봉 전)에서 33% 되돌림 여부 점검.
    MT5 copy_rates_from_pos: rates[0]=과거(오래된), rates[n-1]=현재 → 2봉 전 = rates[n-3].
    반환: (충족여부, 메시지, 직전봉 BB상단값, 되돌림기준가, 진입가용 SL가)
    """
    rates = _get_rates(symbol, mt5_tf, count=15)
    if rates is None or len(rates) < 7:
        return False, "봉 데이터 부족", None, None, None

    n = len(rates)
    idx_2bar = n - 3  # 2봉 전 (MT5는 과거→현재 순)
    # 직전봉 = 2봉 전 기준 Bar 구간 표시
    bar_time_str = _format_bar_time_range_kst(rates, idx_2bar, mt5_tf, now_kst=datetime.now(KST))
    bar_time_suffix = f", Bar(직전봉=2봉전)={bar_time_str}" if bar_time_str else ""
    # 돌파 봉 i: 2봉 전(idx_2bar)부터 과거로. BB용 4개 시가 = open[i-3]~open[i]
    for i in range(idx_2bar, 2, -1):
        if i < 3:
            break
        opens_4 = [float(rates["open"][i + k]) for k in range(-3, 1)]
        bb4_upper = _bb_upper_series(opens_4, 4, 4)
        if bb4_upper is None:
            continue
        close_i = float(rates["close"][i])
        if close_i <= bb4_upper:
            continue
        high_i = float(rates["high"][i])
        low_i = float(rates["low"][i])
        range_i = high_i - low_i
        if range_i <= 0:
            continue
        retrace_level = high_i - range_i * 0.33
        start_j = max(1, i - RETRACE_BARS_AFTER_BREAKOUT)
        for j in range(start_j, i):
            low_j = float(rates["low"][j])
            if low_j <= retrace_level:
                bars_after = i - j
                bars_ago = (n - 1) - i
                return (
                    True,
                    f"{bars_ago}봉 전 BB상단 돌파 마감 + 그 다음 {bars_after}봉째에서 33% 되돌림 충족 (BB상단={bb4_upper:.2f}, 되돌림선={retrace_level:.2f}{bar_time_suffix})",
                    bb4_upper,
                    retrace_level,
                    low_i,
                )
    # 직전봉(2봉 전) 돌파만 있고 되돌림 미충족인 경우
    if n >= 7 and idx_2bar >= 3:
        opens_4_2 = [float(rates["open"][idx_2bar + k]) for k in range(-3, 1)]
        bb4 = _bb_upper_series(opens_4_2, 4, 4)
        close_2 = float(rates["close"][idx_2bar])
        if bb4 is not None and close_2 > bb4:
            high_2 = float(rates["high"][idx_2bar])
            low_2 = float(rates["low"][idx_2bar])
            r2 = high_2 - low_2
            if r2 > 0:
                rl = high_2 - r2 * 0.33
                return (
                    False,
                    f"직전봉(2봉전) 종가={close_2:.2f}, 4B상단={bb4:.2f}, 되돌림선={rl:.2f}{bar_time_suffix} - 진입조건 만족 X",
                    bb4,
                    rl,
                    low_2,
                )
    # 직전봉(2봉 전) 종가·4B·Bar Time 로그
    close_prev = float(rates["close"][idx_2bar])
    if idx_2bar >= 3:
        opens_4_prev = [float(rates["open"][idx_2bar + k]) for k in range(-3, 1)]
        bb4_prev = _bb_upper_series(opens_4_prev, 4, 4)
        if bb4_prev is not None:
            msg = f"직전봉(2봉전) 종가={close_prev:.2f}, 4B상단={bb4_prev:.2f}{bar_time_suffix} - 진입조건 만족 X"
        else:
            msg = f"직전봉(2봉전) 종가={close_prev:.2f}{bar_time_suffix} - 진입조건 만족 X"
    else:
        msg = f"직전봉(2봉전) 종가={close_prev:.2f}{bar_time_suffix} - 진입조건 만족 X"
    return False, msg, None, None, None


def check_breakout_doublebottom_within_bars(
    symbol: str,
    mt5_tf: int,
    weight_pct: float,
    within_bars: int = 5,
) -> Tuple[bool, str, Optional[float], Optional[float], Optional[float]]:
    """
    실시간 진입용: 직전봉(2봉 전) 기준 돌파더블비 먼저 확인 후,
    이전 within_bars개 봉 안에서 '돌파 봉(i) + 그 다음 마감봉(j>=1)에서 33% 되돌림' 탐색.
    반환: (충족여부, 메시지, BB상단값, 되돌림기준가, SL가).
    """
    rates = _get_rates(symbol, mt5_tf, count=15)
    if rates is None or len(rates) < 7:
        return False, "봉 데이터 부족", None, None, None

    # 1) 직전봉(2봉 전) 기준 돌파더블비 먼저 확인
    matched, msg, bb_upper, retrace_level, sl_price = check_breakout_doublebottom(symbol, mt5_tf, weight_pct)
    if matched:
        return True, msg, bb_upper, retrace_level, sl_price

    # 2) 이전 봉들 안에서 돌파(i) + 그 다음 마감봉(j>=1)에서 되돌림 탐색 (MT5: rates[0]=과거, rates[n-1]=현재)
    n = len(rates)
    idx_2bar = n - 3
    for i in range(idx_2bar, max(2, idx_2bar - within_bars), -1):
        if i < 3:
            break
        opens_4 = [float(rates["open"][i + k]) for k in range(-3, 1)]
        bb4 = _bb_upper_series(opens_4, 4, 4)
        if bb4 is None:
            continue
        close_i = float(rates["close"][i])
        if close_i <= bb4:
            continue
        high_i = float(rates["high"][i])
        low_i = float(rates["low"][i])
        range_i = high_i - low_i
        if range_i <= 0:
            continue
        retrace_level_i = high_i - range_i * 0.33
        start_j = max(1, i - RETRACE_BARS_AFTER_BREAKOUT)
        for j in range(start_j, i):
            low_j = float(rates["low"][j])
            if low_j <= retrace_level_i:
                bars_after = i - j
                bars_ago = (n - 1) - i
                return (
                    True,
                    f"이전 {bars_ago}봉 전 BB돌파 + 그 다음 {bars_after}봉째에서 33% 되돌림 충족 (BB상단={bb4:.2f}, 되돌림선={retrace_level_i:.2f})",
                    bb4,
                    retrace_level_i,
                    low_i,
                )

    return False, "이전 봉 내 돌파+되돌림 없음", None, None, None


def check_breakout_sell(
    symbol: str,
    mt5_tf: int,
    weight_pct: float,
) -> Tuple[bool, str, Optional[float], Optional[float], Optional[float]]:
    """
    매도 조건: 직전 봉이 4/4 시가 BB 하단 돌파 마감 + 현재 봉에서 33% 이상 상승 되돌림.
    볼린저 밴드는 예약/선택된 타임프레임(mt5_tf)의 봉 데이터로만 계산 (5분 예약이면 5분봉 BB 사용).
    반환: (충족여부, 메시지, 직전봉 BB하단값, 되돌림기준가, 진입가용 SL가=직전봉 고가)
    """
    # 예약된(또는 선택된) 타임프레임의 봉으로 BB 계산 (MT5: rates[0]=과거, rates[n-1]=현재)
    rates = _get_rates(symbol, mt5_tf, count=10)
    if rates is None or len(rates) < 6:
        return False, "봉 데이터 부족", None, None, None

    n = len(rates)
    idx_current = n - 1
    idx_prev = n - 2

    close_prev = float(rates["close"][idx_prev])
    high_prev = float(rates["high"][idx_prev])
    low_prev = float(rates["low"][idx_prev])
    high_current = float(rates["high"][idx_current])

    if idx_prev < 3:
        return False, "시가 4봉 부족", None, None, None
    opens_4 = [float(rates["open"][idx_prev + k]) for k in range(-3, 1)]

    bb4_lower = _bb_lower_series(opens_4, 4, 4)
    if bb4_lower is None:
        return False, "BB(4,4) 하단 계산 불가", None, None, None

    # 1) 직전 봉이 시가 BB 하단 돌파 마감 (종가가 하단 아래)
    if close_prev >= bb4_lower:
        return (
            False,
            f"종가({close_prev:.2f}) >= 4B하단({bb4_lower:.2f}) → 돌파 X",
            bb4_lower,
            None,
            None,
        )

    # 2) 현재 봉에서 33% 상승 되돌림: low_prev + range*0.33 이상 터치
    range_prev = high_prev - low_prev
    if range_prev <= 0:
        return False, "직전 봉 범위 0", bb4_lower, None, None

    retrace_level = low_prev + range_prev * 0.33
    if high_current < retrace_level:
        return (
            False,
            f"현재 봉 고가({high_current:.2f}) < 33%되돌림선({retrace_level:.2f}) → 되돌림 미충족",
            bb4_lower,
            retrace_level,
            high_prev,
        )

    # 충족: 매도 진입. SL = 직전 봉 고가
    return (
        True,
        f"직전 봉 4B하단 돌파 마감 + 현재 봉 33% 상승 되돌림 충족 (BB하단={bb4_lower:.2f}, 되돌림선={retrace_level:.2f})",
        bb4_lower,
        retrace_level,
        high_prev,
    )


def check_breakout_sell_within_bars(
    symbol: str,
    mt5_tf: int,
    weight_pct: float,
    within_bars: int = 5,
) -> Tuple[bool, str, Optional[float], Optional[float], Optional[float]]:
    """실시간 매도용: 이전 within_bars개 봉 안에 4B 하단 돌파 마감 + 다음 봉 33% 상승 되돌림이 있으면 충족."""
    rates = _get_rates(symbol, mt5_tf, count=10)
    if rates is None or len(rates) < 6:
        return False, "봉 데이터 부족", None, None, None

    matched, msg, bb_lower, retrace_level, sl_price = check_breakout_sell(symbol, mt5_tf, weight_pct)
    if matched:
        return True, msg, bb_lower, retrace_level, sl_price

    n = len(rates)
    # MT5: rates[0]=과거, rates[n-1]=현재. 1봉 전 = n-2, 2봉 전 = n-3, ...
    for i in range(n - 2, max(2, n - 2 - within_bars), -1):
        if i < 3 or i + 1 >= n:
            continue
        opens_4 = [float(rates["open"][i + k]) for k in range(-3, 1)]
        bb4 = _bb_lower_series(opens_4, 4, 4)
        if bb4 is None:
            continue
        close_i = float(rates["close"][i])
        if close_i >= bb4:
            continue
        high_i = float(rates["high"][i])
        low_i = float(rates["low"][i])
        range_i = high_i - low_i
        if range_i <= 0:
            continue
        retrace_level_i = low_i + range_i * 0.33
        high_next = float(rates["high"][i + 1])  # 다음 봉(더 최신)
        if high_next < retrace_level_i:
            continue
        sl_price_i = high_i
        bars_ago = (n - 1) - i
        return (
            True,
            f"이전 {bars_ago}봉 전 4B하단 돌파 + 그 다음 봉 33% 상승 되돌림 충족 (BB하단={bb4:.2f}, 되돌림선={retrace_level_i:.2f})",
            bb4,
            retrace_level_i,
            sl_price_i,
        )

    return False, "이전 5봉 내 4B하단 돌파+되돌림 없음", None, None, None


def calc_lot_by_margin_weight(
    balance: float,
    weight_pct: float,
    symbol: str,
    entry_price: float,
    side: str = "BUY",
) -> float:
    """
    비중% = 잔액의 N%를 마진으로 사용하여 랏수 계산.
    side: "BUY" | "SELL"
    """
    if balance <= 0 or weight_pct <= 0 or entry_price <= 0:
        return 0.0
    margin_to_use = balance * (weight_pct / 100.0)
    order_type = mt5.ORDER_TYPE_SELL if (side or "BUY").upper() == "SELL" else mt5.ORDER_TYPE_BUY
    margin_1lot: Optional[float] = None
    if tr.init_mt5():
        margin_1lot = mt5.order_calc_margin(order_type, symbol, 1.0, entry_price)
    if margin_1lot is None or margin_1lot <= 0:
        sym_base = (symbol or "").strip().upper().rstrip("+")
        margin_1lot = MARGIN_PER_LOT_FALLBACK.get(sym_base, 519.0)
    lot = margin_to_use / float(margin_1lot)
    return round(min(max(lot, 0.01), 100.0), 2)


class BreakoutDoubleBottomApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("돌파더블비 전용 (Breakout Order)")
        self.root.minsize(650, 646)
        self.root.geometry("650x646")
        self.monitor_running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self.reservations: List[Dict[str, Any]] = []
        self._reservations_lock = threading.Lock()

        self._build_ui()
        self._load_breakout_reservations()

        # 창이 뜨면 자동으로 모니터 시작
        self.root.after(100, self._on_start_monitor)

    def _log(self, msg: str):
        def _append():
            self.log_text.insert(tk.END, msg + "\n")
            self.log_text.see(tk.END)

        try:
            self.root.after(0, _append)
        except Exception:
            pass

    def _build_ui(self):
        # 돌파더블비 전용: 아래 6가지만 표시 (탭/예약/KTR/실시간오더 등 없음)
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill=tk.BOTH, expand=True)

        row = 0

        # 0) 매수 / 매도 라디오 (맨 위)
        ttk.Label(main, text="진입:").grid(row=row, column=0, sticky=tk.W, padx=(0, 4))
        buy_sell_f = ttk.Frame(main)
        buy_sell_f.grid(row=row, column=1, sticky=tk.W, pady=4)
        self.var_buysell = tk.StringVar(value="매수")
        ttk.Radiobutton(buy_sell_f, text="매수", variable=self.var_buysell, value="매수").pack(side=tk.LEFT, padx=(0, 12))
        ttk.Radiobutton(buy_sell_f, text="매도", variable=self.var_buysell, value="매도").pack(side=tk.LEFT, padx=(0, 12))
        row += 1

        # 1) 설명 한 줄 (모드에 따라 문구 변경은 런타임에서 처리 가능, 여기서는 공통 설명)
        desc = (
            "매수: 직전 봉 4B 상단 돌파 마감 → 33% 되돌림 시 진입. "
            "매도: 직전 봉 4B 하단 돌파 마감 → 33% 상승 되돌림 시 진입(상위 TF 역배열 확인)."
        )
        ttk.Label(main, text=desc, wraplength=480).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=(0, 8))
        row += 1

        # 2) 심볼
        ttk.Label(main, text="심볼:").grid(row=row, column=0, sticky=tk.W, padx=(0, 4))
        sym_radio_f = ttk.Frame(main)
        sym_radio_f.grid(row=row, column=1, sticky=tk.W, pady=4)
        self.var_symbol = tk.StringVar(value="XAUUSD+")
        for val in ("XAUUSD+", "NAS100+"):
            ttk.Radiobutton(sym_radio_f, text=val, variable=self.var_symbol, value=val, command=self._refresh_ktr_label).pack(side=tk.LEFT, padx=(0, 12))
        row += 1

        # 3) 타임프레임 (단일 선택 라디오)
        ttk.Label(main, text="타임프레임:").grid(row=row, column=0, sticky=tk.W, padx=(0, 4))
        tf_radio_f = ttk.Frame(main)
        tf_radio_f.grid(row=row, column=1, sticky=tk.W, pady=4)
        self.var_tf = tk.StringVar(value="10분")
        for lbl in TF_LABELS:
            ttk.Radiobutton(tf_radio_f, text=lbl, variable=self.var_tf, value=lbl, command=self._refresh_ktr_label).pack(side=tk.LEFT, padx=(0, 10))
        row += 1

        # 4) 비중 (1% / 2%) + 예상 랏수 + 현재 잔액
        ttk.Label(main, text="비중:").grid(row=row, column=0, sticky=tk.W, padx=(0, 4))
        self.var_weight = tk.StringVar(value="1%")
        weight_frame = ttk.Frame(main)
        weight_frame.grid(row=row, column=1, sticky=tk.W, pady=4)
        ttk.Radiobutton(weight_frame, text="1%", variable=self.var_weight, value="1%").pack(side=tk.LEFT, padx=(0, 4))
        self.label_lot_1 = ttk.Label(weight_frame, text="—랏")
        self.label_lot_1.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Radiobutton(weight_frame, text="2%", variable=self.var_weight, value="2%").pack(side=tk.LEFT, padx=(0, 4))
        self.label_lot_2 = ttk.Label(weight_frame, text="—랏")
        self.label_lot_2.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(weight_frame, text="잔액:").pack(side=tk.LEFT, padx=(0, 4))
        self.label_balance = ttk.Label(weight_frame, text="—")
        self.label_balance.pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(weight_frame, text="새로고침", width=8, command=self._refresh_balance_and_lots).pack(side=tk.LEFT)
        row += 1

        # KTR 타임프레임 (5분/10분/1시간) + KTR 값 (Supabase ktr_records 최신)
        ttk.Label(main, text="KTR 타임프레임:").grid(row=row, column=0, sticky=tk.W, padx=(0, 4))
        ktr_tf_f = ttk.Frame(main)
        ktr_tf_f.grid(row=row, column=1, sticky=tk.W, pady=4)
        self.var_ktr_tf = tk.StringVar(value="10분")
        for lbl in KTR_TF_LABELS:
            ttk.Radiobutton(ktr_tf_f, text=lbl, variable=self.var_ktr_tf, value=lbl, command=self._refresh_ktr_label).pack(side=tk.LEFT, padx=(0, 10))
        row += 1
        ttk.Label(main, text="KTR:").grid(row=row, column=0, sticky=tk.W, padx=(0, 4))
        ktr_f = ttk.Frame(main)
        ktr_f.grid(row=row, column=1, sticky=tk.W, pady=4)
        self.label_ktr = ttk.Label(ktr_f, text="—")
        self.label_ktr.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(ktr_f, text="새로고침", width=8, command=self._refresh_ktr_label).pack(side=tk.LEFT)
        row += 1

        # T/P 체크박스 + KTR 배수 라디오 (0.5, 1, 2, 3, 없음, 기본 1) + X2 체크 (한 줄)
        self.var_tp_enabled = tk.BooleanVar(value=False)
        tp_row_f = ttk.Frame(main)
        tp_row_f.grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=4)
        tp_cb = ttk.Checkbutton(tp_row_f, variable=self.var_tp_enabled)
        tp_cb.pack(side=tk.LEFT, padx=(0, 6))
        ttk.Label(tp_row_f, text="T/P (KTR 배수):").pack(side=tk.LEFT, padx=(0, 8))
        self.var_tp_ktr = tk.StringVar(value="1")
        for val in ("0.5", "1", "2", "3", "없음"):
            ttk.Radiobutton(tp_row_f, text=val, variable=self.var_tp_ktr, value=val).pack(side=tk.LEFT, padx=(0, 6))
        self.var_tp_x2 = tk.BooleanVar(value=False)
        ttk.Checkbutton(tp_row_f, text="X2", variable=self.var_tp_x2).pack(side=tk.LEFT, padx=(12, 0))
        row += 1

        # 실시간 진입 / 예약 추가 / 선택 예약 삭제 버튼
        reserve_btn_f = ttk.Frame(main)
        reserve_btn_f.grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=4)
        ttk.Button(reserve_btn_f, text="실시간 진입", command=self._on_realtime_entry).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(reserve_btn_f, text="예약 추가", command=self._on_add_reservation).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(reserve_btn_f, text="선택 예약 삭제", command=self._on_delete_selected_reservation).pack(side=tk.LEFT)
        row += 1

        # 예약 목록 (메인 GUI 내) — 세로 스크롤바 + 마우스 휠 스크롤
        list_frame = ttk.LabelFrame(main, text="예약 목록 (모니터링 대상)", padding=4)
        list_frame.grid(row=row, column=0, columnspan=3, sticky=tk.NSEW, pady=4)
        main.rowconfigure(row, weight=0, minsize=140)
        row += 1
        cols = ("심볼", "매수/매도", "타임프레임", "비중", "T/P")
        self.tree_reservations = ttk.Treeview(list_frame, columns=cols, show="headings", height=5, selectmode="browse")
        for c in cols:
            self.tree_reservations.heading(c, text=c)
            w = 60 if c == "매수/매도" else (160 if c == "타임프레임" else 90)
            self.tree_reservations.column(c, width=w)
        scroll_res = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree_reservations.yview)
        self.tree_reservations.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_res.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree_reservations.configure(yscrollcommand=scroll_res.set)
        # 마우스 휠로 예약 목록 위아래 스크롤
        def _on_mousewheel(event):
            self.tree_reservations.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self.tree_reservations.bind("<MouseWheel>", _on_mousewheel)
        list_frame.bind("<MouseWheel>", _on_mousewheel)

        # 5) 모니터 시작/중지 + 프로그램 종료 버튼
        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=8)
        self.btn_start = ttk.Button(btn_frame, text="모니터 시작", command=self._on_start_monitor)
        self.btn_start.pack(side=tk.LEFT, padx=(0, 8))
        self.btn_stop = ttk.Button(btn_frame, text="모니터 중지", command=self._on_stop_monitor, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btn_frame, text="프로그램 종료", command=self._on_quit).pack(side=tk.LEFT)
        row += 1

        # 6) 로그
        log_frame = ttk.LabelFrame(main, text="로그", padding=4)
        log_frame.grid(row=row, column=0, columnspan=3, sticky=tk.NSEW, pady=8)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(row, weight=1)
        main.rowconfigure(row, weight=1)
        main.columnconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=12, width=70, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)

        self._refresh_ktr_label()
        self._refresh_balance_label()
        self._refresh_lot_labels()
        self._refresh_reservation_tree()
        self._log("돌파더블비 전용. 타임프레임·비중 선택 후 모니터가 자동으로 시작됩니다.")

    def _load_breakout_reservations(self):
        """Supabase breakout_reservations 테이블에서 예약 목록 로드. 비활성/실패 시 무시."""
        if not get_breakout_reservations_supabase:
            return
        data = get_breakout_reservations_supabase()
        if not data:
            return
        with self._reservations_lock:
            self.reservations.clear()
            for row in data:
                tfs_str = (row.get("tfs") or "").strip()
                tfs_list = [s.strip() for s in tfs_str.split(",") if s.strip()]
                self.reservations.append({
                    "id": row.get("id"),
                    "symbol": (row.get("symbol") or "").strip() or "XAUUSD+",
                    "tfs": tfs_list if tfs_list else ["10분"],
                    "weight_pct": float(row.get("weight_pct", 1.0)),
                    "tp_enabled": bool(row.get("tp_enabled", False)),
                    "tp_ktr": (row.get("tp_ktr") or "1").strip(),
                    "tp_x2": bool(row.get("tp_x2", False)),
                    "side": (row.get("side") or "매수").strip() or "매수",
                })
        self._refresh_reservation_tree()
        if self.reservations:
            self._log(f"Supabase에서 예약 {len(self.reservations)}건 로드했습니다.")

    def _send_telegram(self, text: str) -> None:
        """텔레그램 메시지 전송 (전송 실패 시 무시)."""
        if send_telegram_msg and text:
            try:
                send_telegram_msg(text, parse_mode="")
            except Exception:
                pass

    def _get_selected_tfs(self):
        """선택된 타임프레임을 (tf_label, mt5_tf) 리스트로 반환 (라디오이므로 1개)."""
        lbl = (self.var_tf.get() or "").strip()
        if lbl and lbl in TF_MAP:
            return [(lbl, TF_MAP[lbl])]
        return []

    def _on_realtime_entry(self):
        """실시간 진입: 선택된 타임프레임에서 직전 봉(방금 마감된 봉)이 돌파더블비였는지 확인하고, 충족 시 즉시 진입 (별도 스레드)."""
        selected_tfs = self._get_selected_tfs()
        if not selected_tfs:
            self._log("실시간 진입: 타임프레임을 선택하세요.")
            return
        symbol = (self.var_symbol.get() or "XAUUSD+").strip()
        if not symbol.endswith("+"):
            symbol = symbol + "+"
        weight_s = (self.var_weight.get() or "1%").strip().replace("%", "")
        try:
            weight_pct = float(weight_s)
        except ValueError:
            weight_pct = 1.0
        tp_enabled = self.var_tp_enabled.get()
        tp_ktr_str = (self.var_tp_ktr.get() or "1").strip()
        tp_x2 = self.var_tp_x2.get()

        def _run():
            now_kst = datetime.now(KST)
            tf_label, mt5_tf = selected_tfs[0]
            is_sell = (self.var_buysell.get() or "매수").strip() == "매도"
            if not _is_bar_closed_at_least_1min_ago(mt5_tf, now_kst):
                self._log("실시간 진입: 해당 타임프레임 봉이 마감된 지 1분이 지난 후에만 진입할 수 있습니다.")
                return
            if is_sell:
                matched, msg, bb_lower, retrace_level, sl_price = check_breakout_sell_within_bars(
                    symbol, mt5_tf, weight_pct, within_bars=5
                )
            else:
                matched, msg, bb_upper, retrace_level, sl_price = check_breakout_doublebottom_within_bars(
                    symbol, mt5_tf, weight_pct, within_bars=5
                )
            self._log(f"[실시간] [{tf_label}] {msg}")
            if not matched or sl_price is None:
                return
            if is_sell:
                ok_align, align_reason = _is_higher_tf_reverse_alignment(symbol, tf_label)
            else:
                ok_align, align_reason = _is_higher_tf_correct_alignment(symbol, tf_label)
            if not ok_align:
                self._log(f"실시간 진입 스킵: {align_reason}")
                return
            if not tr.init_mt5():
                self._log("실시간 진입: MT5 연결 실패")
                return
            ask, bid = tr.get_market_price(symbol)
            entry_price = (bid if is_sell else ask)
            if entry_price is None or entry_price <= 0:
                self._log("실시간 진입: 호가 조회 실패")
                return
            acc = tr.get_account_info()
            balance = float(acc.get("balance", 0) or 0)
            if balance <= 0:
                self._log("실시간 진입: 잔액 없음")
                return
            side = "SELL" if is_sell else "BUY"
            lot = calc_lot_by_margin_weight(balance, weight_pct, symbol, entry_price, side=side)
            if lot < 0.01:
                self._log(f"실시간 진입: 랏수 너무 작음 ({lot})")
                return
            comment = f"BBdb_{'sell' if is_sell else 'buy'}_{tf_label}_{weight_pct}%"
            ok, result = tr.execute_market_order(
                symbol, side, lot, magic=MAGIC_BREAKOUT_DB, comment=comment
            )
            if ok:
                self._log(f"실시간 진입 성공: {symbol} {side} {lot}랏 (비중 {weight_pct}%)")
                tp_price = 0.0
                try:
                    if tp_enabled and tp_ktr_str and tp_ktr_str != "없음" and tp_ktr_str in ("0.5", "1", "2", "3"):
                        ktr_tf = TF_TO_KTR_DB.get(tf_label, "1H")
                        ktr_value, _ = get_ktr_from_db_auto(symbol, ktr_tf)
                        if ktr_value and ktr_value > 0:
                            tp_mult = float(tp_ktr_str)
                            ktr_eff = ktr_value * 2.0 if tp_x2 else ktr_value
                            if is_sell:
                                tp_price = entry_price - ktr_eff * tp_mult  # 매도: 진입가 - KTR
                            else:
                                tp_price = entry_price + ktr_eff * tp_mult
                            x2_note = " (X2)" if tp_x2 else ""
                            self._log(f"T/P: 진입가 {'- KTR' if is_sell else '+ KTR'}×{tp_ktr_str}{x2_note} = {tp_price:.2f}")
                        else:
                            self._log("T/P: KTR 값 없음 → T/P 미설정")
                except Exception as e:
                    self._log(f"T/P 계산 실패(무시): {e}")
                try:
                    positions = mt5.positions_get(symbol=symbol)
                    if positions:
                        last_pos = max(positions, key=lambda p: p.time)
                        tr.modify_position_sltp(last_pos.ticket, symbol, sl_price, tp_price if tp_price > 0 else 0.0)
                        self._log(f"손절: {sl_price:.2f}" + (f", T/P: {tp_price:.2f}" if tp_price > 0 else ""))
                except Exception as e:
                    self._log(f"손절/T/P 설정 실패(무시): {e}")
                # KTR 오더 추가: 매수=체결가-KTR, 매도=체결가+KTR 예약
                try:
                    ktr_ok, ktr_msg = _place_ktr_order_after_breakout_entry(
                        symbol, side, lot, entry_price, tf_label
                    )
                    if ktr_ok:
                        self._log(f"KTR 오더: {ktr_msg}")
                    else:
                        self._log(f"KTR 오더 미등록: {ktr_msg}")
                except Exception as e:
                    self._log(f"KTR 오더 실패(무시): {e}")
                msg_telegram = f"🟢 돌파더블비 실시간 진입: {symbol} {tf_label} {side} {lot}랏 (비중 {weight_pct}%) S/L {sl_price:.2f}"
                if tp_price > 0:
                    msg_telegram += f" T/P {tp_price:.2f}"
                self._send_telegram(msg_telegram)
            else:
                self._log(f"실시간 진입 실패: {result}")
                self._send_telegram(f"🔴 돌파더블비 실시간 진입 실패: {symbol} {tf_label} — {result}")

        threading.Thread(target=_run, daemon=True).start()

    def _on_add_reservation(self):
        """현재 폼 설정을 예약 목록에 추가 (Supabase 저장)."""
        selected_tfs = self._get_selected_tfs()
        if not selected_tfs:
            messagebox.showwarning("예약 추가", "모니터링할 타임프레임을 선택하세요.")
            return
        symbol = (self.var_symbol.get() or "XAUUSD+").strip()
        if not symbol.endswith("+"):
            symbol = symbol + "+"
        weight_s = (self.var_weight.get() or "1%").strip().replace("%", "")
        try:
            weight_pct = float(weight_s)
        except ValueError:
            weight_pct = 1.0
        tp_enabled = self.var_tp_enabled.get()
        tp_ktr = (self.var_tp_ktr.get() or "1").strip()
        tp_x2 = self.var_tp_x2.get()
        tfs_list = [lbl for lbl, _ in selected_tfs]
        tfs_str = ",".join(tfs_list)
        side = (self.var_buysell.get() or "매수").strip() or "매수"
        item = {
            "symbol": symbol,
            "tfs": tfs_list,
            "weight_pct": weight_pct,
            "tp_enabled": tp_enabled,
            "tp_ktr": tp_ktr,
            "tp_x2": tp_x2,
            "side": side,
        }
        if insert_breakout_reservation_supabase:
            rid = insert_breakout_reservation_supabase(
                symbol, tfs_str, weight_pct, tp_enabled, tp_ktr, tp_x2, side=side
            )
            if rid is not None:
                item["id"] = rid
            else:
                self._log("예약 추가: Supabase 저장 실패(로컬에만 반영)")
        with self._reservations_lock:
            self.reservations.append(item)
        tf_str = ", ".join(item["tfs"])
        self._log(f"예약 추가: {symbol} | TF: {tf_str} | 비중 {weight_pct}% | T/P: {'KTR×' + tp_ktr if tp_enabled and tp_ktr != '없음' else '없음'}")
        self._refresh_reservation_tree()

    def _refresh_reservation_tree(self):
        """메인 GUI의 예약 트리를 현재 reservations 로 갱신."""
        if not getattr(self, "tree_reservations", None):
            return
        self.tree_reservations.delete(*self.tree_reservations.get_children())
        with self._reservations_lock:
            for i, r in enumerate(self.reservations):
                sym = (r.get("symbol") or "").strip()
                side = (r.get("side") or "매수").strip() or "매수"
                tfs = r.get("tfs") or []
                tf_str = ", ".join(tfs)
                weight = r.get("weight_pct", 1.0)
                tp_en = r.get("tp_enabled", False)
                tp_ktr = (r.get("tp_ktr") or "1").strip()
                tp_x2 = r.get("tp_x2", False)
                if tp_en and tp_ktr != "없음":
                    tp_str = f"KTR×{tp_ktr}" + (" X2" if tp_x2 else "")
                else:
                    tp_str = "없음"
                self.tree_reservations.insert("", tk.END, iid=str(i), values=(sym, side, tf_str, f"{weight}%", tp_str))

    def _on_delete_selected_reservation(self):
        """트리에서 선택된 예약 항목을 삭제 (Supabase에서도 삭제)."""
        if not getattr(self, "tree_reservations", None):
            return
        sel = self.tree_reservations.selection()
        if not sel:
            messagebox.showinfo("삭제", "삭제할 예약을 목록에서 선택하세요.")
            return
        indices = []
        for iid in sel:
            try:
                indices.append(int(iid))
            except ValueError:
                pass
        if not indices:
            return
        indices_set = set(indices)
        with self._reservations_lock:
            to_remove = [(i, self.reservations[i]) for i in indices_set if 0 <= i < len(self.reservations)]
        for i, res in to_remove:
            if res.get("id") is not None and delete_breakout_reservation_supabase:
                delete_breakout_reservation_supabase(int(res["id"]))
        with self._reservations_lock:
            new_list = [r for i, r in enumerate(self.reservations) if i not in indices_set]
            self.reservations.clear()
            self.reservations.extend(new_list)
        self._refresh_reservation_tree()
        self._log("선택한 예약을 삭제했습니다.")

    def _refresh_ktr_label(self):
        """선택된 심볼·KTR 타임프레임에 맞는 최신 KTR을 Supabase ktr_records에서 조회해 라벨에 표시."""
        try:
            sym = (self.var_symbol.get() or "XAUUSD+").strip().rstrip("+")
            if not sym:
                sym = "XAUUSD"
            ktr_tf_label = (self.var_ktr_tf.get() or "10분").strip()
            ktr_tf = KTR_TF_TO_DB.get(ktr_tf_label, "10M")
            if get_most_recent_ktr_supabase:
                ktr_value, session = get_most_recent_ktr_supabase(sym, ktr_tf)
                if ktr_value and ktr_value > 0 and session:
                    self.label_ktr.config(text=f"{ktr_value:.2f} ({session} {ktr_tf})")
                else:
                    ktr_value, session = get_ktr_from_db_auto(sym + "+", ktr_tf)
                    if ktr_value and ktr_value > 0 and session:
                        self.label_ktr.config(text=f"{ktr_value:.2f} ({session} {ktr_tf}) [로컬]")
                    else:
                        self.label_ktr.config(text="— (Supabase·로컬 없음)")
            else:
                ktr_value, session = get_ktr_from_db_auto(sym + "+", ktr_tf)
                if ktr_value and ktr_value > 0 and session:
                    self.label_ktr.config(text=f"{ktr_value:.2f} ({session} {ktr_tf})")
                else:
                    self.label_ktr.config(text="— (로컬 없음)")
        except Exception as e:
            self.label_ktr.config(text=f"— ({e})")
        self._refresh_lot_labels()

    def _refresh_balance_label(self):
        """MT5 계정 잔액을 조회해 라벨에 표시."""
        try:
            if not getattr(self, "label_balance", None):
                return
            if not tr.init_mt5():
                self.label_balance.config(text="— (MT5 미연결)")
                return
            acc = tr.get_account_info()
            balance = float(acc.get("balance", 0) or 0)
            self.label_balance.config(text=f"{balance:,.2f}")
        except Exception as e:
            if getattr(self, "label_balance", None):
                self.label_balance.config(text=f"— ({e})")

    def _refresh_balance_and_lots(self):
        """잔액과 예상 랏수 라벨을 함께 갱신."""
        self._refresh_balance_label()
        self._refresh_lot_labels()

    def _refresh_lot_labels(self):
        """선택 심볼·잔액·호가 기준으로 비중 1%·2% 시 마진 사용 랏수를 계산해 라벨에 표시."""
        try:
            if not getattr(self, "label_lot_1", None) or not getattr(self, "label_lot_2", None):
                return
            symbol = (self.var_symbol.get() or "XAUUSD+").strip()
            if not symbol.endswith("+"):
                symbol = symbol + "+"
            if not tr.init_mt5():
                self.label_lot_1.config(text="—랏")
                self.label_lot_2.config(text="—랏")
                return
            acc = tr.get_account_info()
            balance = float(acc.get("balance", 0) or 0)
            ask, _ = tr.get_market_price(symbol)
            if balance <= 0 or ask is None or ask <= 0:
                self.label_lot_1.config(text="—랏")
                self.label_lot_2.config(text="—랏")
                return
            lot_1 = calc_lot_by_margin_weight(balance, 1.0, symbol, ask)
            lot_2 = calc_lot_by_margin_weight(balance, 2.0, symbol, ask)
            self.label_lot_1.config(text=f"→ {lot_1:.2f}랏")
            self.label_lot_2.config(text=f"→ {lot_2:.2f}랏")
        except Exception:
            if getattr(self, "label_lot_1", None):
                self.label_lot_1.config(text="—랏")
            if getattr(self, "label_lot_2", None):
                self.label_lot_2.config(text="—랏")

    def _on_start_monitor(self):
        if self.monitor_running:
            return
        with self._reservations_lock:
            has_reservations = len(self.reservations) > 0
        if not has_reservations and not self._get_selected_tfs():
            self._log("예약을 추가하거나, 타임프레임을 선택한 뒤 [모니터 시작]을 누르세요.")
            return
        self.monitor_running = True
        self.btn_start.config(state=tk.DISABLED)
        self.btn_stop.config(state=tk.NORMAL)
        self._log("모니터링 시작 (봉 마감 시점에만 조건 점검)")
        self._monitor_thread = threading.Thread(target=self._run_monitor, daemon=True)
        self._monitor_thread.start()

    def _on_stop_monitor(self):
        self.monitor_running = False
        self.btn_start.config(state=tk.NORMAL)
        self.btn_stop.config(state=tk.DISABLED)
        self._log("모니터링 중지됨")

    def _on_quit(self):
        """모니터 중지 후 창을 닫고 프로세스를 완전히 종료."""
        self.monitor_running = False
        try:
            self.root.quit()
            self.root.destroy()
        except Exception:
            pass
        os._exit(0)

    def _run_monitor(self):
        TF_ORDER = ["1시간", "10분", "5분", "2분"]
        while self.monitor_running:
            try:
                now_kst = datetime.now(KST)
                with self._reservations_lock:
                    reservations_copy = [dict(r) for r in self.reservations]
                if reservations_copy:
                    work_list = []
                    now_kst = datetime.now(KST)
                    for res in reservations_copy:
                        symbol = (res.get("symbol") or "XAUUSD+").strip()
                        if not symbol.endswith("+"):
                            symbol = symbol + "+"
                        tfs = res.get("tfs") or []
                        weight_pct = float(res.get("weight_pct", 1.0))
                        for tf_lbl in TF_ORDER:
                            if tf_lbl not in tfs:
                                continue
                            mt5_tf = TF_MAP.get(tf_lbl)
                            if mt5_tf is None:
                                continue
                            if not _is_bar_closed_at_least_1min_ago(mt5_tf, now_kst):
                                continue
                            work_list.append((res, symbol, tf_lbl, mt5_tf, weight_pct))
                            break
                    work_list.sort(key=lambda x: TF_ORDER.index(x[2]) if x[2] in TF_ORDER else 99)
                    if not work_list:
                        time.sleep(60.0)
                        continue
                    res, symbol, tf_label, mt5_tf, weight_pct = work_list[0]
                    # 진입 점검 시 예약된 타임프레임(tf_label/mt5_tf)의 봉으로 BB 계산 (5분 예약 → 5분봉 BB)
                    is_sell = (res.get("side") or "매수").strip() == "매도"
                    if is_sell:
                        matched, msg, bb_lower, retrace_level, sl_price = check_breakout_sell(
                            symbol, mt5_tf, weight_pct
                        )
                    else:
                        matched, msg, bb_upper, retrace_level, sl_price = check_breakout_doublebottom(
                            symbol, mt5_tf, weight_pct
                        )
                    tp_enabled = res.get("tp_enabled", False)
                    tp_ktr_str = (res.get("tp_ktr") or "1").strip()
                    tp_x2 = res.get("tp_x2", False)
                else:
                    selected_tfs = self._get_selected_tfs()
                    if not selected_tfs:
                        time.sleep(60.0)
                        continue
                    symbol = (self.var_symbol.get() or "XAUUSD+").strip()
                    if not symbol.endswith("+"):
                        symbol = symbol + "+"
                    weight_s = (self.var_weight.get() or "1%").strip().replace("%", "")
                    try:
                        weight_pct = float(weight_s)
                    except ValueError:
                        weight_pct = 1.0
                    now_kst = datetime.now(KST)
                    matched = False
                    tf_label = None
                    mt5_tf = None
                    sl_price = None
                    msg = None
                    is_sell = (self.var_buysell.get() or "매수").strip() == "매도"
                    # 예약 없을 때: 선택된 타임프레임의 봉으로 BB 계산
                    for tf_lbl, tf_mt5 in selected_tfs:
                        if not _is_bar_closed_at_least_1min_ago(tf_mt5, now_kst):
                            continue
                        if is_sell:
                            matched, msg, bb_lower, retrace_level, sl_price = check_breakout_sell(
                                symbol, tf_mt5, weight_pct
                            )
                        else:
                            matched, msg, bb_upper, retrace_level, sl_price = check_breakout_doublebottom(
                                symbol, tf_mt5, weight_pct
                            )
                        tf_label = tf_lbl
                        mt5_tf = tf_mt5
                        if matched:
                            break
                    tp_enabled = self.var_tp_enabled.get()
                    tp_ktr_str = (self.var_tp_ktr.get() or "1").strip()
                    tp_x2 = self.var_tp_x2.get()
                    # now_kst already set above

                if not msg:
                    time.sleep(60.0)
                    continue

                self._log(f"[{now_kst.strftime('%H:%M')}] [{tf_label or '?'}] {msg}")

                if matched and sl_price is not None and tf_label is not None:
                    is_sell = (self.var_buysell.get() or "매수").strip() == "매도"
                    if is_sell:
                        ok_align, align_reason = _is_higher_tf_reverse_alignment(symbol, tf_label)
                    else:
                        ok_align, align_reason = _is_higher_tf_correct_alignment(symbol, tf_label)
                    if not ok_align:
                        self._log(f"⏭️ [{tf_label}] {align_reason}")
                        time.sleep(60.0)
                        continue
                    if not tr.init_mt5():
                        self._log("MT5 연결 실패 → 진입 스킵")
                        time.sleep(60)
                        continue
                    ask, bid = tr.get_market_price(symbol)
                    side = "SELL" if is_sell else "BUY"
                    entry_price = (bid if is_sell else ask)
                    if entry_price is None or entry_price <= 0:
                        self._log("호가 조회 실패 → 진입 스킵")
                        time.sleep(60)
                        continue
                    acc = tr.get_account_info()
                    balance = float(acc.get("balance", 0) or 0)
                    if balance <= 0:
                        self._log("잔액 없음 → 진입 스킵")
                        time.sleep(60)
                        continue
                    lot = calc_lot_by_margin_weight(balance, weight_pct, symbol, entry_price, side=side)
                    if lot < 0.01:
                        self._log(f"랏수 계산 결과 너무 작음 ({lot}) → 스킵")
                        time.sleep(60)
                        continue
                    comment = f"BBdb_{'sell' if is_sell else 'buy'}_{tf_label}_{weight_pct}%"
                    ok, result = tr.execute_market_order(
                        symbol, side, lot, magic=MAGIC_BREAKOUT_DB, comment=comment
                    )
                    if ok:
                        self._log(f"진입 성공: {symbol} {side} {lot}랏 (비중 {weight_pct}%)")
                        tp_price = 0.0
                        try:
                            if tp_enabled and tp_ktr_str and tp_ktr_str != "없음" and tp_ktr_str in ("0.5", "1", "2", "3"):
                                ktr_tf = TF_TO_KTR_DB.get(tf_label, "1H")
                                ktr_value, _ = get_ktr_from_db_auto(symbol, ktr_tf)
                                if ktr_value and ktr_value > 0:
                                    tp_mult = float(tp_ktr_str)
                                    ktr_eff = ktr_value * 2.0 if tp_x2 else ktr_value
                                    if is_sell:
                                        tp_price = entry_price - ktr_eff * tp_mult
                                    else:
                                        tp_price = entry_price + ktr_eff * tp_mult
                                    x2_note = " (X2)" if tp_x2 else ""
                                    self._log(f"T/P: 진입가 {'- KTR' if is_sell else '+ KTR'}×{tp_ktr_str}{x2_note} = {tp_price:.2f} (KTR={ktr_eff:.2f})")
                                else:
                                    self._log("T/P: KTR 값 없음 → T/P 미설정")
                        except Exception as e:
                            self._log(f"T/P 계산 실패(무시): {e}")
                        try:
                            positions = mt5.positions_get(symbol=symbol)
                            if positions:
                                last_pos = max(positions, key=lambda p: p.time)
                                tr.modify_position_sltp(last_pos.ticket, symbol, sl_price, tp_price if tp_price > 0 else 0.0)
                                self._log(f"손절 설정: {sl_price:.2f}" + (f", T/P: {tp_price:.2f}" if tp_price > 0 else ""))
                        except Exception as e:
                            self._log(f"손절/T/P 설정 실패(무시): {e}")
                        # KTR 오더 추가: 매수=체결가-KTR, 매도=체결가+KTR 예약
                        try:
                            ktr_ok, ktr_msg = _place_ktr_order_after_breakout_entry(
                                symbol, side, lot, entry_price, tf_label
                            )
                            if ktr_ok:
                                self._log(f"KTR 오더: {ktr_msg}")
                            else:
                                self._log(f"KTR 오더 미등록: {ktr_msg}")
                        except Exception as e:
                            self._log(f"KTR 오더 실패(무시): {e}")
                        msg_telegram = f"🟢 돌파더블비 진입(모니터): {symbol} {tf_label} {side} {lot}랏 (비중 {weight_pct}%) S/L {sl_price:.2f}"
                        if tp_price > 0:
                            msg_telegram += f" T/P {tp_price:.2f}"
                        self._send_telegram(msg_telegram)
                    else:
                        self._log(f"진입 실패: {result}")
                        self._send_telegram(f"🔴 돌파더블비 진입 실패: {symbol} {tf_label} — {result}")
                    time.sleep(60)

            except Exception as e:
                import traceback
                self._log(f"오류: {e}")
                self.root.after(0, lambda: self._log(traceback.format_exc()))
            # 진입조건 점검: 1분 단위로 수행
            time.sleep(60.0)

    def run(self):
        self.root.mainloop()
        self.monitor_running = False


def main():
    app = BreakoutDoubleBottomApp()
    app.run()


if __name__ == "__main__":
    main()
