# -*- coding: utf-8 -*-
"""
KTR 측정 및 DB 저장·텔레그램 알림. 인자로 5M, 10M, 1H 타임프레임 선택.
노션 업로드 없음 (DB·텔레그램만 사용).

- 5M: 해당 세션 시작 5분봉 1개의 High−Low (예: 아시아 08:00 봉).
  아시아만: 직전 봉(6:55)과 세션 시작 봉(8:00)을 합쳐 KTR = max(고가들)−min(저가들).
- 10M: 해당 세션 시작 10분 구간 5M 두 봉 High−Low. MT5 우선, 없으면 포지션모니터 DB.
  아시아만: 직전 봉(6:50)과 세션 시작 봉(8:00) M5 두 봉을 합쳐 KTR 산출.
- 1H: 해당 세션 1시간 구간 5M 봉들 max(High)−min(Low). MT5 우선, 없으면 포지션모니터 DB.
  아시아만: 08:00 1H 봉 + 10분봉 06:50 봉을 합쳐 High/Low로 KTR 산출.
  → 항상 MT5 기준으로만 사용 (yfinance 미사용).

사용법:
  python ktr_measure_calculator.py 5M
  python ktr_measure_calculator.py 10M
  python ktr_measure_calculator.py 1H
  python ktr_measure_calculator.py --timeframe 10M
"""
import argparse
import os
import sys
import MetaTrader5 as mt5
import pytz
from datetime import datetime, timedelta

# v2 루트를 path에 추가 (다른 cwd에서 실행 시 아래 import를 위해)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

try:
    from cronjob_logger import log_cronjob
except ImportError:
    def log_cronjob(job_label: str, message: str, is_error: bool = False) -> None:
        """v2: cronjob_logger 없을 때 no-op."""
        pass

from telegram_sender_utils import send_telegram_msg
from ktr_db_utils import KTRDatabase
from ktr_lots import get_ktrlots_lots
from mt5_trade_utils import init_mt5, MT5_PATH
from mt5_time_utils import mt5_ts_to_kst

# 출력 인코딩 설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# --- 설정값 --- (MT5_PATH는 mt5_trade_utils에서 path_config 기준으로 로드)
KST = pytz.timezone("Asia/Seoul")
SYMBOLS = {"NAS100": "NQ=F", "XAUUSD": "GC=F"}  # 키만 사용 (MT5 심볼은 + 붙여서 별도 처리)
try:
    from db_config import UNIFIED_DB_PATH
    KTR_DB_PATH = UNIFIED_DB_PATH
except ImportError:
    KTR_DB_PATH = os.path.join(_SCRIPT_DIR, "scheduler.db")

# 세션별 KTR 기준 봉 시각 (KST): 아시아 8시, 유럽 17시, 미국 5M/10M=23:30 / 1H=23시
SESSION_BAR_5M = {"Asia": (8, 0), "Europe": (17, 0), "US": (23, 30)}
SESSION_BAR_10M = {"Asia": (8, 0), "Europe": (17, 0), "US": (23, 30)}  # 10분봉도 세션 시작 동일
SESSION_BAR_1H = {"Asia": (8, 0), "Europe": (17, 0), "US": (23, 0)}

try:
    import position_monitor_db as _pm_db
except ImportError:
    _pm_db = None


def _tg(msg: str) -> None:
    """텔레그램 전송 (실패 시 로그만)."""
    try:
        send_telegram_msg(msg)
    except Exception as e:
        print(f"⚠️ 텔레그램 전송 실패: {e}")


def send_telegram(market: str, balance: float, results_list: list, timeframe: str) -> None:
    tf_label = "5M" if timeframe == "5M" else ("10M" if timeframe == "10M" else "1H")
    msg = f"📊 **{market} Market KTR {tf_label} 리포트**\n"
    msg += f"💰 Balance: `${balance:,.2f}`\n"
    msg += f"{'─' * 20}\n"
    for r in results_list:
        msg += f"🔸 **{r['symbol']}** (KTR: {r['ktr']})\n"
        msg += f"└ 1st: `{r['lot_1st']}` lot\n"
        msg += f"└ 2nd: `{r['lot_2nd']}` lot\n"
        msg += f"└ 3rd: `{r['lot_3rd']}` lot\n\n"
    msg += "※ 로컬 DB 저장 완료"
    _tg(msg)


def get_target_time_info_5m() -> tuple:
    """5M: 분석할 시장과 타겟 시각(시, 분). 아시아 8시, 유럽 17시, 미국 23:30."""
    now = datetime.now(KST)
    h, m = now.hour, now.minute
    if (h == 23 and m >= 30) or (0 <= h < 8):
        return SESSION_BAR_5M["US"][0], SESSION_BAR_5M["US"][1], "US"
    if h >= 17:
        return SESSION_BAR_5M["Europe"][0], SESSION_BAR_5M["Europe"][1], "Europe"
    return SESSION_BAR_5M["Asia"][0], SESSION_BAR_5M["Asia"][1], "Asia"


def get_current_session_kst() -> str:
    """현재 시각(KST) 기준 세션. 누락 업데이트 시 이 세션만 채움. 5M/10M과 동일 구간: US 23:30~08:00, Europe 17~23:29, Asia 08~16:59."""
    now = datetime.now(KST)
    h, m = now.hour, now.minute
    if (h == 23 and m >= 30) or (0 <= h < 8):
        return "US"
    if h >= 17:
        return "Europe"
    return "Asia"


def get_target_time_info_10m() -> tuple:
    """10M: 분석할 시장과 타겟 시각(시, 분). 5M과 동일(아시아 8시, 유럽 17시, 미국 23:30)."""
    now = datetime.now(KST)
    h, m = now.hour, now.minute
    if (h == 23 and m >= 30) or (0 <= h < 8):
        return SESSION_BAR_10M["US"][0], SESSION_BAR_10M["US"][1], "US"
    if h >= 17:
        return SESSION_BAR_10M["Europe"][0], SESSION_BAR_10M["Europe"][1], "Europe"
    return SESSION_BAR_10M["Asia"][0], SESSION_BAR_10M["Asia"][1], "Asia"


def get_target_time_info_1h() -> tuple:
    """1H: 분석할 시장, 타겟 시각(시, 분). 아시아 8시, 유럽 17시, 미국 23시."""
    now = datetime.now(KST)
    h = now.hour
    if 0 <= h < 8:
        return SESSION_BAR_1H["US"][0], SESSION_BAR_1H["US"][1], "US"
    if h >= 18:
        return SESSION_BAR_1H["Europe"][0], SESSION_BAR_1H["Europe"][1], "Europe"
    return SESSION_BAR_1H["Asia"][0], SESSION_BAR_1H["Asia"][1], "Asia"


def _symbol_for_pm_db(name: str) -> str:
    """KTR 심볼명(NAS100, XAUUSD) → 포지션 모니터 DB 심볼(NAS100+, XAUUSD+)."""
    return name.strip() + "+" if not name.endswith("+") else name


def get_ktr_from_pm_db(symbol_name: str, session: str, timeframe: str, record_date: str | None = None):
    """포지션 모니터 DB에서 세션 기준 봉을 찾아 KTR(High−Low) 반환.
    record_date: YYYY-MM-DD (기본: 오늘 KST). 해당 날짜 봉만 사용.
    반환: (ktr_value, bar_time_str) 또는 (None, None).
    아시아 5M: 6:55+8:00 두 봉 합산. 아시아 10M: 6:50+8:00 두 봉 합산. 아시아 1H: 08:00 1H 봉 + 10분봉 06:50 봉 합산."""
    if _pm_db is None:
        return None, None
    record_prefix = (record_date or datetime.now(KST).strftime("%Y-%m-%d")).strip()[:10]
    # US 세션: 봉은 전날 23:00/23:30에 열리므로 record_date가 세션 마감일일 때 DB 검색은 전날
    try:
        rd = datetime.strptime(record_prefix, "%Y-%m-%d").date()
        bar_date = (rd - timedelta(days=1)) if (session == "US" and timeframe in ("1H", "5M", "10M")) else rd
        date_prefix = bar_date.strftime("%Y-%m-%d")
    except Exception:
        date_prefix = record_prefix
    sym = _symbol_for_pm_db(symbol_name)
    if timeframe == "5M":
        tf, hour, minute = "M5", SESSION_BAR_5M[session][0], SESSION_BAR_5M[session][1]
    elif timeframe == "10M":
        tf, hour, minute = "M10", SESSION_BAR_10M[session][0], SESSION_BAR_10M[session][1]
    else:
        tf, hour, minute = "H1", SESSION_BAR_1H[session][0], SESSION_BAR_1H[session][1]
    limit = 500 if tf == "M5" else (300 if tf == "M10" else 200)
    bars = _pm_db.get_bars_from_db(sym, tf, limit=limit)
    if not bars:
        return None, None

    # 아시아 5M: 6:55 봉 + 8:00 봉 합쳐 KTR (해당 날짜만)
    if timeframe == "5M" and session == "Asia":
        suffixes = [" 06:55:00", " 08:00:00"]
        found = []
        for suf in suffixes:
            for b in bars:
                bt = (b.get("bar_time") or "").strip()
                if bt.startswith(date_prefix) and bt.endswith(suf):
                    found.append((float(b["high"]), float(b["low"])))
                    break
        if len(found) == 2:
            high_max = max(found[0][0], found[1][0])
            low_min = min(found[0][1], found[1][1])
            return round(high_max - low_min, 2), (date_prefix + " 06:55+08:00")
        return None, None

    # 아시아 10M: 6:50, 8:00 M5 두 봉만 합쳐 KTR (해당 날짜만, DB는 M5 봉으로 조회)
    if timeframe == "10M" and session == "Asia":
        bars_m5 = _pm_db.get_bars_from_db(sym, "M5", limit=500)
        if not bars_m5:
            return None, None
        suffixes_2 = [" 06:50:00", " 08:00:00"]
        found = []
        for suf in suffixes_2:
            for b in bars_m5:
                bt = (b.get("bar_time") or "").strip()
                if bt.startswith(date_prefix) and bt.endswith(suf):
                    found.append((float(b["high"]), float(b["low"])))
                    break
        if len(found) == 2:
            high_max = max(f[0] for f in found)
            low_min = min(f[1] for f in found)
            return round(high_max - low_min, 2), (date_prefix + " 06:50+08:00")
        return None, None

    # 아시아 1H: 08:00 1H 봉 + 10분봉 06:50 봉 합쳐 KTR (해당 날짜만)
    if timeframe == "1H" and session == "Asia":
        bars_h1 = _pm_db.get_bars_from_db(sym, "H1", limit=200)
        bars_m10 = _pm_db.get_bars_from_db(sym, "M10", limit=300)
        if not bars_h1 or not bars_m10:
            return None, None
        h1_bar = None
        for b in bars_h1:
            bt = (b.get("bar_time") or "").strip()
            if bt.startswith(date_prefix) and bt.endswith(" 08:00:00"):
                h1_bar = (float(b["high"]), float(b["low"]))
                break
        m10_bar = None
        for b in bars_m10:
            bt = (b.get("bar_time") or "").strip()
            if bt.startswith(date_prefix) and bt.endswith(" 06:50:00"):
                m10_bar = (float(b["high"]), float(b["low"]))
                break
        if h1_bar is not None and m10_bar is not None:
            high_max = max(h1_bar[0], m10_bar[0])
            low_min = min(h1_bar[1], m10_bar[1])
            return round(high_max - low_min, 2), (date_prefix + " 06:50+08:00 1H")
        return None, None

    # 그 외: 해당 날짜의 세션 시작 봉 1개(또는 10M 두 봉)
    suffix = f" {hour:02d}:{minute:02d}:00"
    for b in bars:
        bt = (b.get("bar_time") or "").strip()
        if bt.startswith(date_prefix) and bt.endswith(suffix):
            high, low = float(b["high"]), float(b["low"])
            return round(high - low, 2), bt
    return None, None


def _mt5_symbol(symbol_name: str) -> str:
    """KTR 심볼명(NAS100, XAUUSD) → MT5 차트/봉 조회용 심볼(NAS100+, XAUUSD+)."""
    n = (symbol_name or "").strip()
    if not n.endswith("+"):
        n = n + "+"
    return n


def get_ktr_from_mt5(symbol_name: str, session: str, timeframe: str, record_date: str | None = None):
    """MT5에서 해당 세션·타임프레임의 봉을 조회해 KTR(High−Low) 산출.
    record_date: YYYY-MM-DD (기본: 오늘 KST). 반환: (ktr_value, bar_time_str) 또는 (None, None)."""
    if not init_mt5():
        return None, None
    sym = _mt5_symbol(symbol_name)
    if not mt5.symbol_select(sym, True):
        return None, None
    today = datetime.now(KST).date()
    if record_date:
        try:
            from datetime import datetime as dt
            target_date = dt.strptime(record_date.strip()[:10], "%Y-%m-%d").date()
        except Exception:
            target_date = today
    else:
        target_date = today

    mt5_m5 = getattr(mt5, "TIMEFRAME_M5", 5)
    count = 600

    if timeframe == "5M":
        hour, minute = SESSION_BAR_5M[session][0], SESSION_BAR_5M[session][1]
        # US 5M: 봉은 전날 23:30, record_date는 세션 마감일(당일 00:00) → 봉은 전날 날짜로 검색
        bar_date_5m = target_date - timedelta(days=1) if session == "US" else target_date
        rates = mt5.copy_rates_from_pos(sym, mt5_m5, 0, count)
        if rates is None or len(rates) == 0:
            return None, None
        # 아시아 5M: 6:55 봉 + 8:00 봉 합쳐 KTR (max(고가)−min(저가))
        if session == "Asia":
            bars_needed = [(6, 55), (8, 0)]
            found = []
            for (h, m) in bars_needed:
                for i in range(len(rates)):
                    bar_ts = int(rates["time"][i])
                    bar_dt = mt5_ts_to_kst(bar_ts)
                    if bar_dt.date() == target_date and bar_dt.hour == h and bar_dt.minute == m:
                        found.append((float(rates["high"][i]), float(rates["low"][i])))
                        break
            if len(found) == 2:
                high_max = max(found[0][0], found[1][0])
                low_min = min(found[0][1], found[1][1])
                return round(high_max - low_min, 2), f"{target_date} 06:55+08:00"
        # 그 외 세션: 기존 단일 봉 (US는 bar_date_5m 사용)
        for i in range(len(rates)):
            bar_ts = int(rates["time"][i])
            bar_dt = mt5_ts_to_kst(bar_ts)
            if bar_dt.date() == bar_date_5m and bar_dt.hour == hour and bar_dt.minute == minute:
                high, low = float(rates["high"][i]), float(rates["low"][i])
                return round(high - low, 2), bar_dt.strftime("%Y-%m-%d %H:%M")
        return None, None

    if timeframe == "10M":
        hour, minute = SESSION_BAR_10M[session][0], SESSION_BAR_10M[session][1]
        # US 10M: 봉은 전날 23:30/23:35, record_date는 세션 마감일 → 봉은 전날 날짜로 검색
        bar_date_10m = target_date - timedelta(days=1) if session == "US" else target_date
        # 아시아 10M: 6:50, 8:00 M5 두 봉만 합쳐 KTR
        if session == "Asia":
            bars_needed = [(6, 50), (8, 0)]
            rates = mt5.copy_rates_from_pos(sym, mt5_m5, 0, count)
            if rates is None or len(rates) < 2:
                return None, None
            found = []
            for (h, m) in bars_needed:
                for i in range(len(rates)):
                    bar_ts = int(rates["time"][i])
                    bar_dt = mt5_ts_to_kst(bar_ts)
                    if bar_dt.date() == target_date and bar_dt.hour == h and bar_dt.minute == m:
                        found.append((float(rates["high"][i]), float(rates["low"][i])))
                        break
            if len(found) == 2:
                high_max = max(found[0][0], found[1][0])
                low_min = min(found[0][1], found[1][1])
                return round(high_max - low_min, 2), f"{target_date} 06:50+08:00"
            return None, None
        # 그 외 세션: 기존 2봉(세션 시작 + 5분). US는 bar_date_10m 사용
        min2 = (minute + 5) % 60
        hour2 = hour if minute < 55 else (hour + 1) % 24
        rates = mt5.copy_rates_from_pos(sym, mt5_m5, 0, count)
        if rates is None or len(rates) < 2:
            return None, None
        bars_1 = bars_2 = None
        for i in range(len(rates)):
            bar_ts = int(rates["time"][i])
            bar_dt = mt5_ts_to_kst(bar_ts)
            if bar_dt.date() != bar_date_10m:
                continue
            if bar_dt.hour == hour and bar_dt.minute == minute:
                bars_1 = (float(rates["high"][i]), float(rates["low"][i]))
            if bar_dt.hour == hour2 and bar_dt.minute == min2:
                bars_2 = (float(rates["high"][i]), float(rates["low"][i]))
        if bars_1 is None or bars_2 is None:
            return None, None
        high_max = max(bars_1[0], bars_2[0])
        low_min = min(bars_1[1], bars_2[1])
        return round(high_max - low_min, 2), f"{target_date} {hour:02d}:{minute:02d}~{hour2:02d}:{min2:02d}"

    if timeframe == "1H":
        hour, minute = SESSION_BAR_1H[session][0], SESSION_BAR_1H[session][1]
        # US 1H: 봉이 전날 23:00에 열리고 당일 00:00에 닫힘 → record_date가 마감일이므로 봉은 전날 날짜로 검색
        bar_date = target_date
        if session == "US":
            bar_date = target_date - timedelta(days=1)
        # 아시아 1H: 08:00 1H 봉 + 10분봉 06:50 봉 합쳐 KTR
        if session == "Asia":
            mt5_h1 = getattr(mt5, "TIMEFRAME_H1", 16385)
            mt5_m10 = getattr(mt5, "TIMEFRAME_M10", 10)
            rates_h1 = mt5.copy_rates_from_pos(sym, mt5_h1, 0, 200)
            rates_m10 = mt5.copy_rates_from_pos(sym, mt5_m10, 0, 300)
            if rates_h1 is None or len(rates_h1) == 0 or rates_m10 is None or len(rates_m10) == 0:
                return None, None
            h1_high = h1_low = m10_high = m10_low = None
            for i in range(len(rates_h1)):
                bar_ts = int(rates_h1["time"][i])
                bar_dt = mt5_ts_to_kst(bar_ts)
                if bar_dt.date() == target_date and bar_dt.hour == 8 and bar_dt.minute == 0:
                    h1_high = float(rates_h1["high"][i])
                    h1_low = float(rates_h1["low"][i])
                    break
            for i in range(len(rates_m10)):
                bar_ts = int(rates_m10["time"][i])
                bar_dt = mt5_ts_to_kst(bar_ts)
                if bar_dt.date() == target_date and bar_dt.hour == 6 and bar_dt.minute == 50:
                    m10_high = float(rates_m10["high"][i])
                    m10_low = float(rates_m10["low"][i])
                    break
            if h1_high is not None and m10_high is not None:
                high_max = max(h1_high, m10_high)
                low_min = min(h1_low, m10_low)
                return round(high_max - low_min, 2), f"{target_date} 06:50+08:00 1H"
            return None, None
        # 그 외 세션: 해당 시각 1시간 구간 5M 봉들 max(High)−min(Low). US 1H는 bar_date(전날) 기준
        rates = mt5.copy_rates_from_pos(sym, mt5_m5, 0, count)
        if rates is None or len(rates) == 0:
            return None, None
        hour_highs, hour_lows = [], []
        for i in range(len(rates)):
            bar_ts = int(rates["time"][i])
            bar_dt = mt5_ts_to_kst(bar_ts)
            if bar_dt.date() == bar_date and bar_dt.hour == hour:
                hour_highs.append(float(rates["high"][i]))
                hour_lows.append(float(rates["low"][i]))
        if not hour_highs or not hour_lows:
            return None, None
        ktr = round(max(hour_highs) - min(hour_lows), 2)
        return ktr, f"{target_date} {hour:02d}:{minute:02d} 1H"

    return None, None


def get_mt5_balance() -> float:
    if not mt5.initialize(MT5_PATH):
        return 100000.0
    balance = mt5.account_info().balance
    mt5.shutdown()
    return balance


def run_5m(ktr_db_path: str | None = None, session_override: str | None = None, record_date: str | None = None, quiet: bool = False) -> None:
    """session_override가 있으면 해당 세션(Asia/Europe/US)만 측정; 없으면 현재 시각 기준 세션. record_date 지정 시 해당 날짜로 조회·저장(누락 보충용)."""
    if session_override and session_override in SESSION_BAR_5M:
        target_hour, target_min = SESSION_BAR_5M[session_override][0], SESSION_BAR_5M[session_override][1]
        market_name = session_override
    else:
        target_hour, target_min, market_name = get_target_time_info_5m()
    balance = get_mt5_balance()
    timeframe = "5M"
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    if not quiet:
        print(f"\n▶ KTR 5M 레포트 시작: {ts}")
        _tg(f"🟡 **KTR 5M 측정 시작** ({market_name})\n{ts}")

    final_results = []
    failed_list = []  # [{"symbol": str, "reason": str}, ...]
    db_success_count = 0
    db_path = ktr_db_path if ktr_db_path else KTR_DB_PATH
    ktr_db = KTRDatabase(db_name=db_path)

    date_str = (record_date or datetime.now(KST).strftime("%Y-%m-%d")).strip()[:10]
    for name, sym in SYMBOLS.items():
        ktr, bar_time_str = get_ktr_from_mt5(name, market_name, timeframe, record_date=date_str)
        if ktr is not None and bar_time_str:
            if not quiet:
                print(f"✓ {name}: MT5 기준 {target_hour:02d}:{target_min:02d} 5M 봉 ({bar_time_str}) - KTR : {ktr}")
        else:
            ktr, bar_time_str = get_ktr_from_pm_db(name, market_name, timeframe, record_date=date_str)
            if ktr is not None and bar_time_str:
                if not quiet:
                    print(f"✓ {name}: DB 기준 {target_hour:02d}:{target_min:02d} 5M 봉 ({bar_time_str}) - KTR : {ktr}")
            else:
                if not quiet:
                    print(f"⚠️ {name}: MT5/DB에 5M 봉 없음 - 스킵")
                failed_list.append({"symbol": name, "reason": "데이터 없음"})
                if not quiet:
                    _tg(f"  ⚠️ **{name}** 5M: 데이터 없음 - 스킵")
                continue

        if not quiet:
            print(f"\n=== ktrlots.com 랏수 조회 (ktr_lots) ===")
            print(f"입력: 증거금${balance}, 리스크10%, 구간수2.5, KTR={ktr}, 종목={name}\n")
        lots = get_ktrlots_lots(balance, 10, 2.5, ktr, name, headless=True)
        if lots:
            if not quiet:
                print(f"▶ 1st={lots['1st']}, 2nd={lots['2nd']}, 3rd={lots['3rd']}")
            lot_1st, lot_2nd, lot_3rd = lots["1st"], lots["2nd"], lots["3rd"]
        else:
            if not quiet:
                print("⚠️ ktrlots 랏수 조회 실패 → KTR만 업로드 (랏수 0)")
            lot_1st, lot_2nd, lot_3rd = 0.0, 0.0, 0.0

        ktr_db.update_ktr(
            symbol=name,
            session=market_name,
            timeframe=timeframe,
            value=ktr,
            balance=balance,
            lot_1st=lot_1st,
            lot_2nd=lot_2nd,
            lot_3rd=lot_3rd,
            record_date=date_str,
        )
        final_results.append({
            "symbol": name, "ktr": ktr,
            "lot_1st": lot_1st, "lot_2nd": lot_2nd, "lot_3rd": lot_3rd,
        })
        db_success_count += 1
        if not quiet:
            if lot_1st > 0:
                _tg(f"  ✓ **{name}** 5M KTR {ktr} DB 저장 완료")
            else:
                _tg(f"  ✓ **{name}** 5M KTR {ktr} DB 저장 완료 (랏수 0)")

    _finish(market_name, balance, final_results, db_success_count, timeframe, failed_list, quiet=quiet)
    return db_success_count


def run_10m(ktr_db_path: str | None = None, session_override: str | None = None, record_date: str | None = None, quiet: bool = False):
    """10M KTR = 세션 시작 10분 구간 1봉(또는 5M 두 봉) High−Low. session_override 시 해당 세션만 측정. record_date 지정 시 누락 보충용."""
    if session_override and session_override in SESSION_BAR_10M:
        target_hour, target_min = SESSION_BAR_10M[session_override][0], SESSION_BAR_10M[session_override][1]
        market_name = session_override
    else:
        target_hour, target_min, market_name = get_target_time_info_10m()
    balance = get_mt5_balance()
    timeframe = "10M"
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    if not quiet:
        print(f"\n▶ KTR 10M 레포트 시작: {ts}")
        _tg(f"🟡 **KTR 10M 측정 시작** ({market_name})\n{ts}")

    final_results = []
    failed_list = []
    db_success_count = 0
    db_path = ktr_db_path if ktr_db_path else KTR_DB_PATH
    ktr_db = KTRDatabase(db_name=db_path)

    date_str = (record_date or datetime.now(KST).strftime("%Y-%m-%d")).strip()[:10]
    for name, sym in SYMBOLS.items():
        ktr, bar_time_str = get_ktr_from_mt5(name, market_name, timeframe, record_date=date_str)
        if ktr is not None and bar_time_str:
            if not quiet:
                print(f"✓ {name}: MT5 기준 {target_hour:02d}:{target_min:02d} 10M 봉 ({bar_time_str}) - KTR : {ktr}")
        else:
            ktr, bar_time_str = get_ktr_from_pm_db(name, market_name, timeframe, record_date=date_str)
            if ktr is not None and bar_time_str:
                if not quiet:
                    print(f"✓ {name}: DB 기준 {target_hour:02d}:{target_min:02d} 10M 봉 ({bar_time_str}) - KTR : {ktr}")
            else:
                if not quiet:
                    print(f"⚠️ {name}: MT5/DB에 10M 봉 없음 - 스킵")
                failed_list.append({"symbol": name, "reason": "데이터 없음"})
                if not quiet:
                    _tg(f"  ⚠️ **{name}** 10M: 데이터 없음 - 스킵")
                continue

        if not quiet:
            print(f"\n=== ktrlots.com 랏수 조회 (ktr_lots) ===")
            print(f"입력: 증거금${balance}, 리스크10%, 구간수2.5, KTR={ktr}, 종목={name}\n")
        lots = get_ktrlots_lots(balance, 10, 2.5, ktr, name, headless=True)
        if lots:
            if not quiet:
                print(f"▶ 1st={lots['1st']}, 2nd={lots['2nd']}, 3rd={lots['3rd']}")
            lot_1st, lot_2nd, lot_3rd = lots["1st"], lots["2nd"], lots["3rd"]
        else:
            if not quiet:
                print("⚠️ ktrlots 랏수 조회 실패 → KTR만 업로드 (랏수 0)")
            lot_1st, lot_2nd, lot_3rd = 0.0, 0.0, 0.0

        ktr_db.update_ktr(
            symbol=name,
            session=market_name,
            timeframe=timeframe,
            value=ktr,
            balance=balance,
            lot_1st=lot_1st,
            lot_2nd=lot_2nd,
            lot_3rd=lot_3rd,
            record_date=date_str,
        )
        final_results.append({
            "symbol": name, "ktr": ktr,
            "lot_1st": lot_1st, "lot_2nd": lot_2nd, "lot_3rd": lot_3rd,
        })
        db_success_count += 1
        if not quiet:
            if lot_1st > 0:
                _tg(f"  ✓ **{name}** 10M KTR {ktr} DB 저장 완료")
            else:
                _tg(f"  ✓ **{name}** 10M KTR {ktr} DB 저장 완료 (랏수 0)")

    _finish(market_name, balance, final_results, db_success_count, timeframe, failed_list, quiet=quiet)
    return db_success_count


def run_1h(ktr_db_path: str | None = None, session_override: str | None = None, record_date: str | None = None, quiet: bool = False):
    """1H KTR = 세션 시작 1시간 구간 5M 봉들 max(High)−min(Low). session_override 시 해당 세션만 측정. record_date 지정 시 누락 보충용."""
    if session_override and session_override in SESSION_BAR_1H:
        target_hour, session_start_min = SESSION_BAR_1H[session_override][0], SESSION_BAR_1H[session_override][1]
        market_name = session_override
    else:
        target_hour, session_start_min, market_name = get_target_time_info_1h()
    balance = get_mt5_balance()
    timeframe = "1H"
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    if not quiet:
        print(f"\n▶ KTR 1H 레포트 시작: {ts}")
        _tg(f"🟡 **KTR 1H 측정 시작** ({market_name})\n{ts}")

    final_results = []
    failed_list = []
    db_success_count = 0
    db_path = ktr_db_path if ktr_db_path else KTR_DB_PATH
    ktr_db = KTRDatabase(db_name=db_path)

    date_str = (record_date or datetime.now(KST).strftime("%Y-%m-%d")).strip()[:10]
    for name, sym in SYMBOLS.items():
        ktr, bar_time_str = get_ktr_from_mt5(name, market_name, timeframe, record_date=date_str)
        if ktr is not None and bar_time_str:
            if not quiet:
                print(f"✓ {name}: MT5 기준 {target_hour:02d}:{session_start_min:02d} 1H 봉 ({bar_time_str}) - KTR : {ktr}")
        else:
            ktr, bar_time_str = get_ktr_from_pm_db(name, market_name, timeframe, record_date=date_str)
            if ktr is not None and bar_time_str:
                if not quiet:
                    print(f"✓ {name}: DB 기준 {target_hour:02d}:{session_start_min:02d} 1H 봉 ({bar_time_str}) - KTR : {ktr}")
            else:
                if not quiet:
                    print(f"⚠️ {name}: MT5/DB에 1H 봉 없음 - 스킵")
                failed_list.append({"symbol": name, "reason": "데이터 없음"})
                if not quiet:
                    _tg(f"  ⚠️ **{name}** 1H: 데이터 없음 - 스킵")
                continue

        if not quiet:
            print(f"\n=== ktrlots.com 랏수 조회 (ktr_lots) ===")
            print(f"입력: 증거금${balance}, 리스크10%, 구간수2.5, KTR={ktr}, 종목={name}\n")
        lots = get_ktrlots_lots(balance, 10, 2.5, ktr, name, headless=True)
        if lots:
            if not quiet:
                print(f"▶ 1st={lots['1st']}, 2nd={lots['2nd']}, 3rd={lots['3rd']}")
            lot_1st, lot_2nd, lot_3rd = lots["1st"], lots["2nd"], lots["3rd"]
        else:
            if not quiet:
                print("⚠️ ktrlots 랏수 조회 실패 → KTR만 업로드 (랏수 0)")
            lot_1st, lot_2nd, lot_3rd = 0.0, 0.0, 0.0

        ktr_db.update_ktr(
            symbol=name,
            session=market_name,
            timeframe=timeframe,
            value=ktr,
            balance=balance,
            lot_1st=lot_1st,
            lot_2nd=lot_2nd,
            lot_3rd=lot_3rd,
            record_date=date_str,
        )
        final_results.append({
            "symbol": name, "ktr": ktr,
            "lot_1st": lot_1st, "lot_2nd": lot_2nd, "lot_3rd": lot_3rd,
        })
        db_success_count += 1
        if not quiet:
            if lot_1st > 0:
                _tg(f"  ✓ **{name}** 1H KTR {ktr} DB 저장 완료")
            else:
                _tg(f"  ✓ **{name}** 1H KTR {ktr} DB 저장 완료 (랏수 0)")

    _finish(market_name, balance, final_results, db_success_count, timeframe, failed_list, quiet=quiet)
    return db_success_count


def run_fill_missing_ktr_for_today(ktr_db_path: str | None = None, quiet: bool = True) -> int:
    """어제·오늘 누락된 (세션, 타임프레임, 날짜) 슬롯을 MT5(및 포지션모니터 DB) 봉 데이터로 측정해 자동 입력. 반환: 자동 입력한 슬롯 수."""
    try:
        db = KTRDatabase(db_name=ktr_db_path or KTR_DB_PATH)
        today_str = datetime.now(KST).strftime("%Y-%m-%d")
        yesterday_str = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
        missing = db.get_missing_ktr_slots([yesterday_str, today_str])
        db.conn.close()
    except Exception:
        return 0
    if not missing:
        return 0
    filled = 0
    for session, tf, record_date in sorted(missing, key=lambda x: (x[2], x[0], x[1])):
        try:
            if tf == "5M":
                written = run_5m(ktr_db_path=ktr_db_path, session_override=session, record_date=record_date, quiet=quiet)
            elif tf == "10M":
                written = run_10m(ktr_db_path=ktr_db_path, session_override=session, record_date=record_date, quiet=quiet)
            else:
                written = run_1h(ktr_db_path=ktr_db_path, session_override=session, record_date=record_date, quiet=quiet)
            if isinstance(written, int) and written > 0:
                filled += 1
            elif not quiet and (written is None or (isinstance(written, int) and written == 0)):
                print(f"  [KTR] {record_date} {session} {tf}: 측정 데이터 없음 → DB 미반영, 수동 입력 필요", flush=True)
        except Exception:
            pass
    return filled


def _finish(
    market_name: str,
    balance: float,
    final_results: list,
    db_success_count: int,
    timeframe: str,
    failed_list: list | None = None,
    quiet: bool = False,
) -> None:
    failed_list = failed_list or []
    tf_label = "5M" if timeframe == "5M" else ("10M" if timeframe == "10M" else "1H")
    job_label = f"KTR ({market_name}) {tf_label}"

    if final_results:
        summary = f"▶ {market_name} KTR {tf_label} 처리 완료 ({db_success_count}건)"
        if not quiet:
            send_telegram(market_name, balance, final_results, timeframe)
        log_cronjob(job_label, summary)
        if not quiet:
            print(summary)
    else:
        msg = f"⚠️ {market_name} KTR {tf_label} 처리 시 데이터를 찾지 못했습니다."
        log_cronjob(job_label, msg, is_error=True)
        if not quiet:
            print(msg)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="KTR 측정 및 DB 저장·텔레그램 알림. 타임프레임을 5M, 10M, 1H 중 지정.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="예: python ktr_measure_calculator.py 5M   또는   python ktr_measure_calculator.py -t 10M",
    )
    parser.add_argument(
        "timeframe",
        nargs="?",
        choices=["5M", "5m", "10M", "10m", "1H", "1h"],
        help="타임프레임: 5M, 10M, 1H",
    )
    parser.add_argument(
        "-t", "--tf",
        dest="tf_flag",
        choices=["5M", "5m", "10M", "10m", "1H", "1h"],
        help="타임프레임 (예: -t 10M)",
    )
    args = parser.parse_args()

    tf = args.tf_flag or args.timeframe
    if not tf:
        parser.error("타임프레임을 지정하세요: 5M, 10M 또는 1H (예: python ktr_measure_calculator.py 10M)")

    tf = tf.upper()
    if tf == "5M":
        run_5m()
    elif tf == "10M":
        run_10m()
    else:
        run_1h()


if __name__ == "__main__":
    main()
