# -*- coding: utf-8 -*-
"""
포지션 모니터용 DB: 심볼별 5/10분·1시간봉 캔들 + BB(20,2) 상·하단, SMA20, SMA120 저장.
v2 독립 실행용.
"""
import os
import sqlite3
import logging
from datetime import date, datetime, timedelta
from typing import Any, List, Optional, Tuple

import pytz

_log = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    from db_config import UNIFIED_DB_PATH
    DB_PATH = UNIFIED_DB_PATH
except ImportError:
    DB_PATH = os.path.join(_SCRIPT_DIR, "scheduler.db")
KST = pytz.timezone("Asia/Seoul")

try:
    from mt5_time_utils import MT5_SESSION_OFFSET_SEC
except ImportError:
    MT5_SESSION_OFFSET_SEC = 0


class _RatesLike:
    def __init__(self, time: List[int], open_: List[float], high: List[float], low: List[float], close: List[float]):
        self._time = time
        self._open = open_
        self._high = high
        self._low = low
        self._close = close

    def __len__(self) -> int:
        return len(self._time)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, slice):
            return _RatesLike(
                self._time[key],
                self._open[key],
                self._high[key],
                self._low[key],
                self._close[key],
            )
        if key == "time":
            return self._time
        if key == "open":
            return self._open
        if key == "high":
            return self._high
        if key == "low":
            return self._low
        if key == "close":
            return self._close
        raise KeyError(key)

BB_PERIOD, BB_STD = 20, 2   # 20/2 밴드: 종가(Close) 기준
BB4_PERIOD, BB4_STD = 4, 4   # 4/4 밴드: 시가(Open) 기준
SMA20_PERIOD = 20
SMA120_PERIOD = 120


def _sma(closes: List[float], period: int) -> Optional[float]:
    if len(closes) < period or period <= 0:
        return None
    return sum(closes[-period:]) / period


def _bollinger_upper(closes: List[float], period: int, num_std: float) -> Optional[float]:
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    var = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = (var ** 0.5) if var > 0 else 0.0
    return mid + num_std * std


def _bollinger_lower(closes: List[float], period: int, num_std: float) -> Optional[float]:
    if len(closes) < period or period <= 1:
        return None
    use = closes[-period:]
    mid = sum(use) / period
    var = sum((x - mid) ** 2 for x in use) / (period - 1)
    std = (var ** 0.5) if var > 0 else 0.0
    return mid - num_std * std


def _round2(x: Optional[float]) -> Optional[float]:
    return round(x, 2) if x is not None else None


def bar_time_string_for_latest(bar_dt: datetime, timeframe: str) -> str:
    """update_latest_bar에 저장되는 bar_time 문자열과 동일한 형식 반환 (존재 여부 검사용)."""
    return _to_bar_start_time(bar_dt, timeframe).strftime("%Y-%m-%d %H:%M:%S")


def bar_exists(conn: sqlite3.Connection, symbol: str, timeframe: str, bar_time: str) -> bool:
    """해당 (symbol, timeframe, bar_time) 레코드가 bars 테이블에 있으면 True. Supabase 우선 조회."""
    try:
        from supabase_sync import bar_exists_supabase, SUPABASE_SYNC_ENABLED
        if SUPABASE_SYNC_ENABLED:
            if bar_exists_supabase(symbol, timeframe, bar_time):
                return True
    except Exception:
        pass
    cur = conn.execute(
        "SELECT 1 FROM bars WHERE symbol = ? AND timeframe = ? AND bar_time = ? LIMIT 1",
        (symbol, timeframe, bar_time),
    )
    return cur.fetchone() is not None


def _to_bar_start_time(bar_dt: datetime, timeframe: str) -> datetime:
    """MT5 봉 시각을 봉 시작 시각으로 정규화. MT5는 봉 시작 시각을 주므로 M5/M10은 그대로 5·10분 경계로 내림."""
    if timeframe == "M5":
        return bar_dt.replace(second=0, microsecond=0, minute=(bar_dt.minute // 5) * 5)
    if timeframe == "M10":
        return bar_dt.replace(second=0, microsecond=0, minute=(bar_dt.minute // 10) * 10)
    if timeframe == "H1":
        # MT5 1H 봉 시각이 봉 시작(11:00 등)이면 그대로 사용; 봉 끝은 _update_candle_db에서 이미 11:00으로 정규화됨
        return bar_dt.replace(minute=0, second=0, microsecond=0)
    if timeframe == "H2":
        bar_start = bar_dt - timedelta(hours=2)
        return bar_start.replace(minute=0, second=0, microsecond=0, hour=(bar_start.hour // 2) * 2)
    if timeframe == "H4":
        bar_start = bar_dt - timedelta(hours=4)
        return bar_start.replace(minute=0, second=0, microsecond=0, hour=(bar_start.hour // 4) * 4)
    bar_start = bar_dt - timedelta(hours=1)
    return bar_start.replace(minute=0, second=0, microsecond=0)


def get_connection(db_path: Optional[str] = None):
    path = db_path or DB_PATH
    path = os.path.abspath(path)
    conn = sqlite3.connect(path)
    create_tables(conn)
    return conn


def _ensure_bb4_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(bars)")
    names = [row[1] for row in cur.fetchall()]
    if "bb4_upper" not in names:
        conn.execute("ALTER TABLE bars ADD COLUMN bb4_upper REAL")
    if "bb4_lower" not in names:
        conn.execute("ALTER TABLE bars ADD COLUMN bb4_lower REAL")
    conn.commit()


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bars (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bar_time TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            bb20_upper REAL,
            bb20_lower REAL,
            bb4_upper REAL,
            bb4_lower REAL,
            sma20 REAL,
            sma120 REAL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, timeframe, bar_time)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bars_symbol_tf ON bars(symbol, timeframe)")
    # 세션당 1레코드: (symbol, session, session_date) 기준으로 high/low만 갱신
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='session_high_low'")
    if cur.fetchone():
        cur = conn.execute("PRAGMA table_info(session_high_low)")
        cols = [row[1] for row in cur.fetchall()]
        if "session_date" not in cols:
            conn.execute("DROP TABLE session_high_low")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS session_high_low (
            symbol TEXT NOT NULL,
            session TEXT NOT NULL,
            session_date TEXT NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, session, session_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_session_high_low_symbol_session ON session_high_low(symbol, session)")
    conn.commit()
    _ensure_bb4_columns(conn)


def _session_from_bar_hour_kst(hour: int) -> str:
    """1시간봉 bar_time의 KST 시각(시간)으로 세션 반환. Asia/Europe/US."""
    if 0 <= hour <= 7:
        return "US"
    if 8 <= hour <= 17:
        return "Asia"
    return "Europe"


def upsert_session_high_low(
    conn: sqlite3.Connection,
    symbol: str,
    bar_time: str,
    high: float,
    low: float,
    sync_to_supabase: bool = True,
) -> None:
    """세션당 1레코드만 유지. Supabase(주 DB)에 먼저 반영, 성공 시 로컬 백업. sync_to_supabase=False면 Supabase 전송 생략(대량 보충용)."""
    try:
        dt = datetime.strptime(bar_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(bar_time, "%Y-%m-%d %H:%M")
        except ValueError:
            return
    session = _session_from_bar_hour_kst(dt.hour)
    session_date = dt.strftime("%Y-%m-%d")
    now_iso = datetime.now().isoformat()
    high, low = round(high, 2), round(low, 2)
    cur = conn.execute(
        "SELECT high, low FROM session_high_low WHERE symbol = ? AND session = ? AND session_date = ?",
        (symbol, session, session_date),
    )
    row = cur.fetchone()
    if row:
        high = max(row[0], high)
        low = min(row[1], low)
    # Supabase(주 DB)에 먼저 반영
    if sync_to_supabase:
        try:
            from supabase_sync import sync_session_high_low, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED and sync_session_high_low:
                ok = sync_session_high_low(symbol, session, session_date, high, low, now_iso)
                if not ok:
                    _log.warning("Supabase 반영 실패, 로컬 백업만 저장: session_high_low (%s %s %s)", symbol, session, session_date)
        except Exception as e:
            _log.debug("Supabase 반영 스킵(비활성 또는 오류): %s", e)
    # 로컬 백업 저장
    conn.execute(
        """INSERT OR REPLACE INTO session_high_low (symbol, session, session_date, high, low, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (symbol, session, session_date, high, low, now_iso),
    )
    conn.commit()


# 세션별 KST 시간 구간 (시, 포함). US 0-7, Asia 8-17, Europe 18-23
_SESSION_HOURS = {"US": (0, 7), "Asia": (8, 17), "Europe": (18, 23)}


def get_past_4_sessions_kst(now_kst: datetime) -> List[Tuple[date, str]]:
    """현재 KST 기준 과거 4개 세션 (가장 최근 순). 예: 지금 유럽이면 [오늘 아시아, 어제 미국, 어제 유럽, 어제 아시아]."""
    hour = now_kst.hour
    today = now_kst.date()
    yesterday = today - timedelta(days=1)
    if 8 <= hour <= 17:
        current = "Asia"
    elif 18 <= hour <= 23:
        current = "Europe"
    else:
        current = "US"
    if current == "Europe":
        return [(today, "Asia"), (yesterday, "US"), (yesterday, "Europe"), (yesterday, "Asia")]
    if current == "Asia":
        return [(today, "US"), (yesterday, "Europe"), (yesterday, "Asia"), (yesterday, "US")]
    # US (0-7)
    return [(yesterday, "Europe"), (yesterday, "Asia"), (yesterday, "US"), (yesterday - timedelta(days=1), "Europe")]


def get_session_high_low_from_bars(
    conn: sqlite3.Connection,
    symbol: str,
    session_date: date,
    session: str,
) -> Optional[Tuple[float, float]]:
    """bars 테이블 H1 봉에서 해당 세션 구간의 high/low 집계. 반환 (max_high, min_low) 또는 None."""
    d = session_date.strftime("%Y-%m-%d")
    h_start, h_end = _SESSION_HOURS[session]
    bar_start = f"{d} {h_start:02d}:00:00"
    bar_end = f"{d} {h_end:02d}:00:00"
    cur = conn.execute(
        """SELECT high, low FROM bars
           WHERE symbol = ? AND timeframe = 'H1' AND bar_time >= ? AND bar_time <= ?""",
        (symbol, bar_start, bar_end),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    highs = [r[0] for r in rows if r[0] is not None]
    lows = [r[1] for r in rows if r[1] is not None]
    if not highs or not lows:
        return None
    return (max(highs), min(lows))


def update_past_4_sessions_high_low(
    conn: sqlite3.Connection,
    symbols: List[str],
) -> List[str]:
    """과거 4개 세션에 대해 bars에서 high/low를 집계해 session_high_low 갱신. 로그 라인 목록 반환."""
    now_kst = datetime.now(KST)
    sessions = get_past_4_sessions_kst(now_kst)
    log_lines = [f"[High/Low] 현재 세션 기준 과거 4세션 갱신: {[(d.strftime('%Y-%m-%d'), s) for d, s in sessions]}"]

    for symbol in symbols:
        for session_date, session in sessions:
            result = get_session_high_low_from_bars(conn, symbol, session_date, session)
            if result is None:
                log_lines.append(f"  {symbol} {session_date} {session}: 봉 없음")
                continue
            high, low = result
            bar_time_repr = f"{session_date.strftime('%Y-%m-%d')} {_SESSION_HOURS[session][0]:02d}:00:00"
            upsert_session_high_low(conn, symbol, bar_time_repr, high, low)
            log_lines.append(f"  {symbol} {session_date} {session}: H={high:.2f} L={low:.2f}")
    return log_lines


def get_min_low_past_4_sessions(conn: sqlite3.Connection, symbol: str) -> Optional[float]:
    """과거 4개 세션의 session_high_low에서 해당 심볼의 최저가(min low) 반환. Supabase 우선 조회."""
    now_kst = datetime.now(KST)
    sessions = get_past_4_sessions_kst(now_kst)
    raw = (symbol or "").strip()
    candidates = [raw, raw.rstrip("+")] if raw.endswith("+") else [raw, raw + "+"]
    lows = []
    try:
        from supabase_sync import get_session_high_low_supabase, SUPABASE_SYNC_ENABLED
        if SUPABASE_SYNC_ENABLED and get_session_high_low_supabase:
            for sym in candidates:
                if not sym:
                    continue
                for session_date, session in sessions:
                    d = session_date.strftime("%Y-%m-%d")
                    row = get_session_high_low_supabase(sym, session, d)
                    if row is not None:
                        try:
                            lows.append(float(row[1]))
                        except (TypeError, ValueError):
                            pass
                if lows:
                    break
            if lows:
                return min(lows)
    except Exception:
        pass
    lows = []
    for sym in candidates:
        if not sym:
            continue
        for session_date, session in sessions:
            d = session_date.strftime("%Y-%m-%d")
            cur = conn.execute(
                "SELECT low FROM session_high_low WHERE symbol = ? AND session = ? AND session_date = ?",
                (sym, session, d),
            )
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                try:
                    lows.append(float(row[0]))
                except (TypeError, ValueError):
                    pass
        if lows:
            break
    return min(lows) if lows else None


def delete_bars_last_24h(conn: sqlite3.Connection) -> int:
    """과거 24시간 구간의 bar 레코드를 삭제. 반환: 삭제된 행 수."""
    cutoff = (datetime.now(KST) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute("DELETE FROM bars WHERE bar_time >= ?", (cutoff,))
    conn.commit()
    return cur.rowcount


def update_bars(conn: sqlite3.Connection, symbol: str, timeframe: str, rates: Any, mt5_ts_to_kst, sync_to_supabase: bool = True) -> int:
    """Supabase(주 DB)에 먼저 반영, 성공 여부와 관계없이 로컬 백업 저장. sync_to_supabase=False면 로컬만."""
    _ensure_bb4_columns(conn)
    if rates is None or len(rates) < BB_PERIOD:
        return 0
    if len(rates) > 1 and int(rates["time"][0]) < int(rates["time"][-1]):
        rates = rates[::-1].copy()
    now_iso = datetime.now().isoformat()
    inserted = 0
    rows_to_sync: List[dict] = []
    n = len(rates)
    for i in range(0, n - BB_PERIOD + 1):
        bar_ts = int(rates["time"][i])
        bar_dt = mt5_ts_to_kst(bar_ts)
        bar_start_dt = _to_bar_start_time(bar_dt, timeframe)
        bar_time = bar_start_dt.strftime("%Y-%m-%d %H:%M:%S")
        o, h, l_, c = _round2(float(rates["open"][i])), _round2(float(rates["high"][i])), _round2(float(rates["low"][i])), _round2(float(rates["close"][i]))
        closes_20 = [float(rates["close"][j]) for j in range(i, i + BB_PERIOD)]
        bb_up, bb_lo = _round2(_bollinger_upper(closes_20, BB_PERIOD, BB_STD)), _round2(_bollinger_lower(closes_20, BB_PERIOD, BB_STD))
        bb4_up = bb4_lo = None
        if i + BB4_PERIOD <= n:
            opens_4 = [float(rates["open"][j]) for j in range(i, i + BB4_PERIOD)]
            bb4_up, bb4_lo = _round2(_bollinger_upper(opens_4, BB4_PERIOD, BB4_STD)), _round2(_bollinger_lower(opens_4, BB4_PERIOD, BB4_STD))
        sma20 = _round2(_sma(closes_20, SMA20_PERIOD))
        sma120 = _round2(_sma([float(rates["close"][j]) for j in range(i, min(i + SMA120_PERIOD, n))], SMA120_PERIOD)) if i + SMA120_PERIOD <= n else None
        conn.execute(
            """INSERT OR REPLACE INTO bars (symbol, timeframe, bar_time, open, high, low, close, bb20_upper, bb20_lower, bb4_upper, bb4_lower, sma20, sma120, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, timeframe, bar_time, o, h, l_, c, bb_up, bb_lo, bb4_up, bb4_lo, sma20, sma120, now_iso),
        )
        rows_to_sync.append({
            "symbol": symbol, "timeframe": timeframe, "bar_time": bar_time,
            "open": o, "high": h, "low": l_, "close": c,
            "bb20_upper": bb_up, "bb20_lower": bb_lo, "bb4_upper": bb4_up, "bb4_lower": bb4_lo,
            "sma20": sma20, "sma120": sma120, "updated_at": now_iso,
        })
        inserted += 1
    # Supabase(주 DB)에 먼저 반영
    if rows_to_sync and sync_to_supabase:
        try:
            from supabase_sync import sync_bars, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED and sync_bars:
                ok = sync_bars(rows_to_sync)
                if not ok:
                    _log.warning("Supabase 반영 실패, 로컬 백업만 저장: bars %d건 (%s %s)", len(rows_to_sync), symbol, timeframe)
        except Exception as e:
            _log.debug("Supabase 반영 스킵(비활성 또는 오류): %s", e)
    conn.commit()
    return inserted


def update_latest_bar(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    rates: Any,
    mt5_ts_to_kst,
    bar_index: Optional[int] = None,
    bar_time_override: Optional[str] = None,
) -> int:
    """bar_time_override가 주어지면 해당 문자열을 bar_time으로 저장(존재 검사와 동일한 봉 식별자 보장)."""
    _ensure_bb4_columns(conn)
    if rates is None or len(rates) < BB_PERIOD:
        return 0
    if hasattr(rates, "copy"):
        rates = rates.copy()
    if bar_index is not None:
        i = bar_index
    else:
        if len(rates) > 1 and int(rates["time"][0]) < int(rates["time"][-1]):
            rates = rates[::-1].copy() if hasattr(rates, "copy") else rates[::-1]
        i = 0
    if i < 0 or i >= len(rates) or i + BB_PERIOD > len(rates):
        return 0
    n = len(rates)
    if bar_time_override:
        bar_time = bar_time_override
    else:
        bar_ts = int(rates["time"][i])
        bar_dt = mt5_ts_to_kst(bar_ts)
        bar_start_dt = _to_bar_start_time(bar_dt, timeframe)
        bar_time = bar_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    o, h, l_, c = _round2(float(rates["open"][i])), _round2(float(rates["high"][i])), _round2(float(rates["low"][i])), _round2(float(rates["close"][i]))
    closes_20 = [float(rates["close"][j]) for j in range(i, i + BB_PERIOD)]
    bb_up, bb_lo = _round2(_bollinger_upper(closes_20, BB_PERIOD, BB_STD)), _round2(_bollinger_lower(closes_20, BB_PERIOD, BB_STD))
    bb4_up = bb4_lo = None
    if i + BB4_PERIOD <= n:
        opens_4 = [float(rates["open"][j]) for j in range(i, i + BB4_PERIOD)]
        bb4_up, bb4_lo = _round2(_bollinger_upper(opens_4, BB4_PERIOD, BB4_STD)), _round2(_bollinger_lower(opens_4, BB4_PERIOD, BB4_STD))
    sma20 = _round2(_sma(closes_20, SMA20_PERIOD))
    sma120 = _round2(_sma([float(rates["close"][j]) for j in range(i, min(i + SMA120_PERIOD, n))], SMA120_PERIOD)) if i + SMA120_PERIOD <= n else None
    now_iso = datetime.now().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO bars (symbol, timeframe, bar_time, open, high, low, close, bb20_upper, bb20_lower, bb4_upper, bb4_lower, sma20, sma120, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (symbol, timeframe, bar_time, o, h, l_, c, bb_up, bb_lo, bb4_up, bb4_lo, sma20, sma120, now_iso),
    )
    # Supabase(주 DB)에 먼저 반영
    try:
        from supabase_sync import sync_bar_one, SUPABASE_SYNC_ENABLED
        if SUPABASE_SYNC_ENABLED and sync_bar_one:
            ok = sync_bar_one(symbol, timeframe, bar_time, o, h, l_, c, bb_up, bb_lo, bb4_up, bb4_lo, sma20, sma120, now_iso)
            if not ok:
                _log.warning("Supabase 반영 실패, 로컬 백업만 저장: bars 1건 (%s %s %s)", symbol, timeframe, bar_time)
    except Exception as e:
        _log.debug("Supabase 반영 스킵(비활성 또는 오류): %s", e)
    conn.commit()
    return 1


def get_bars_from_db(symbol: str, timeframe: str, limit: int = 150) -> List[dict]:
    """Supabase 우선 조회, 실패/비활성 시 로컬 DB."""
    try:
        from supabase_sync import get_bars_supabase, SUPABASE_SYNC_ENABLED
        if SUPABASE_SYNC_ENABLED:
            rows = get_bars_supabase(symbol, timeframe, limit=limit)
            if rows is not None:
                return rows
    except Exception:
        pass
    conn = get_connection()
    cur = conn.execute(
        """SELECT bar_time, open, high, low, close, bb20_upper, bb20_lower, bb4_upper, bb4_lower, sma20, sma120
           FROM bars WHERE symbol = ? AND timeframe = ? ORDER BY bar_time DESC LIMIT ?""",
        (symbol, timeframe, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {"bar_time": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "bb20_upper": r[5], "bb20_lower": r[6], "bb4_upper": r[7], "bb4_lower": r[8], "sma20": r[9], "sma120": r[10]}
        for r in rows
    ]


def get_rates_from_db(symbol: str, timeframe: str, limit: int = 150) -> Optional[_RatesLike]:
    """DB 봉을 rates 형태로 반환. time_ts는 mt5_ts_to_kst()로 KST 표시가 맞도록 보정해 둠."""
    bars = get_bars_from_db(symbol, timeframe, limit=limit)
    if not bars:
        return None
    # bar_time은 KST. mt5_ts_to_kst(ts) = fromtimestamp(ts + MT5_SESSION_OFFSET_SEC, KST) 이므로
    # ts = unix_utc - MT5_SESSION_OFFSET_SEC 로 넣어야 직전 봉 표시 등에서 KST가 맞게 나옴.
    unix_ts_list = [int(datetime.strptime(b["bar_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).timestamp()) for b in bars]
    time_ts = [t - MT5_SESSION_OFFSET_SEC for t in unix_ts_list]
    return _RatesLike(time_ts, [b["open"] for b in bars], [b["high"] for b in bars], [b["low"] for b in bars], [b["close"] for b in bars])
