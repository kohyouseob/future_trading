# -*- coding: utf-8 -*-
"""
DB 정책: Supabase를 주 데이터베이스로 사용. 로컬 DB(scheduler.db)는 백업용.
- 읽기: Supabase 우선 조회, 실패/비활성 시 로컬 fallback.
- 쓰기: Supabase에 먼저 반영, 성공 시 로컬에도 백업 저장. Supabase 실패 시에도 로컬에 저장(데이터 보존).
- 'Supabase 동기화' 버튼: 과거 24시간 구간에서 로컬에만 있는 데이터를 Supabase로 보충할 때 사용.
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import pytz
    _KST = pytz.timezone("Asia/Seoul")
except Exception:
    _KST = None

try:
    from db_config import SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SYNC_ENABLED
except ImportError:
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
    SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
    SUPABASE_SYNC_ENABLED = bool(SUPABASE_URL and SUPABASE_ANON_KEY and SUPABASE_URL.startswith("http"))

_BASE = (SUPABASE_URL.rstrip("/") + "/rest/v1") if SUPABASE_URL else ""
_log = logging.getLogger(__name__)


def _headers(prefer: str = "resolution=merge-duplicates") -> Dict[str, str]:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _post_upsert(table: str, payload: Any, on_conflict: str) -> bool:
    """기존 동작: 충돌 시 병합(덮어쓰기)."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    count = len(payload) if isinstance(payload, list) else 1
    try:
        import requests
        url = f"{_BASE}/{table}"
        h = _headers("resolution=merge-duplicates")
        if isinstance(payload, list):
            resp = requests.post(url, json=payload, headers=h, params={"on_conflict": on_conflict}, timeout=15)
        else:
            resp = requests.post(url, json=payload, headers=h, params={"on_conflict": on_conflict}, timeout=15)
        if resp.status_code in (200, 201, 204):
            _log.info("Supabase 업데이트 성공: %s %d건", table, count)
            return True
        reason = (resp.text or resp.reason or str(resp.status_code))[:500]
        if resp.status_code == 404 or "PGRST205" in reason or "Could not find the table" in reason:
            _log.warning(
                "Supabase 테이블 없음(404): %s → 로컬만 사용. 테이블 생성: Supabase Dashboard → SQL Editor에서 supabase_migration_position_status_sent.sql 내용 실행.",
                table,
            )
        else:
            _log.warning("Supabase 업데이트 실패: %s - status=%s, 원인: %s", table, resp.status_code, reason)
        return False
    except Exception as e:
        _log.warning("Supabase 업데이트 실패: %s - 예외: %s", table, e)
        return False


def _get_existing_keys_supabase(
    table: str,
    key_columns: List[str],
    log_fn: Optional[Callable[[str], None]] = None,
    filter_params: Optional[Dict[str, str]] = None,
) -> Optional[set]:
    """Supabase 테이블에 이미 있는 행의 자연키 집합 조회. filter_params 있으면 해당 기간만 조회(예: created_at=gte.2025-03-02T00:00:00+09:00)."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return None
    try:
        import requests
        url = f"{_BASE}/{table}"
        select = ",".join(key_columns)
        params = {"select": select}
        if filter_params:
            params.update(filter_params)
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        existing: set = set()
        step = 1000
        start = 0
        while True:
            headers_get = {**headers, "Range": f"{start}-{start + step - 1}"}
            resp = requests.get(url, headers=headers_get, params=params, timeout=30)
            if resp.status_code not in (200, 206):
                if log_fn:
                    log_fn(f"    Supabase 기존 키 조회 실패: {table} status={resp.status_code}")
                return None
            data = resp.json()
            if not data:
                break
            for row in data:
                key = tuple(row.get(c) for c in key_columns)
                existing.add(key)
            if len(data) < step:
                break
            start += step
        return existing
    except Exception as e:
        _log.warning("Supabase 기존 키 조회 예외: %s - %s", table, e)
        if log_fn:
            log_fn(f"    Supabase 기존 키 조회 예외: {table} - {e}")
        return None


def _post_insert_ignore_duplicates(table: str, payload: Any, on_conflict: str) -> Tuple[bool, str]:
    """Supabase에 없는 레코드만 삽입. 이미 있는 키는 무시. 반환: (성공 여부, 실패 시 원인 문자열)."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False, "동기화 비활성"
    count = len(payload) if isinstance(payload, list) else 1
    try:
        import requests
        url = f"{_BASE}/{table}"
        h = _headers("resolution=ignore-duplicates")
        if isinstance(payload, list):
            resp = requests.post(url, json=payload, headers=h, params={"on_conflict": on_conflict}, timeout=60)
        else:
            resp = requests.post(url, json=payload, headers=h, params={"on_conflict": on_conflict}, timeout=60)
        if resp.status_code in (200, 201, 204):
            _log.info("Supabase 삽입(중복무시) 성공: %s %d건", table, count)
            return True, ""
        reason = (resp.text or resp.reason or str(resp.status_code))[:800]
        _log.warning("Supabase 삽입 실패: %s - status=%s, 원인: %s", table, resp.status_code, reason)
        return False, f"HTTP {resp.status_code}: {reason}"
    except Exception as e:
        _log.warning("Supabase 삽입 실패: %s - 예외: %s", table, e)
        return False, str(e)


def _get_supabase(
    table: str,
    select_cols: str,
    filters: Optional[Dict[str, str]] = None,
    order: Optional[str] = None,
    limit: int = 1000,
) -> Optional[List[Dict[str, Any]]]:
    """Supabase GET. filters 예: {"symbol": "eq.XAUUSD+", "timeframe": "eq.H1"}. order 예: "bar_time.desc". 실패 시 None."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return None
    try:
        import requests
        url = f"{_BASE}/{table}"
        params = {"select": select_cols}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        params["limit"] = str(min(limit, 2000))
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        if resp.status_code not in (200, 206):
            return None
        return resp.json()
    except Exception as e:
        _log.debug("Supabase GET 실패: %s - %s", table, e)
        return None


# ----- 읽기 API (Supabase 기본, 호출처에서 실패 시 로컬 fallback) -----

def get_bars_supabase(symbol: str, timeframe: str, limit: int = 150) -> Optional[List[Dict[str, Any]]]:
    """bars 테이블 조회. bar_time 내림차순. 반환 형식: [{"bar_time":..., "open":..., ...}]. 실패/비활성 시 None."""
    data = _get_supabase(
        "bars",
        "bar_time,open,high,low,close,bb20_upper,bb20_lower,bb4_upper,bb4_lower,sma20,sma120",
        filters={"symbol": f"eq.{symbol}", "timeframe": f"eq.{timeframe}"},
        order="bar_time.desc",
        limit=limit,
    )
    if not data:
        return None
    return data


def bar_exists_supabase(symbol: str, timeframe: str, bar_time: str) -> bool:
    """해당 (symbol, timeframe, bar_time) 행이 Supabase bars에 있으면 True."""
    data = _get_supabase(
        "bars",
        "bar_time",
        filters={"symbol": f"eq.{symbol}", "timeframe": f"eq.{timeframe}", "bar_time": f"eq.{bar_time}"},
        limit=1,
    )
    return bool(data)


def get_ktr_records_supabase(limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """ktr_records 조회. created_at 내림차순. 반환 키: symbol, session, timeframe, record_date, ktr_value, balance, lot_1st, lot_2nd, lot_3rd, created_at. id는 Supabase 자동."""
    data = _get_supabase(
        "ktr_records",
        "symbol,session,timeframe,record_date,ktr_value,balance,lot_1st,lot_2nd,lot_3rd,created_at",
        order="created_at.desc",
        limit=limit,
    )
    return data


def has_ktr_for_session_timeframe_date_supabase(session: str, timeframe: str, record_date: str) -> bool:
    """해당 세션·타임프레임·측정일에 레코드가 1건이라도 있으면 True."""
    if not record_date or not isinstance(record_date, str):
        return False
    rd = record_date.strip()[:10]
    data = _get_supabase(
        "ktr_records",
        "symbol",
        filters={"session": f"eq.{session}", "timeframe": f"eq.{timeframe}", "record_date": f"eq.{rd}"},
        limit=1,
    )
    return bool(data)


def has_both_ktr_symbols_for_slot_supabase(session: str, timeframe: str, record_date: str) -> bool:
    """해당 (세션, 타임프레임, 측정일)에 NAS100·XAUUSD 두 심볼 모두 있으면 True. 누락 판정용."""
    if not record_date or not isinstance(record_date, str):
        return False
    rd = record_date.strip()[:10]
    data = _get_supabase(
        "ktr_records",
        "symbol",
        filters={"session": f"eq.{session}", "timeframe": f"eq.{timeframe}", "record_date": f"eq.{rd}"},
        limit=10,
    )
    if not data:
        return False
    found = {r.get("symbol") for r in data if r.get("symbol")}
    return "NAS100" in found and "XAUUSD" in found


def get_latest_ktr_supabase(symbol: str, session: str, timeframe: str) -> Optional[float]:
    """해당 symbol, session, timeframe의 최신 ktr_value. 없으면 None."""
    data = _get_supabase(
        "ktr_records",
        "ktr_value",
        filters={"symbol": f"eq.{symbol}", "session": f"eq.{session}", "timeframe": f"eq.{timeframe}"},
        order="created_at.desc",
        limit=1,
    )
    if not data or data[0].get("ktr_value") is None:
        return None
    try:
        return float(data[0]["ktr_value"])
    except (TypeError, ValueError):
        return None


def get_most_recent_ktr_supabase(symbol: str, timeframe: str) -> Tuple[float, Optional[str]]:
    """(ktr_value, session). 없으면 (0.0, None)."""
    data = _get_supabase(
        "ktr_records",
        "ktr_value,session",
        filters={"symbol": f"eq.{symbol}", "timeframe": f"eq.{timeframe}"},
        order="created_at.desc",
        limit=1,
    )
    if not data:
        return 0.0, None
    row = data[0]
    try:
        val = float(row["ktr_value"]) if row.get("ktr_value") is not None else 0.0
    except (TypeError, ValueError):
        val = 0.0
    return val, row.get("session")


def get_most_recent_session_supabase(symbol: str, timeframe: str) -> Optional[str]:
    """해당 symbol, timeframe의 최신 session. 없으면 None."""
    data = _get_supabase(
        "ktr_records",
        "session",
        filters={"symbol": f"eq.{symbol}", "timeframe": f"eq.{timeframe}"},
        order="created_at.desc",
        limit=1,
    )
    if not data:
        return None
    return data[0].get("session")


def get_session_high_low_supabase(symbol: str, session: str, session_date: str) -> Optional[Tuple[float, float]]:
    """(high, low) 또는 None."""
    data = _get_supabase(
        "session_high_low",
        "high,low",
        filters={"symbol": f"eq.{symbol}", "session": f"eq.{session}", "session_date": f"eq.{session_date}"},
        limit=1,
    )
    if not data or data[0].get("high") is None or data[0].get("low") is None:
        return None
    try:
        return float(data[0]["high"]), float(data[0]["low"])
    except (TypeError, ValueError):
        return None


# ----- 돌파더블비 예약 (breakout_order_gui) -----

def get_breakout_reservations_supabase() -> Optional[List[Dict[str, Any]]]:
    """breakout_reservations 테이블 전체 조회. created_at 오름차순. 실패/비활성 시 None.
    ※ side 컬럼은 요청하지 않음(테이블에 없으면 조회 실패하므로). 로드 시 side 없으면 '매수'로 처리.
    테이블에 side 컬럼이 있으면 여기 select에 'side' 추가하면 됨."""
    data = _get_supabase(
        "breakout_reservations",
        "id,symbol,tfs,weight_pct,tp_enabled,tp_ktr,tp_x2",
        order="id.asc",
        limit=500,
    )
    return data


def insert_breakout_reservation_supabase(
    symbol: str,
    tfs_str: str,
    weight_pct: float,
    tp_enabled: bool,
    tp_ktr: str,
    tp_x2: bool,
    side: str = "매수",
) -> Optional[int]:
    """breakout_reservations 1건 삽입. 반환: 생성된 id, 실패 시 None. side: '매수' | '매도'.
    테이블에 side 컬럼이 없으면 side 없이 재시도하여 기존 스키마에서도 저장 가능."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return None
    try:
        import requests
        url = f"{_BASE}/breakout_reservations"
        payload_with_side = {
            "symbol": symbol,
            "tfs": tfs_str,
            "weight_pct": weight_pct,
            "tp_enabled": tp_enabled,
            "tp_ktr": tp_ktr or "1",
            "tp_x2": bool(tp_x2),
            "side": (side or "매수").strip() or "매수",
        }
        payload_without_side = {
            "symbol": symbol,
            "tfs": tfs_str,
            "weight_pct": weight_pct,
            "tp_enabled": tp_enabled,
            "tp_ktr": tp_ktr or "1",
            "tp_x2": bool(tp_x2),
        }
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        resp = requests.post(url, json=payload_with_side, headers=headers, timeout=15)
        if resp.status_code not in (200, 201) and resp.status_code >= 400:
            resp = requests.post(url, json=payload_without_side, headers=headers, timeout=15)
        if resp.status_code not in (200, 201):
            return None
        out = resp.json()
        row = out[0] if isinstance(out, list) and out else (out if isinstance(out, dict) else None)
        if row and "id" in row:
            return int(row["id"])
        return None
    except Exception as e:
        _log.warning("Supabase breakout_reservations 삽입 실패: %s", e)
        return None


def delete_breakout_reservation_supabase(reservation_id: int) -> bool:
    """breakout_reservations에서 id로 1건 삭제."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    try:
        import requests
        url = f"{_BASE}/breakout_reservations"
        params = {"id": f"eq.{reservation_id}"}
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        resp = requests.delete(url, headers=headers, params=params, timeout=10)
        return resp.status_code in (200, 204)
    except Exception as e:
        _log.warning("Supabase breakout_reservations 삭제 실패: %s", e)
        return False


def sync_ktr_record(
    symbol: str,
    session: str,
    timeframe: str,
    record_date: str,
    ktr_value: float,
    balance: Optional[float] = None,
    lot_1st: Optional[float] = None,
    lot_2nd: Optional[float] = None,
    lot_3rd: Optional[float] = None,
    created_at: Optional[str] = None,
) -> bool:
    """KTR 레코드 1건 Supabase upsert. 로컬 commit 후 호출."""
    if not SUPABASE_SYNC_ENABLED:
        return False
    payload = {
        "symbol": symbol,
        "session": session,
        "timeframe": timeframe,
        "record_date": record_date,
        "ktr_value": ktr_value,
        "balance": balance,
        "lot_1st": lot_1st,
        "lot_2nd": lot_2nd,
        "lot_3rd": lot_3rd,
        "created_at": created_at,
    }
    return _post_upsert("ktr_records", payload, "symbol,session,timeframe,record_date")


def sync_ktr_delete_by_natural_key(symbol: str, session: str, timeframe: str, record_date: str) -> bool:
    """Supabase에서 (symbol, session, timeframe, record_date) 일치 행 삭제. 로컬 delete_by_id 후 필요 시 호출."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    try:
        import requests
        url = f"{_BASE}/ktr_records"
        params = {"symbol": f"eq.{symbol}", "session": f"eq.{session}", "timeframe": f"eq.{timeframe}", "record_date": f"eq.{record_date}"}
        resp = requests.delete(url, headers={"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}, params=params, timeout=10)
        if resp.status_code in (200, 204):
            _log.info("Supabase 삭제 성공: ktr_records 1건 (%s %s %s %s)", symbol, session, timeframe, record_date)
            return True
        reason = (resp.text or resp.reason or str(resp.status_code))[:500]
        _log.warning("Supabase 삭제 실패: ktr_records - status=%s, 원인: %s", resp.status_code, reason)
        return False
    except Exception as e:
        _log.warning("Supabase 삭제 실패: ktr_records - 예외: %s", e)
        return False


def sync_bars(rows: List[Dict[str, Any]]) -> bool:
    """bars 테이블 여러 행 upsert. 로컬 update_bars/update_latest_bar commit 후 호출."""
    if not SUPABASE_SYNC_ENABLED or not rows:
        return False
    return _post_upsert("bars", rows, "symbol,timeframe,bar_time")


def sync_bar_one(
    symbol: str,
    timeframe: str,
    bar_time: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    bb20_upper: Optional[float] = None,
    bb20_lower: Optional[float] = None,
    bb4_upper: Optional[float] = None,
    bb4_lower: Optional[float] = None,
    sma20: Optional[float] = None,
    sma120: Optional[float] = None,
    updated_at: Optional[str] = None,
) -> bool:
    """bars 1건 Supabase upsert."""
    if not SUPABASE_SYNC_ENABLED:
        return False
    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_time": bar_time,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "bb20_upper": bb20_upper,
        "bb20_lower": bb20_lower,
        "bb4_upper": bb4_upper,
        "bb4_lower": bb4_lower,
        "sma20": sma20,
        "sma120": sma120,
        "updated_at": updated_at,
    }
    return _post_upsert("bars", payload, "symbol,timeframe,bar_time")


def sync_session_high_low(
    symbol: str,
    session: str,
    session_date: str,
    high: float,
    low: float,
    updated_at: Optional[str] = None,
) -> bool:
    """session_high_low 1건 Supabase upsert."""
    if not SUPABASE_SYNC_ENABLED:
        return False
    payload = {
        "symbol": symbol,
        "session": session,
        "session_date": session_date,
        "high": high,
        "low": low,
        "updated_at": updated_at,
    }
    return _post_upsert("session_high_low", payload, "symbol,session,session_date")


def telegram_bar_sent_exists_supabase(tf_label: str, bar_key: str) -> bool:
    """Supabase telegram_bar_sent에 (tf_label, bar_key)가 이미 있으면 True."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    try:
        import requests
        url = f"{_BASE}/telegram_bar_sent"
        params = {"tf_label": f"eq.{tf_label}", "bar_key": f"eq.{bar_key}", "select": "tf_label"}
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code not in (200, 206):
            return False
        data = resp.json()
        return isinstance(data, list) and len(data) > 0
    except Exception:
        return False


def telegram_bar_sent_insert_supabase(tf_label: str, bar_key: str, sent_at: str) -> bool:
    """Supabase telegram_bar_sent에 1건 삽입. 성공 True, 중복 등 실패 False."""
    if not SUPABASE_SYNC_ENABLED:
        return False
    payload = {"tf_label": tf_label, "bar_key": bar_key, "sent_at": sent_at}
    return _post_upsert("telegram_bar_sent", payload, "tf_label,bar_key")


def telegram_bar_sent_delete_old_supabase(before_sent_at: str) -> bool:
    """Supabase telegram_bar_sent에서 sent_at < before_sent_at 인 행 삭제. 오늘 이전 정리용."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    try:
        import requests
        url = f"{_BASE}/telegram_bar_sent"
        params = {"sent_at": f"lt.{before_sent_at}"}
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        resp = requests.delete(url, headers=headers, params=params, timeout=15)
        if resp.status_code in (200, 204):
            return True
        return False
    except Exception:
        return False


def position_status_sent_exists_supabase(slot_name: str) -> bool:
    """Supabase position_status_sent에 slot_name이 이미 있으면 True."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    try:
        import requests
        url = f"{_BASE}/position_status_sent"
        params = {"slot_name": f"eq.{slot_name}", "select": "slot_name"}
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code not in (200, 206):
            return False
        data = resp.json()
        return isinstance(data, list) and len(data) > 0
    except Exception:
        return False


def position_status_sent_insert_supabase(slot_name: str, sent_at: str) -> bool:
    """Supabase position_status_sent에 1건 삽입. 성공 True, 중복 등 실패 False."""
    if not SUPABASE_SYNC_ENABLED:
        return False
    payload = {"slot_name": slot_name, "sent_at": sent_at}
    return _post_upsert("position_status_sent", payload, "slot_name")


def position_status_sent_delete_old_supabase(before_sent_at: str) -> bool:
    """Supabase position_status_sent에서 sent_at < before_sent_at 인 행 삭제. 2일 이전 정리용."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return False
    try:
        import requests
        url = f"{_BASE}/position_status_sent"
        params = {"sent_at": f"lt.{before_sent_at}"}
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        resp = requests.delete(url, headers=headers, params=params, timeout=15)
        return resp.status_code in (200, 204)
    except Exception:
        return False


def get_supabase_missing_counts(
    ktr_db_path: str,
    pm_db_path: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, int]]:
    """과거 24시간 구간에서 로컬에는 있지만 Supabase에는 없는 레코드 건수만 반환. 업로드 없음.
    반환: {"ktr_records": n, "bars": n, "session_high_low": n} 또는 실패/비활성 시 None."""
    if not SUPABASE_SYNC_ENABLED or not _BASE:
        return None
    def _log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        _log.info("%s", msg)
    try:
        now_kst = datetime.now(_KST) if _KST else datetime.now()
        cutoff_dt = now_kst - timedelta(hours=24)
        cutoff_24h_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
        cutoff_date_str = cutoff_dt.strftime("%Y-%m-%d")
        cutoff_24h_iso = cutoff_dt.isoformat() if hasattr(cutoff_dt, "isoformat") and (getattr(cutoff_dt, "tzinfo", None)) else (cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S") + "+09:00")

        counts: Dict[str, int] = {"ktr_records": 0, "bars": 0, "session_high_low": 0}

        # ktr_records
        ktr_keys = _get_existing_keys_supabase(
            "ktr_records", ["symbol", "session", "timeframe", "record_date"], log_fn,
            filter_params={"created_at": f"gte.{cutoff_24h_iso}"},
        )
        from ktr_db_utils import KTRDatabase
        ktr_db = KTRDatabase(db_name=ktr_db_path)
        all_ktr = ktr_db.get_recent_records(limit=10000)
        ktr_db.conn.close()
        ktr_rows = [r for r in all_ktr if (r.get("created_at") or "") >= cutoff_24h_str]
        key_cols = ["symbol", "session", "timeframe", "record_date"]
        for r in ktr_rows:
            key = tuple(r.get(c) for c in key_cols)
            if ktr_keys is None or key not in ktr_keys:
                counts["ktr_records"] += 1

        # bars
        import position_monitor_db as _pm
        bar_keys = _get_existing_keys_supabase(
            "bars", ["symbol", "timeframe", "bar_time"], log_fn,
            filter_params={"bar_time": f"gte.{cutoff_24h_str}"},
        )
        conn = _pm.get_connection(pm_db_path)
        cur = conn.execute(
            "SELECT symbol, timeframe, bar_time FROM bars WHERE bar_time >= ?",
            (cutoff_24h_str,),
        )
        bar_rows = cur.fetchall()
        conn.close()
        for r in bar_rows:
            key = (r[0], r[1], r[2])
            if bar_keys is None or key not in bar_keys:
                counts["bars"] += 1

        # session_high_low
        shl_keys = _get_existing_keys_supabase(
            "session_high_low", ["symbol", "session", "session_date"], log_fn,
            filter_params={"session_date": f"gte.{cutoff_date_str}"},
        )
        conn2 = _pm.get_connection(pm_db_path)
        cur2 = conn2.execute(
            "SELECT symbol, session, session_date FROM session_high_low WHERE session_date >= ?",
            (cutoff_date_str,),
        )
        shl_rows = cur2.fetchall()
        conn2.close()
        for r in shl_rows:
            if shl_keys is None or (r[0], r[1], r[2]) not in shl_keys:
                counts["session_high_low"] += 1

        return counts
    except Exception as e:
        _log.warning("Supabase 누락 건수 조회 실패: %s", e)
        if log_fn:
            log_fn(f"  Supabase 누락 건수 조회 실패: {e}")
        return None


def sync_all_from_local(
    ktr_db_path: str,
    pm_db_path: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Tuple[bool, str]:
    """로컬 DB(ktr_records, bars, session_high_low) 중 과거 24시간 데이터만 Supabase로 업로드. 반환: (성공 여부, 요약 메시지)."""
    def _log_msg(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        _log.info("%s", msg)

    if not SUPABASE_SYNC_ENABLED or not _BASE:
        out = "Supabase URL/KEY 미설정으로 동기화 비활성"
        _log_msg(out)
        return False, out
    # 과거 24시간 기준 시각 (KST)
    now_kst = datetime.now(_KST) if _KST else datetime.now()
    cutoff_dt = now_kst - timedelta(hours=24)
    cutoff_24h_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    cutoff_date_str = cutoff_dt.strftime("%Y-%m-%d")
    cutoff_24h_iso = cutoff_dt.isoformat() if hasattr(cutoff_dt, "isoformat") and (getattr(cutoff_dt, "tzinfo", None)) else (cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S") + "+09:00")
    _log_msg(f"  동기화 범위: 과거 24시간 (기준 ~ {cutoff_24h_str} 이후)")

    summary_lines: List[str] = []
    try:
        # 1) ktr_records — 과거 24시간만 조회, Supabase에 이미 있는 키 제외 후 빠진 것만 업로드
        _log_msg("  ktr_records Supabase 기존 키 조회 중(24h)...")
        ktr_keys = _get_existing_keys_supabase(
            "ktr_records", ["symbol", "session", "timeframe", "record_date"], log_fn,
            filter_params={"created_at": f"gte.{cutoff_24h_iso}"},
        )
        if ktr_keys is not None:
            _log_msg(f"  ktr_records Supabase 기존(24h) {len(ktr_keys)}건 확인.")
        _log_msg("  ktr_records 로컬 조회 중(과거 24시간)...")
        from ktr_db_utils import KTRDatabase
        ktr_db = KTRDatabase(db_name=ktr_db_path)
        all_ktr = ktr_db.get_recent_records(limit=10000)
        ktr_db.conn.close()
        ktr_rows = [r for r in all_ktr if (r.get("created_at") or "") >= cutoff_24h_str]
        n_ktr = len(ktr_rows)
        _log_msg(f"  ktr_records 로컬(24h) {n_ktr}건 조회됨.")
        if ktr_rows:
            # 빠진 것만: (symbol, session, timeframe, record_date)가 Supabase에 없는 행만
            key_cols = ["symbol", "session", "timeframe", "record_date"]
            missing_rows = []
            for r in ktr_rows:
                key = tuple(r.get(c) for c in key_cols)
                if ktr_keys is None or key not in ktr_keys:
                    missing_rows.append(r)
            if not missing_rows:
                summary_lines.append("ktr_records: 이미 동기화됨(빠진 건 0건)")
                _log_msg("  ktr_records: 이미 동기화됨, 업로드 스킵.")
            else:
                _log_msg(f"  ktr_records 빠진 건 {len(missing_rows)}건 → 업로드 대상.")
                payloads = []
                for r in missing_rows:
                    payloads.append({
                        "symbol": r.get("symbol"),
                        "session": r.get("session"),
                        "timeframe": r.get("timeframe"),
                        "record_date": r.get("record_date"),
                        "ktr_value": r.get("ktr_value"),
                        "balance": r.get("balance"),
                        "lot_1st": r.get("lot_1st"),
                        "lot_2nd": r.get("lot_2nd"),
                        "lot_3rd": r.get("lot_3rd"),
                    })
                KTR_BATCH = 50
                ktr_ok_count = 0
                ktr_fail_reason = ""
                for i in range(0, len(payloads), KTR_BATCH):
                    chunk = payloads[i : i + KTR_BATCH]
                    batch_no = i // KTR_BATCH + 1
                    total_batches = (len(payloads) + KTR_BATCH - 1) // KTR_BATCH
                    _log_msg(f"  ktr_records 배치 {batch_no}/{total_batches} 업로드 중 ({len(chunk)}건)...")
                    ok, err = _post_insert_ignore_duplicates("ktr_records", chunk, "symbol,session,timeframe,record_date")
                    if ok:
                        ktr_ok_count += len(chunk)
                    else:
                        ktr_fail_reason = err
                        _log_msg(f"  ktr_records 배치 {batch_no} 실패: {err}")
                if ktr_fail_reason and ktr_ok_count == 0:
                    summary_lines.append(f"ktr_records: 실패 — {ktr_fail_reason[:200]}")
                else:
                    summary_lines.append(f"ktr_records: 빠진 {len(payloads)}건 전송 — 성공 {ktr_ok_count}건" + (f", 실패: {ktr_fail_reason[:80]}" if ktr_fail_reason else ""))
                _log_msg(f"  ktr_records 업로드 완료: 성공 {ktr_ok_count}/{len(payloads)}건" + (f", 실패: {ktr_fail_reason}" if ktr_fail_reason else ""))
        else:
            summary_lines.append("ktr_records: 0건(스킵)")
            _log_msg("  ktr_records 0건 → 스킵.")

        # 2) bars — 과거 24시간만 조회, Supabase에 이미 있는 (symbol, timeframe, bar_time) 제외 후 빠진 것만 업로드
        _log_msg("  bars Supabase 기존 키 조회 중(24h)...")
        import position_monitor_db as _pm
        bar_keys = _get_existing_keys_supabase(
            "bars", ["symbol", "timeframe", "bar_time"], log_fn,
            filter_params={"bar_time": f"gte.{cutoff_24h_str}"},
        )
        if bar_keys is not None:
            _log_msg(f"  bars Supabase 기존(24h) {len(bar_keys)}건 확인.")
        _log_msg("  bars 로컬 조회 중(과거 24시간)...")
        conn = _pm.get_connection(pm_db_path)
        cur = conn.execute(
            "SELECT symbol, timeframe, bar_time, open, high, low, close, bb20_upper, bb20_lower, bb4_upper, bb4_lower, sma20, sma120, updated_at FROM bars WHERE bar_time >= ?",
            (cutoff_24h_str,),
        )
        bar_rows = cur.fetchall()
        conn.close()
        n_bars = len(bar_rows)
        _log_msg(f"  bars 로컬(24h) {n_bars}건 조회됨.")
        cols = ["symbol", "timeframe", "bar_time", "open", "high", "low", "close", "bb20_upper", "bb20_lower", "bb4_upper", "bb4_lower", "sma20", "sma120", "updated_at"]
        if bar_rows:
            missing_bar_rows = []
            for r in bar_rows:
                key = (r[0], r[1], r[2])
                if bar_keys is None or key not in bar_keys:
                    missing_bar_rows.append(r)
            if not missing_bar_rows:
                summary_lines.append("bars: 이미 동기화됨(빠진 건 0건)")
                _log_msg("  bars: 이미 동기화됨, 업로드 스킵.")
            else:
                _log_msg(f"  bars 빠진 건 {len(missing_bar_rows)}건 → 업로드 대상.")
                BATCH = 200
                ok_count = 0
                total_batches = (len(missing_bar_rows) + BATCH - 1) // BATCH
                for i in range(0, len(missing_bar_rows), BATCH):
                    batch_no = i // BATCH + 1
                    chunk = [dict(zip(cols, r)) for r in missing_bar_rows[i : i + BATCH]]
                    ok, err = _post_insert_ignore_duplicates("bars", chunk, "symbol,timeframe,bar_time")
                    if ok:
                        ok_count += len(chunk)
                    elif err:
                        _log_msg(f"  bars 배치 {batch_no} 실패: {err[:300]}")
                    if total_batches > 1:
                        _log_msg(f"  bars 배치 {batch_no}/{total_batches} 완료 ({ok_count}/{len(missing_bar_rows)}건)")
                summary_lines.append(f"bars: 빠진 {len(missing_bar_rows)}건 전송 — 완료 {ok_count}건")
                _log_msg(f"  bars 업로드 완료: {ok_count}/{len(missing_bar_rows)}건")
        else:
            summary_lines.append("bars: 0건(스킵)")
            _log_msg("  bars 0건 → 스킵.")

        # 3) session_high_low — 과거 24시간(해당 session_date 이상)만 조회, Supabase에 이미 있는 키 제외 후 빠진 것만 업로드
        _log_msg("  session_high_low Supabase 기존 키 조회 중(24h)...")
        shl_keys = _get_existing_keys_supabase(
            "session_high_low", ["symbol", "session", "session_date"], log_fn,
            filter_params={"session_date": f"gte.{cutoff_date_str}"},
        )
        if shl_keys is not None:
            _log_msg(f"  session_high_low Supabase 기존(24h) {len(shl_keys)}건 확인.")
        _log_msg("  session_high_low 로컬 조회 중(과거 24시간)...")
        conn2 = _pm.get_connection(pm_db_path)
        cur2 = conn2.execute(
            "SELECT symbol, session, session_date, high, low, updated_at FROM session_high_low WHERE session_date >= ?",
            (cutoff_date_str,),
        )
        shl_rows = cur2.fetchall()
        conn2.close()
        n_shl = len(shl_rows)
        _log_msg(f"  session_high_low 로컬(24h) {n_shl}건 조회됨.")
        shl_cols = ["symbol", "session", "session_date", "high", "low", "updated_at"]
        if shl_rows:
            missing_shl = [r for r in shl_rows if shl_keys is None or (r[0], r[1], r[2]) not in shl_keys]
            if not missing_shl:
                summary_lines.append("session_high_low: 이미 동기화됨(빠진 건 0건)")
                _log_msg("  session_high_low: 이미 동기화됨, 업로드 스킵.")
            else:
                _log_msg(f"  session_high_low 빠진 건 {len(missing_shl)}건 → 업로드 대상.")
                payloads_shl = [dict(zip(shl_cols, r)) for r in missing_shl]
                ok, err = _post_insert_ignore_duplicates("session_high_low", payloads_shl, "symbol,session,session_date")
                if not ok:
                    _log_msg(f"  session_high_low 실패 원인: {err}")
                summary_lines.append(f"session_high_low: 빠진 {len(payloads_shl)}건 전송 — {'성공' if ok else '실패'}")
                _log_msg(f"  session_high_low 업로드 {'성공' if ok else '실패'}.")
        else:
            summary_lines.append("session_high_low: 0건(스킵)")
            _log_msg("  session_high_low 0건 → 스킵.")

        _log_msg("  [Supabase 동기화] 전체 완료.")
        return True, "\n".join(summary_lines)
    except Exception as e:
        _log.warning("Supabase 전체 동기화 실패: %s", e)
        err_msg = f"동기화 중 오류: {e}"
        _log_msg(f"  [Supabase 동기화] 실패: {err_msg}")
        return False, err_msg
