import sqlite3
import logging
from datetime import datetime, timedelta, time
import pytz

from db_config import UNIFIED_DB_PATH

KST = pytz.timezone("Asia/Seoul")
_log = logging.getLogger(__name__)

# 누락 판정 시 두 심볼 모두 있어야 "비누락". 이 심볼명은 ktr_records.symbol 값과 일치해야 함.
KTR_SLOT_SYMBOLS = ("NAS100", "XAUUSD")


class KTRDatabase:
    def __init__(self, db_name=None):
        if db_name is None:
            db_name = UNIFIED_DB_PATH
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_table()

    def create_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ktr_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                session TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                record_date TEXT,
                ktr_value REAL NOT NULL,
                balance REAL,
                lot_1st REAL,
                lot_2nd REAL,
                lot_3rd REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, session, timeframe, record_date)
            )
        """)
        self.conn.commit()
        # 기존 테이블에 UNIQUE 없을 수 있음 → 인덱스로 중복 조회/대체에 활용
        try:
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_ktr_unique ON ktr_records(symbol, session, timeframe, record_date)"
            )
            self.conn.commit()
        except Exception:
            pass

    def update_ktr(self, symbol, session, timeframe, value, balance=None, lot_1st=None, lot_2nd=None, lot_3rd=None, record_date=None):
        created_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        if record_date is None or (isinstance(record_date, str) and not record_date.strip()):
            record_date = datetime.now(KST).strftime("%Y-%m-%d")
        # Supabase 우선 반영(주 DB), 성공 시 로컬 백업. 실패 시에도 로컬에 저장(데이터 보존).
        supabase_ok = False
        try:
            from supabase_sync import sync_ktr_record, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED and sync_ktr_record:
                supabase_ok = sync_ktr_record(
                    symbol=symbol, session=session, timeframe=timeframe, record_date=record_date,
                    ktr_value=value, balance=balance, lot_1st=lot_1st, lot_2nd=lot_2nd, lot_3rd=lot_3rd,
                    created_at=created_at,
                )
                if supabase_ok:
                    _log.info("Supabase 반영: ktr_records 1건 성공 (%s %s %s %s)", symbol, session, timeframe, record_date)
                else:
                    _log.warning("Supabase 반영 실패, 로컬 백업만 저장: ktr_records (%s %s %s %s)", symbol, session, timeframe, record_date)
        except Exception as e:
            _log.debug("Supabase 반영 스킵(비활성 또는 오류), 로컬 백업만 저장: %s", e)
        # 로컬 백업 저장 (Supabase 성공 여부와 관계없이 항상 반영)
        cur = self.conn.execute(
            "SELECT id FROM ktr_records WHERE symbol=? AND session=? AND timeframe=? AND record_date=?",
            (symbol, session, timeframe, record_date),
        )
        row = cur.fetchone()
        if row:
            self.conn.execute(
                """
                UPDATE ktr_records SET ktr_value=?, balance=?, lot_1st=?, lot_2nd=?, lot_3rd=?, created_at=?
                WHERE symbol=? AND session=? AND timeframe=? AND record_date=?
                """,
                (value, balance, lot_1st, lot_2nd, lot_3rd, created_at, symbol, session, timeframe, record_date),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO ktr_records (symbol, session, timeframe, record_date, ktr_value, balance, lot_1st, lot_2nd, lot_3rd, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (symbol, session, timeframe, record_date, value, balance, lot_1st, lot_2nd, lot_3rd, created_at),
            )
        self.conn.commit()

    def has_ktr_for_session_timeframe_date(self, session: str, timeframe: str, record_date: str) -> bool:
        """해당 세션·타임프레임·측정일에 레코드가 1건이라도 있으면 True (이미 측정됨). Supabase 우선 조회."""
        try:
            from supabase_sync import has_ktr_for_session_timeframe_date_supabase, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED and has_ktr_for_session_timeframe_date_supabase(session, timeframe, record_date):
                return True
        except Exception:
            pass
        if not record_date or not isinstance(record_date, str):
            return False
        rd = record_date.strip()[:10]
        cur = self.conn.execute(
            """SELECT 1 FROM ktr_records
               WHERE session=? AND timeframe=?
               AND (record_date = ? OR (record_date IS NOT NULL AND date(record_date) = date(?))
                    OR (record_date IS NULL AND date(created_at) = date(?)))
               LIMIT 1""",
            (session, timeframe, rd, rd, rd),
        )
        return cur.fetchone() is not None

    def has_both_symbols_for_slot(self, session: str, timeframe: str, record_date: str) -> bool:
        """해당 (세션, 타임프레임, 측정일)에 NAS100·XAUUSD 두 심볼 모두 레코드가 있으면 True. 누락 판정용."""
        try:
            from supabase_sync import has_both_ktr_symbols_for_slot_supabase, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED:
                return has_both_ktr_symbols_for_slot_supabase(session, timeframe, record_date)
        except Exception:
            pass
        if not record_date or not isinstance(record_date, str):
            return False
        rd = record_date.strip()[:10]
        cur = self.conn.execute(
            """SELECT symbol FROM ktr_records
               WHERE session=? AND timeframe=?
               AND (record_date = ? OR (record_date IS NOT NULL AND date(record_date) = date(?))
                    OR (record_date IS NULL AND date(created_at) = date(?)))""",
            (session, timeframe, rd, rd, rd),
        )
        found = {row[0] for row in cur.fetchall() if row and row[0]}
        return all(s in found for s in KTR_SLOT_SYMBOLS)

    def get_latest_ktr(self, symbol, session, timeframe):
        """Supabase 우선 조회."""
        try:
            from supabase_sync import get_latest_ktr_supabase, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED:
                v = get_latest_ktr_supabase(symbol, session, timeframe)
                if v is not None:
                    return v
        except Exception:
            pass
        cur = self.conn.execute(
            "SELECT ktr_value FROM ktr_records WHERE symbol = ? AND session = ? AND timeframe = ? ORDER BY created_at DESC LIMIT 1",
            (symbol, session, timeframe),
        )
        row = cur.fetchone()
        return row[0] if row else 0.0

    def get_most_recent_session(self, symbol, timeframe):
        """Supabase 우선 조회."""
        try:
            from supabase_sync import get_most_recent_session_supabase, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED:
                s = get_most_recent_session_supabase(symbol, timeframe)
                if s is not None:
                    return s
        except Exception:
            pass
        cur = self.conn.execute(
            "SELECT session FROM ktr_records WHERE symbol = ? AND timeframe = ? ORDER BY created_at DESC LIMIT 1",
            (symbol, timeframe),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def get_most_recent_ktr(self, symbol, timeframe):
        """해당 symbol·timeframe으로 가장 최근에 기록된 레코드의 (ktr_value, session). Supabase 우선 조회."""
        try:
            from supabase_sync import get_most_recent_ktr_supabase, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED:
                val, sess = get_most_recent_ktr_supabase(symbol, timeframe)
                return (val, sess)
        except Exception:
            pass
        cur = self.conn.execute(
            "SELECT ktr_value, session FROM ktr_records WHERE symbol = ? AND timeframe = ? ORDER BY created_at DESC LIMIT 1",
            (symbol, timeframe),
        )
        row = cur.fetchone()
        return (float(row[0]), row[1]) if row and row[0] is not None else (0.0, None)

    def get_recent_records(self, limit=100):
        """Supabase 우선 조회. Supabase 반환 시 각 행에 id=None (로컬 id 없음)."""
        try:
            from supabase_sync import get_ktr_records_supabase, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED:
                data = get_ktr_records_supabase(limit=limit)
                if data is not None:
                    cols = ["id", "symbol", "session", "timeframe", "record_date", "ktr_value", "balance", "lot_1st", "lot_2nd", "lot_3rd", "created_at"]
                    return [dict(zip(cols, [None, r.get("symbol"), r.get("session"), r.get("timeframe"), r.get("record_date"), r.get("ktr_value"), r.get("balance"), r.get("lot_1st"), r.get("lot_2nd"), r.get("lot_3rd"), r.get("created_at")])) for r in data]
        except Exception:
            pass
        cur = self.conn.execute(
            "SELECT id, symbol, session, timeframe, COALESCE(record_date, date(created_at)), ktr_value, balance, lot_1st, lot_2nd, lot_3rd, created_at FROM ktr_records ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        cols = ["id", "symbol", "session", "timeframe", "record_date", "ktr_value", "balance", "lot_1st", "lot_2nd", "lot_3rd", "created_at"]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def get_missing_ktr_slots(self, dates):
        """확인할 날짜(YYYY-MM-DD) 리스트에 대해, (session, timeframe, record_date) 조합 중
        ktr_records에 레코드가 하나도 없고, 해당 세션의 측정 시각이 이미 지난 슬롯만 반환.
        오늘 8:58이면 유럽·미국(아직 안 지남)은 누락에서 제외."""
        slots = [
            ("Asia", "5M"), ("Asia", "10M"), ("Asia", "1H"),
            ("Europe", "5M"), ("Europe", "10M"), ("Europe", "1H"),
            ("US", "5M"), ("US", "10M"), ("US", "1H"),
        ]
        missing = []
        now_kst = datetime.now(KST)
        for record_date in dates:
            if not record_date or not isinstance(record_date, str):
                continue
            record_date = record_date.strip()
            for session, timeframe in slots:
                if not self._slot_measurement_past(session, timeframe, record_date, now_kst):
                    continue
                # 두 심볼(NAS100, XAUUSD) 모두 있어야 비누락. 하나라도 없으면 누락으로 표시
                if not self.has_both_symbols_for_slot(session, timeframe, record_date):
                    missing.append((session, timeframe, record_date))
        return missing

    def _slot_measurement_past(self, session: str, timeframe: str, record_date: str, now_kst: datetime) -> bool:
        """해당 (세션, 타임프레임, 날짜)의 KTR 측정 시각이 이미 지났는지. 지났으면 True."""
        try:
            d = datetime.strptime(record_date.strip()[:10], "%Y-%m-%d").date()
        except Exception:
            return True
        # 각 세션별 "이 시각 이후면 측정 가능" (KST 기준)
        # Asia: 8:00 5M→8:05, 10M→8:10, 1H→9:00
        # Europe: 17:00 5M→17:05, 10M→17:10, 1H→18:00
        # US: 23:30 5M→23:35, 10M→23:40, 1H→당일 00:00 (record_date가 봉 마감일이므로 같은 날 00:00에 측정됨)
        if session == "Asia":
            if timeframe == "5M":
                t = time(8, 5)
            elif timeframe == "10M":
                t = time(8, 10)
            else:
                t = time(9, 0)
            cutoff = KST.localize(datetime.combine(d, t))
        elif session == "Europe":
            if timeframe == "5M":
                t = time(17, 5)
            elif timeframe == "10M":
                t = time(17, 10)
            else:
                t = time(18, 0)
            cutoff = KST.localize(datetime.combine(d, t))
        else:
            if timeframe == "5M":
                t = time(23, 35)
            elif timeframe == "10M":
                t = time(23, 40)
            else:
                # US 1H: record_date = 봉 마감일(00:00 해당일) → 측정 시각은 당일 00:00
                t = time(0, 0)
            cutoff = KST.localize(datetime.combine(d, t))
        if now_kst.tzinfo is None:
            now_kst = KST.localize(now_kst)
        return now_kst >= cutoff

    def delete_by_id(self, rec_id: int) -> bool:
        """지정한 id의 ktr_records 행을 삭제. Supabase에서 먼저 삭제 후 로컬 백업에서도 삭제."""
        if rec_id is None or rec_id <= 0:
            return False
        cur_sel = self.conn.execute(
            "SELECT symbol, session, timeframe, record_date FROM ktr_records WHERE id = ?", (rec_id,)
        )
        row = cur_sel.fetchone()
        if not row:
            cur = self.conn.execute("DELETE FROM ktr_records WHERE id = ?", (rec_id,))
            self.conn.commit()
            return cur.rowcount > 0
        sym, sess, tf, rd = row[0], row[1], row[2], row[3] or ""
        # Supabase(주 DB)에서 먼저 삭제
        try:
            from supabase_sync import sync_ktr_delete_by_natural_key, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED and sync_ktr_delete_by_natural_key:
                sync_ktr_delete_by_natural_key(sym, sess, tf, rd)
        except Exception as e:
            _log.debug("Supabase 삭제 스킵: %s", e)
        cur = self.conn.execute("DELETE FROM ktr_records WHERE id = ?", (rec_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def delete_by_natural_key(self, symbol: str, session: str, timeframe: str, record_date: str) -> bool:
        """(symbol, session, timeframe, record_date)로 Supabase에서 먼저 삭제 후 로컬 백업에서 삭제."""
        symbol = (symbol or "").strip()
        session = (session or "").strip()
        timeframe = (timeframe or "").strip()
        rd = (record_date or "").strip()[:10]
        if not symbol or not session or not timeframe:
            return False
        # Supabase(주 DB)에서 먼저 삭제
        try:
            from supabase_sync import sync_ktr_delete_by_natural_key, SUPABASE_SYNC_ENABLED
            if SUPABASE_SYNC_ENABLED and sync_ktr_delete_by_natural_key:
                sync_ktr_delete_by_natural_key(symbol, session, timeframe, rd or "")
        except Exception:
            pass
        cur = self.conn.execute(
            "DELETE FROM ktr_records WHERE symbol = ? AND session = ? AND timeframe = ? AND COALESCE(record_date, '') = ?",
            (symbol, session, timeframe, rd),
        )
        self.conn.commit()
        if cur.rowcount > 0:
            return True
        return False

    def delete_duplicate_records(self) -> int:
        """동일 (symbol, session, timeframe, record_date) 기준으로 중복 행 제거. 각 그룹에서 id가 가장 큰(최신) 1건만 남기고 나머지 삭제. 삭제된 행 수 반환."""
        cur = self.conn.execute("""
            DELETE FROM ktr_records WHERE id NOT IN (
                SELECT MAX(id) FROM ktr_records
                GROUP BY symbol, session, timeframe, record_date
            )
        """)
        deleted = cur.rowcount
        self.conn.commit()
        return deleted
