# -*- coding: utf-8 -*-
"""
MT5 봉 시각 공통 보정.
브로커가 UTC+2 등으로 타임스탬프를 주면 2시간 앞서 보이므로 기본 -7200(초) 적용.
환경변수 MT5_SESSION_OFFSET_SEC로 변경 가능 (0이면 보정 없음).
"""
import os
from datetime import datetime
import pytz

KST = pytz.timezone("Asia/Seoul")
MT5_SESSION_OFFSET_SEC = int(os.environ.get("MT5_SESSION_OFFSET_SEC", str(-2 * 3600)))


def mt5_ts_to_kst(ts: int) -> datetime:
    if ts > 1e10:
        ts = ts // 1000
    corrected = ts + MT5_SESSION_OFFSET_SEC
    return datetime.fromtimestamp(corrected, tz=KST)


def mt5_ts_to_kst_str(ts: int, fmt: str = "%Y-%m-%d %H:%M") -> str:
    return mt5_ts_to_kst(ts).strftime(fmt)
