# -*- coding: utf-8 -*-
"""
MT5 봉 시각 공통 보정.
브로커 서버가 UTC+2 등으로 타임스탬프를 주면 테이블/표시가 실제보다 2시간 앞서 보임.
기본 -7200(초) 적용 시: ts - 2h 를 UTC로 취급해 KST로 변환 → 실제 KST와 맞춤.
환경변수 MT5_SESSION_OFFSET_SEC로 변경 가능 (0이면 보정 없음).
"""
import os
from datetime import datetime
import pytz

KST = pytz.timezone("Asia/Seoul")
# 기본 -2시간: 브로커 타임스탬프가 2시간 앞서는 경우 보정. 테이블이 실제 KST와 맞으면 유지
MT5_SESSION_OFFSET_SEC = int(os.environ.get("MT5_SESSION_OFFSET_SEC", str(-2 * 3600)))


def mt5_ts_to_kst(ts: int) -> datetime:
    if ts > 1e10:
        ts = ts // 1000
    corrected = ts + MT5_SESSION_OFFSET_SEC
    return datetime.fromtimestamp(corrected, tz=KST)


def mt5_ts_to_kst_str(ts: int, fmt: str = "%Y-%m-%d %H:%M") -> str:
    return mt5_ts_to_kst(ts).strftime(fmt)
