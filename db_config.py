# -*- coding: utf-8 -*-
"""
v2 DB 정책: Supabase를 주 데이터베이스로 사용. 로컬 SQLite(scheduler.db)는 백업용.
- 읽기: Supabase 우선, 실패/비활성 시 로컬 fallback.
- 쓰기: Supabase에 먼저 반영, 성공 시 로컬에도 백업 저장.

로컬 DB 경로: path_config.yaml → 환경변수 WINDOWS_SCHEDULER_DB → v2/scheduler.db
Supabase: 주 DB. 환경변수 SUPABASE_URL, SUPABASE_ANON_KEY 로 오버라이드 가능.
"""
import os

# path_config 우선, 없으면 스크립트 기준
try:
    from path_config import UNIFIED_DB_PATH
except ImportError:
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    UNIFIED_DB_PATH = os.environ.get(
        "WINDOWS_SCHEDULER_DB",
        os.path.join(_SCRIPT_DIR, "scheduler.db"),
    )
    if not os.path.isabs(UNIFIED_DB_PATH):
        UNIFIED_DB_PATH = os.path.join(_SCRIPT_DIR, UNIFIED_DB_PATH)

# Supabase: 동기화용. 비어 있으면 동기화 비활성(로컬만 사용)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://afyjzlsbrpkjohywudar.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "sb_publishable__OMB22JYONVqGLRPGvubMA_AvxwBX0u")
# URL/KEY가 없거나 비활성화면 동기화 스킵
SUPABASE_SYNC_ENABLED = bool(SUPABASE_URL and SUPABASE_ANON_KEY and SUPABASE_URL.startswith("http"))
