# -*- coding: utf-8 -*-
"""
경로 환경 설정 (path_config.yaml).
다른 PC에서 실행 시 이 파일만 수정하면 됩니다.
- path_config.yaml 이 없으면 환경변수(MT5_PATH, WINDOWS_SCHEDULER_DB 등)와 기본값 사용.
"""
from __future__ import annotations

import os
from typing import Optional

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 기본값 (path_config.yaml 없을 때)
_DEFAULT_MT5_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"
_DEFAULT_DB_PATH = os.path.join(_SCRIPT_DIR, "scheduler.db")


def _load_path_config() -> dict:
    """path_config.yaml 로드. 없거나 실패 시 빈 dict."""
    try:
        import yaml
        path = os.path.join(_SCRIPT_DIR, "path_config.yaml")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


_config = _load_path_config()


def _get(key: str, env_key: Optional[str], default: str) -> str:
    val = (_config.get(key) or "").strip() if isinstance(_config.get(key), str) else ""
    if not val and env_key:
        val = (os.environ.get(env_key) or "").strip()
    return val if val else default


# 프로젝트 루트 (v2 폴더 = 이 파일이 있는 디렉터리)
PROJECT_ROOT = _SCRIPT_DIR

# MT5 실행 파일 경로 (다른 PC에서는 path_config.yaml의 mt5_path 만 수정)
MT5_PATH = _get("mt5_path", "MT5_PATH", _DEFAULT_MT5_PATH)

# MT5 터미널 데이터 폴더 (필요 시에만 지정)
MT5_TERMINAL_DATA_FOLDER = _get("mt5_terminal_data_folder", "MT5_TERMINAL_DATA_FOLDER", "")

# 통합 DB 파일 경로 (비우면 v2/scheduler.db 사용)
_raw_db = _get("db_path", "WINDOWS_SCHEDULER_DB", _DEFAULT_DB_PATH)
if not _raw_db:
    UNIFIED_DB_PATH = _DEFAULT_DB_PATH
elif os.path.isabs(_raw_db):
    UNIFIED_DB_PATH = _raw_db
else:
    UNIFIED_DB_PATH = os.path.join(_SCRIPT_DIR, _raw_db)
