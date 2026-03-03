# -*- coding: utf-8 -*-
"""
선택된 타임프레임/심볼에 맞춰 BB 데이터를 한 번 조회해 stdout으로 출력.
포지션 모니터 런처의 '새로고침' 버튼에서 호출됨.
실행: python position_monitor_bb_refresh.py
"""
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
os.chdir(SCRIPT_DIR)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import MetaTrader5 as mt5

from position_monitoring_closing import init_mt5, _emit_bb_bands


def main():
    if not init_mt5():
        print("[BB_BANDS]", flush=True)
        print("TF|", flush=True)
        print("[/BB_BANDS]", flush=True)
        return
    try:
        _emit_bb_bands()
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()
