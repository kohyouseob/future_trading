# -*- coding: utf-8 -*-
"""
BAR 테이블 보충 + 볼린저밴드 재계산. 빠진 봉 채우고 BB/SMA 다시 계산.
포지션 모니터 런처의 'BAR 보충·BB 갱신' 버튼 또는 직접 실행: python position_monitor_bar_backfill.py
실행 로그를 텔레그램으로 전송.
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
from position_monitoring_closing import init_mt5, run_bar_backfill

try:
    from telegram_sender_utils import send_telegram_msg
except ImportError:
    def send_telegram_msg(msg: str, **kwargs) -> None:
        print("[Telegram 미연결]", msg)


def main():
    print("=== BAR 보충·BB 갱신 ===", flush=True)
    if not init_mt5():
        err = "MT5 연결 실패. 터미널 실행 및 로그인 확인."
        print(err, flush=True)
        send_telegram_msg(f"❌ **BAR 보충·BB 갱신**\n{err}")
        return
    try:
        log_lines, total = run_bar_backfill()
        for line in log_lines:
            print(line, flush=True)
        body = "\n".join(log_lines)
        send_telegram_msg(f"📊 **BAR 보충·BB 갱신**\n{body}")
    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
