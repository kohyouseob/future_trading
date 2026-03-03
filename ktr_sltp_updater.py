# -*- coding: utf-8 -*-
"""
KTR 포지션의 TP/SL을 코멘트에 저장된 차익실현·손절 정보로 갱신하는 프로그램.
코멘트 형식: KTR1 Asia1H|TP:20이평|SL:N 또는 SL:잔액비 -10%
실행: python ktr_sltp_updater.py  (1회 실행)
      python ktr_sltp_updater.py --loop 600  (10분(600초)마다 반복)
"""
import argparse
import os
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import MetaTrader5 as mt5

import mt5_trade_utils as tr
from ktr_sltp_utils import (
    get_tp_level,
    get_sl_price,
    get_ktr_from_db,
    load_sltp_overrides,
)

MAGIC_KTR = 888001


# comment ASCII 코드 → get_tp_level/get_sl_price용 옵션 문자열 (ktr_order_reservation_gui와 대응)
_CODE_TO_TP = {"TP20": "20이평", "TP120": "120이평", "TP20B": "20B상단", "TP4B": "4B상단"}
_CODE_TO_SL = {"SLN": "N기준", "SLa10": "잔액비 -10%", "SLa20": "잔액비 -20%", "SLa50": "잔액비 50%", "SLx": "사용하지 않음"}


def _parse_comment(comment: str):
    """코멘트에서 TP 옵션, SL 옵션, 세션, 타임프레임 추출.
    지원 형식: KTR1 Europe TF:10M TP20 SLN (공백 구분, ASCII) 또는 KTR1 Asia|TF:1H|TP:20이평|SL:N (구 형식)
    반환: (tp_option, sl_option, session, timeframe) 또는 None
    """
    if not comment:
        return None
    # 새 형식: 공백 구분, ASCII (KTR1 Europe TF:10M TP20 SLN)
    parts = comment.split()
    if len(parts) >= 4:
        base = parts[0]  # KTR1
        session = ""
        timeframe = ""
        tp_option = ""
        sl_option = ""
        i = 1
        while i < len(parts):
            p = parts[i]
            if p.startswith("TF:"):
                timeframe = p[3:].strip()
            elif p in _CODE_TO_TP:
                tp_option = _CODE_TO_TP[p]
            elif p in _CODE_TO_SL:
                sl_option = _CODE_TO_SL[p]
            elif p and not p.startswith("TF:") and not session:
                session = p  # Europe, Asia 등
            i += 1
        if session and timeframe and tp_option and sl_option:
            return (tp_option, sl_option, session, timeframe)
    # 구 형식: 파이프 구분
    if "|" in comment:
        parts = [p.strip() for p in comment.split("|")]
        tp_option = sl_option = session = timeframe = ""
        for p in parts:
            if p.startswith("TP:"):
                tp_option = p[3:].strip()
            elif p.startswith("SL:"):
                sl_option = p[3:].strip()
            elif p.startswith("TF:"):
                timeframe = p[3:].strip()
        if not tp_option or not sl_option:
            return None
        if sl_option == "N":
            sl_option = "N기준"
        base = parts[0].strip()
        rest = base.split(None, 1)[-1] if " " in base else base
        if timeframe:
            session = rest
        else:
            if rest.endswith("1H"):
                session, timeframe = rest[:-2], "1H"
            elif rest.endswith("5M"):
                session, timeframe = rest[:-2], "5M"
            else:
                for suf in ("10M", "4H", "2H", "1H", "5M", "2M"):
                    if rest.endswith(suf):
                        session, timeframe = rest[: -len(suf)], suf
                        break
        if session and timeframe:
            return (tp_option, sl_option, session, timeframe)
    return None


def _compute_sl_for_n(symbol: str, is_buy: bool, session: str, timeframe: str) -> float:
    """SL:N인 경우: 동일 심볼·매직 포지션 중 마지막 진입가(가장 끝) ± KTR."""
    positions = mt5.positions_get(symbol=symbol)
    positions = [p for p in positions if getattr(p, "magic", 0) == MAGIC_KTR]
    if not positions:
        return 0.0
    ktr_value = get_ktr_from_db(symbol, session, timeframe)
    if ktr_value <= 0:
        return 0.0
    if is_buy:
        last_entry = min(p.price_open for p in positions)
        return last_entry - ktr_value
    else:
        last_entry = max(p.price_open for p in positions)
        return last_entry + ktr_value


def run_once(log_fn=None):
    """MAGIC_KTR 포지션을 순회하며 코멘트(또는 override) 기반으로 TP/SL 갱신. 1회 실행."""
    if not tr.init_mt5():
        if log_fn:
            log_fn("MT5 연결 실패")
        return
    balance = mt5.account_info().balance
    positions = mt5.positions_get()
    positions = [p for p in positions if getattr(p, "magic", 0) == MAGIC_KTR]
    overrides = load_sltp_overrides()
    updated = 0
    for pos in positions:
        ov = overrides.get(str(pos.ticket))
        if ov:
            tp_option = ov.get("tp_option", "")
            sl_option = ov.get("sl_option", "")
            session = ov.get("session", "Asia")
            timeframe = ov.get("timeframe", "1H")
        else:
            comment = getattr(pos, "comment", "") or ""
            parsed = _parse_comment(comment)
            if not parsed:
                continue
            tp_option, sl_option, session, timeframe = parsed
        sym = pos.symbol
        is_buy = pos.type == mt5.ORDER_TYPE_BUY

        tp_level = get_tp_level(sym, tp_option)
        if sl_option == "N":
            sl_price = _compute_sl_for_n(sym, is_buy, session, timeframe)
        else:
            sl_price = get_sl_price(
                sym, is_buy, pos.price_open, pos.volume, balance, sl_option
            )
        sl_f = sl_price if sl_price and sl_price > 0 else 0.0
        tp_f = tp_level if tp_level and tp_level > 0 else 0.0
        if sl_f <= 0 and tp_f <= 0:
            continue
        ok, msg = tr.modify_position_sltp(pos.ticket, sym, sl_f, tp_f)
        if ok:
            updated += 1
            if log_fn:
                log_fn(f"#{pos.ticket} {sym} SL/TP 갱신 완료 (TP:{tp_option} SL:{sl_option})")
        else:
            if log_fn:
                log_fn(f"#{pos.ticket} 갱신 실패: {msg}")
    return updated


def main():
    parser = argparse.ArgumentParser(description="KTR 포지션 TP/SL 코멘트 기반 갱신")
    parser.add_argument("--loop", type=float, default=0, metavar="SEC",
                        help="갱신 간격(초). 0이면 1회만 실행")
    args = parser.parse_args()

    def log(msg):
        print(f"[KTR SL/TP] {msg}")

    if args.loop <= 0:
        n = run_once(log_fn=log)
        log(f"완료. 갱신된 포지션: {n or 0}개")
        return

    log(f"{args.loop}초 간격으로 갱신 시작 (Ctrl+C 종료)")
    try:
        while True:
            run_once(log_fn=log)
            time.sleep(args.loop)
    except KeyboardInterrupt:
        log("중지됨")


if __name__ == "__main__":
    main()
