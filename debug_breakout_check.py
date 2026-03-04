# -*- coding: utf-8 -*-
"""
돌파더블비 진입 조건 점검 디버그 스크립트.

사용 예:
  python debug_breakout_check.py
  python debug_breakout_check.py --symbol NAS100+ --tf 5분

출력:
  - 현재 시각(KST)
  - [진입 TF] 5분봉 rates: index별 Bar Time, O, H, L, C, 4B상단(해당 시), 되돌림선
  - 돌파 봉(i) 후보별: close > 4B상단 여부, 되돌림 봉(j)별 low <= 되돌림선 여부
  - 최종 메시지(실제 로그와 동일 형식)
  - [상위 TF] 10분봉 20이평/120이평, 정배열 여부, "역배열 → 진입 스킵" 메시지
"""
import sys
import os
import argparse
from datetime import datetime

import pytz

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

KST = pytz.timezone("Asia/Seoul")


def main():
    parser = argparse.ArgumentParser(description="돌파더블비 조건 점검 디버그")
    parser.add_argument("--symbol", default="NAS100+", help="심볼 (기본 NAS100+)")
    parser.add_argument("--tf", default="5분", choices=["2분", "5분", "10분", "1시간"], help="진입 타임프레임")
    parser.add_argument("--weight", type=float, default=1.0, help="비중 %% (메시지용)")
    args = parser.parse_args()

    from breakout_order_gui import (
        _get_rates,
        _format_bar_time_range_kst,
        _bb_upper_series,
        _is_higher_tf_correct_alignment,
        TF_MAP,
        HIGHER_TF_FOR_ALIGNMENT,
        RETRACE_BARS_AFTER_BREAKOUT,
        _sma,
    )

    now_kst = datetime.now(KST)
    symbol = args.symbol.strip()
    if not symbol.endswith("+"):
        symbol = symbol + "+"
    tf_label = args.tf
    mt5_tf = TF_MAP.get(tf_label)
    if mt5_tf is None:
        print(f"지원하지 않는 TF: {tf_label}")
        return

    print("=" * 60)
    print(f"현재 시각(KST): {now_kst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"심볼: {symbol}  진입 TF: {tf_label}")
    print("=" * 60)

    # ---------- 진입 TF 봉 데이터 ----------
    rates = _get_rates(symbol, mt5_tf, count=15)
    if rates is None or len(rates) < 7:
        print("[진입 TF] 봉 데이터 부족")
        return

    n = len(rates)
    idx_2bar = n - 3
    print(f"\n[진입 TF] {tf_label}봉 n={n}, idx_2bar(2봉전)={idx_2bar}")
    print("MT5 copy_rates_from_pos: rates[0]=과거(오래된), rates[n-1]=현재")
    print("-" * 60)
    print(f"{'idx':>3}  {'Bar Time(KST)':<22}  {'O':>10}  {'H':>10}  {'L':>10}  {'C':>10}  |  bars_ago")
    print("-" * 60)

    for idx in range(n):
        bar_time = _format_bar_time_range_kst(rates, idx, mt5_tf, now_kst=now_kst)
        o = float(rates["open"][idx])
        h = float(rates["high"][idx])
        l = float(rates["low"][idx])
        c = float(rates["close"][idx])
        bars_ago = (n - 1) - idx
        print(f"{idx:>3}  {bar_time:<22}  {o:>10.2f}  {h:>10.2f}  {l:>10.2f}  {c:>10.2f}  |  {bars_ago}봉 전")
    print()

    # ---------- 돌파+되돌림 탐색 (check_breakout_doublebottom 로직) ----------
    bar_time_str = _format_bar_time_range_kst(rates, idx_2bar, mt5_tf, now_kst=now_kst)
    bar_time_suffix = f", Bar(직전봉=2봉전)={bar_time_str}" if bar_time_str else ""

    print("[돌파 봉 탐색] i = idx_2bar 부터 과거로, 4B상단(open[i-3]~open[i]) 돌파 여부")
    print("-" * 60)

    found = False
    for i in range(idx_2bar, 2, -1):
        if i < 3:
            break
        opens_4 = [float(rates["open"][i + k]) for k in range(-3, 1)]
        bb4_upper = _bb_upper_series(opens_4, 4, 4)
        if bb4_upper is None:
            continue
        close_i = float(rates["close"][i])
        high_i = float(rates["high"][i])
        low_i = float(rates["low"][i])
        range_i = high_i - low_i
        if range_i <= 0:
            continue
        retrace_level = high_i - range_i * 0.33
        bars_ago_i = (n - 1) - i
        bar_time_i = _format_bar_time_range_kst(rates, i, mt5_tf, now_kst=now_kst)

        breakout_ok = close_i > bb4_upper
        print(f"\n  i={i} ({bars_ago_i}봉 전) Bar={bar_time_i}")
        print(f"      open[i-3..i]={[f'{x:.2f}' for x in opens_4]} → 4B상단={bb4_upper:.2f}")
        print(f"      close={close_i:.2f}  high={high_i:.2f}  low={low_i:.2f}  range={range_i:.2f}")
        print(f"      되돌림선(high-0.33*range)={retrace_level:.2f}  돌파여부(close>4B)={breakout_ok}")

        if not breakout_ok:
            continue

        start_j = max(1, i - RETRACE_BARS_AFTER_BREAKOUT)
        for j in range(start_j, i):
            low_j = float(rates["low"][j])
            hit = low_j <= retrace_level
            bars_ago_j = (n - 1) - j
            bar_time_j = _format_bar_time_range_kst(rates, j, mt5_tf, now_kst=now_kst)
            print(f"      j={j} ({bars_ago_j}봉 전) Bar={bar_time_j}  low={low_j:.2f} <= 되돌림선? {hit}")
            if hit:
                bars_after = i - j
                print()
                print(">>> 조건 충족 (실제 로그와 동일 메시지)")
                msg = (
                    f"{bars_ago_i}봉 전 BB상단 돌파 마감 + 그 다음 {bars_after}봉째에서 33% 되돌림 충족 "
                    f"(BB상단={bb4_upper:.2f}, 되돌림선={retrace_level:.2f}{bar_time_suffix})"
                )
                print(f"    [{tf_label}] {msg}")
                print()
                found = True
                break
        if found:
            break
    else:
        print("\n>>> 돌파+되돌림 조건 충족 봉 없음")
        close_prev = float(rates["close"][idx_2bar])
        if idx_2bar >= 3:
            opens_4_prev = [float(rates["open"][idx_2bar + k]) for k in range(-3, 1)]
            bb4_prev = _bb_upper_series(opens_4_prev, 4, 4)
            if bb4_prev is not None:
                print(f"    직전봉(2봉전) 종가={close_prev:.2f}, 4B상단={bb4_prev:.2f}{bar_time_suffix} - 진입조건 만족 X")
            else:
                print(f"    직전봉(2봉전) 종가={close_prev:.2f}{bar_time_suffix} - 진입조건 만족 X")
        else:
            print(f"    직전봉(2봉전) 종가={close_prev:.2f}{bar_time_suffix} - 진입조건 만족 X")

    # ---------- 상위 TF 정배열(20이평 > 120이평) ----------
    higher_label = HIGHER_TF_FOR_ALIGNMENT.get(tf_label)
    if higher_label is None:
        print("\n[상위 TF] 1시간봉 진입 → 정배열 검사 없음")
        return

    higher_tf = TF_MAP.get(higher_label)
    if higher_tf is None:
        print(f"\n[상위 TF] {higher_label} MT5 TF 없음")
        return

    rates_h = _get_rates(symbol, higher_tf, count=125)
    if rates_h is None or len(rates_h) < 121:
        print(f"\n[상위 TF] {higher_label}봉 데이터 부족(120봉 필요)")
        return

    closes = [float(rates_h["close"][i]) for i in range(len(rates_h))]
    use_20 = closes[1 : 1 + 20]
    use_120 = closes[1 : 1 + 120]
    if len(use_20) < 20 or len(use_120) < 120:
        print(f"\n[상위 TF] {higher_label}봉 봉 수 부족")
        return

    sma20 = _sma(use_20, 20)
    sma120 = _sma(use_120, 120)
    if sma20 is None or sma120 is None:
        print(f"\n[상위 TF] {higher_label}봉 20/120이평 계산 불가")
        return

    print()
    print("=" * 60)
    print(f"[상위 TF] {higher_label}봉 정배열(20이평 > 120이평) 점검")
    print("  (진입 TF 5분 → 상위 10분, closes[1:21]=20봉, closes[1:121]=120봉)")
    print(f"  20이평(SMA closes[1:21]) = {sma20:.2f}")
    print(f"  120이평(SMA closes[1:121]) = {sma120:.2f}")
    ok_align, align_reason = _is_higher_tf_correct_alignment(symbol, tf_label)
    if ok_align:
        print("  결과: 정배열 → 진입 허용")
    else:
        print(f"  결과: {align_reason}")
        print(f"  (실제 로그) ⏭️ [{tf_label}] {align_reason}")
    print("=" * 60)


if __name__ == "__main__":
    main()
