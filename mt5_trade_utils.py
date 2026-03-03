# -*- coding: utf-8 -*-
import os
import shutil
import subprocess
import sys
import time
from typing import Any, List, Optional, Tuple, Union
import MetaTrader5 as _mt5  # type: ignore[reportMissingImports]
mt5: Any = _mt5

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# MT5 경로: path_config.yaml → 환경변수 MT5_PATH → 기본값
try:
    from path_config import MT5_PATH as _PC_MT5, MT5_TERMINAL_DATA_FOLDER as _PC_MT5_DATA
    MT5_PATH = _PC_MT5
    MT5_TERMINAL_DATA_FOLDER = _PC_MT5_DATA
except ImportError:
    MT5_PATH = os.environ.get("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe")
    MT5_TERMINAL_DATA_FOLDER = os.getenv("MT5_TERMINAL_DATA_FOLDER", "")


def init_mt5() -> bool:
    if mt5.initialize():
        return True
    if mt5.initialize(MT5_PATH):
        return True
    print(f"❌ MT5 초기화 실패: {mt5.last_error()}")
    return False


def login_mt5(login: Union[int, str], password: str, server: str) -> bool:
    """지정한 계좌로 MT5 로그인. 입출금 등 다중 계좌 사용 시 사용."""
    if not mt5.initialize(MT5_PATH):
        if not mt5.initialize():
            print(f"❌ MT5 초기화 실패: {mt5.last_error()}")
            return False
    authorized = mt5.login(login=int(login), password=password, server=server)
    if authorized:
        print(f"✅ 계좌 로그인 성공: {login} [{server}]")
        return True
    print(f"❌ 로그인 실패 ({login}): {mt5.last_error()}")
    return False


# 주문 시 랏수 상한. 매직넘버 등이 volume으로 잘못 넘어가 마진/청산 사고 방지.
VOLUME_MAX_LOTS = 100.0

# 잔액 대비 사용 마진 상한(%). 이 비율을 초과하면 새 오더 생성 불가.
MARGIN_PCT_MAX = 7.0


def _check_margin_pct_limit(margin_required: float) -> Tuple[bool, Optional[str]]:
    """현재 마진 + 추가 마진이 잔액 대비 MARGIN_PCT_MAX%를 초과하면 오더 불가. (성공 여부, 실패 시 메시지)."""
    acc = get_account_info()
    if not acc:
        return False, "계정 정보 조회 실패"
    balance = float(acc.get("balance") or 0)
    margin_used = float(acc.get("margin") or 0)
    if balance <= 0:
        return False, "잔액이 없어 오더를 생성할 수 없습니다."
    margin_required = float(margin_required or 0)
    total_margin = margin_used + margin_required
    pct = (total_margin / balance) * 100.0
    if pct > MARGIN_PCT_MAX:
        return False, (
            f"잔액 대비 마진 한도 초과: 예상 마진 {total_margin:,.2f} (기존 {margin_used:,.2f} + 신규 {margin_required:,.2f}) / 잔액 {balance:,.2f} = {pct:.1f}% (상한 {MARGIN_PCT_MAX}%)"
        )
    return True, None


def _validate_volume(volume: float, context: str = "주문") -> Tuple[bool, Optional[str]]:
    """랏수가 유효 범위(0 초과, VOLUME_MAX_LOTS 이하)인지 검사. (성공 여부, 실패 시 메시지)."""
    if volume is None or (isinstance(volume, (int, float)) and volume <= 0):
        return False, f"{context}: 랏수는 0보다 커야 합니다 (받은 값: {volume})"
    try:
        v = float(volume)
    except (TypeError, ValueError):
        return False, f"{context}: 랏수가 숫자가 아닙니다 (받은 값: {volume})"
    if v > VOLUME_MAX_LOTS:
        return False, f"{context}: 랏수 {v}는 상한 {VOLUME_MAX_LOTS} 초과. 마진/오입력 방지를 위해 거부합니다."
    return True, None


def _check_trade_allowed() -> Tuple[bool, Optional[str]]:
    info = mt5.terminal_info()
    if info is None:
        return False, "터미널 정보 조회 실패"
    if getattr(info, "trade_allowed", False):
        return True, None
    return False, "연결된 MT5에서 'Algo Trading'이 꺼져 있습니다. MT5 상단 툴바에서 켜 주세요."


def _is_autotrading_disabled_error(result: Any) -> bool:
    if result is None:
        return False
    comment = (getattr(result, "comment", None) or "").strip().lower()
    return "autotrading disabled" in comment


SW_RESTORE = 9


def _find_mt5_window_handle() -> Optional[int]:
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        from ctypes import wintypes
        user32 = ctypes.windll.user32  # type: ignore[attr-defined]
        found: List[int] = []

        def enum_cb(hwnd: int, _: int) -> int:
            if user32.IsWindowVisible(hwnd) and user32.GetParent(hwnd) == 0:
                length = user32.GetWindowTextLengthW(hwnd) + 1
                buf = ctypes.create_unicode_buffer(length)
                user32.GetWindowTextW(hwnd, buf, length)
                t = (buf.value or "").lower()
                if "metatrader" in t or "infinox" in t or "terminal" in t:
                    found.append(hwnd)
            return 1
        WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_int, wintypes.HWND, wintypes.LPARAM)
        user32.EnumWindows(WNDENUMPROC(enum_cb), 0)
        return found[0] if found else None
    except Exception:
        return None


def _try_enable_autotrading_via_hotkey() -> bool:
    if sys.platform != "win32":
        return False
    hwnd = _find_mt5_window_handle()
    if hwnd is None:
        return False
    try:
        import ctypes
        user32 = ctypes.windll.user32  # type: ignore[attr-defined]
        user32.ShowWindow(hwnd, SW_RESTORE)
        time.sleep(0.2)
        user32.SetForegroundWindow(hwnd)
        time.sleep(0.4)
        import pyautogui  # type: ignore[import-untyped]
        pyautogui.hotkey("ctrl", "e")
        return True
    except Exception:
        return False


def _do_mt5_restart_and_wait() -> None:
    try:
        mt5.shutdown()
    except Exception:
        pass
    try:
        subprocess.run(["taskkill", "/F", "/IM", "terminal64.exe"], capture_output=True, timeout=10)
        time.sleep(0.5)
    except Exception:
        pass
    time.sleep(2)
    if os.path.isfile(MT5_PATH):
        subprocess.Popen([MT5_PATH], cwd=os.path.dirname(MT5_PATH), creationflags=subprocess.DETACHED_PROCESS | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    time.sleep(8)


def get_market_price(symbol: str):
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return None, None
    return tick.ask, tick.bid


def _symbol_point_and_stops(symbol: str):
    """심볼의 point와 최소 SL/TP 거리(가격 단위). 반환: (point, min_distance_price)."""
    info = mt5.symbol_info(symbol)
    point = 0.01
    digits = 2
    if info is not None:
        point = getattr(info, "point", None) or 0.01
        digits = getattr(info, "digits", 2)
    if not point or point <= 0:
        point = 10.0 ** (-digits) if digits else 0.01
    # trade_stops_level: 브로커 최소 거리(포인트). symbol_info 속성 또는 symbol_info_int 사용
    stops_level = 0
    if info is not None:
        stops_level = int(getattr(info, "trade_stops_level", 0) or 0)
    if stops_level <= 0 and hasattr(mt5, "symbol_info_int"):
        try:
            sy = getattr(mt5, "SYMBOL_TRADE_STOPS_LEVEL", 16)
            v = mt5.symbol_info_int(symbol, sy)
            if v is not None:
                stops_level = int(v)
        except Exception:
            pass
    # 브로커가 0을 반환해도 일부는 최소 거리를 요구하므로 기본값 적용
    if stops_level <= 0:
        stops_level = 30
    min_dist = (stops_level + 2) * point  # 여유 2포인트
    return point, min_dist


def get_min_stops_distance_price(symbol: str) -> float:
    """심볼의 SL/TP 최소 거리(가격 단위). T/P 갱신 시 현재가 대비 최소 이격 확인용."""
    _, min_dist = _symbol_point_and_stops(symbol)
    return min_dist


def _normalize_sl_tp_for_position(symbol: str, pos_type: int, sl: float, tp: float):
    """
    포지션 SL/TP를 브로커 최소 거리(stops_level) 이상이 되도록 조정.
    pos_type: mt5.ORDER_TYPE_BUY or ORDER_TYPE_SELL.
    반환: (sl_adj, tp_adj) - 0이면 미설정.
    """
    if not init_mt5():
        return 0.0, 0.0
    ask, bid = get_market_price(symbol)
    if ask is None:
        return sl, tp
    point, min_dist = _symbol_point_and_stops(symbol)
    if min_dist <= 0:
        return sl, tp

    def _norm(p: float) -> float:
        if p is None or p <= 0:
            return 0.0
        return round(p / point) * point

    is_buy = pos_type == mt5.ORDER_TYPE_BUY
    ref = bid if is_buy else ask  # 매수 포지션 기준가는 bid, 매도는 ask
    sl_out = 0.0
    tp_out = 0.0
    if sl and sl > 0:
        if is_buy and sl >= ref - min_dist:
            sl_out = _norm(ref - min_dist)
        elif not is_buy and sl <= ref + min_dist:
            sl_out = _norm(ref + min_dist)
        else:
            sl_out = _norm(sl)
    if tp and tp > 0:
        if is_buy and tp <= ref + min_dist:
            tp_out = _norm(ref + min_dist)
        elif not is_buy and tp >= ref - min_dist:
            tp_out = _norm(ref - min_dist)
        else:
            tp_out = _norm(tp)
    return sl_out, tp_out


def _normalize_sl_tp_for_pending(symbol: str, order_price: float, sl: float, tp: float, is_buy: bool):
    """
    예약 주문의 SL/TP를 트리거 가격 기준 최소 거리 이상이 되도록 조정.
    반환: (sl_adj, tp_adj).
    """
    point, min_dist = _symbol_point_and_stops(symbol)
    if min_dist <= 0:
        return sl, tp

    def _norm(p: float) -> float:
        if p is None or p <= 0:
            return 0.0
        return round(p / point) * point

    sl_out = 0.0
    tp_out = 0.0
    if sl and sl > 0:
        if is_buy and sl >= order_price - min_dist:
            sl_out = _norm(order_price - min_dist)
        elif not is_buy and sl <= order_price + min_dist:
            sl_out = _norm(order_price + min_dist)
        else:
            sl_out = _norm(sl)
    if tp and tp > 0:
        if is_buy and tp <= order_price + min_dist:
            tp_out = _norm(order_price + min_dist)
        elif not is_buy and tp >= order_price - min_dist:
            tp_out = _norm(order_price - min_dist)
        else:
            tp_out = _norm(tp)
    return sl_out, tp_out


def get_active_positions(symbol: str):
    if not init_mt5():
        return []
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return []
    return sorted(positions, key=lambda x: x.time)


def _volume_to_lots(volume: float) -> float:
    """브로커 Invalid volume 방지: 소수 둘째 자리로 반올림."""
    return round(float(volume), 2)


def execute_market_order(symbol: str, action: str, volume: float, magic: int = 123456, comment: str = ""):
    if not init_mt5():
        return False, "연결 실패"
    volume = _volume_to_lots(volume)
    ok_vol, err_vol = _validate_volume(volume, "시장가 진입")
    if not ok_vol:
        return False, err_vol
    allowed, err = _check_trade_allowed()
    if not allowed:
        for _ in range(3):
            _try_enable_autotrading_via_hotkey()
            time.sleep(2.5)
            allowed, err = _check_trade_allowed()
            if allowed:
                break
        if not allowed:
            return False, (err or "자동매매 비허용")
    ask, bid = get_market_price(symbol)
    if ask is None:
        return False, "가격 조회 실패"
    order_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
    price = ask if action == "BUY" else bid
    margin_req = mt5.order_calc_margin(order_type, symbol, volume, price)
    if margin_req is not None:
        ok_limit, err_limit = _check_margin_pct_limit(float(margin_req))
        if not ok_limit:
            return False, (err_limit or "잔액 대비 마진 한도 초과")
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": magic,
        "comment": comment[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result is None:
        return False, f"진입 실패: {mt5.last_error()}"
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        return True, f"진입 성공 ({volume}랏)"
    if _is_autotrading_disabled_error(result) and _try_enable_autotrading_via_hotkey():
        time.sleep(1.5)
        ask, bid = get_market_price(symbol)
        if ask is not None:
            request["price"] = ask if action == "BUY" else bid
        result = mt5.order_send(request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            return True, f"진입 성공 ({volume}랏)"
    return False, f"진입 실패: {getattr(result, 'comment', '')}"


def close_market_order(symbol: str, ticket: int, volume: float, magic: int = 123456, comment: str = "Close"):
    if not init_mt5():
        return False, "연결 실패"
    volume = _volume_to_lots(volume)
    ok_vol, err_vol = _validate_volume(volume, "청산")
    if not ok_vol:
        return False, err_vol
    allowed, err = _check_trade_allowed()
    if not allowed:
        return False, (err or "자동매매 비허용")
    pos = mt5.positions_get(ticket=ticket)
    if not pos:
        return False, "포지션 없음"
    ask, bid = get_market_price(symbol)
    order_type = mt5.ORDER_TYPE_SELL if pos[0].type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    price = bid if pos[0].type == mt5.ORDER_TYPE_BUY else ask
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "position": ticket,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": magic,
        "comment": comment[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        return True, f"청산 성공 ({volume}랏)"
    return False, getattr(result, "comment", "unknown")


def get_account_info():
    if not init_mt5():
        return None
    account = mt5.account_info()
    if account is None:
        return None
    return {
        "login": account.login,
        "balance": account.balance,
        "equity": account.equity,
        "profit": account.profit,
        "margin": account.margin,
        "free_margin": account.margin_free,
        "margin_level": account.margin_level if account.margin > 0 else 0,
        "leverage": account.leverage,
        "server": account.server,
        "currency": account.currency,
    }


def place_pending_limit(symbol: str, action: str, volume: float, price: float, sl: float = 0.0, tp: float = 0.0, magic: int = 123456, comment: str = ""):
    if not init_mt5():
        return False, "연결 실패"
    volume = _volume_to_lots(volume)
    ok_vol, err_vol = _validate_volume(volume, "예약 주문")
    if not ok_vol:
        return False, err_vol
    if not mt5.symbol_select(symbol, True):
        return False, "종목 선택 실패"
    is_buy = action.upper() == "BUY"
    point, min_dist = _symbol_point_and_stops(symbol)
    ask, bid = get_market_price(symbol)
    # 예약 가격이 현재가와 너무 가까우면 Invalid price → bid/ask 기준 최소 거리로 보수적 조정
    if ask is not None and bid is not None and min_dist > 0:
        def _round_price(p: float) -> float:
            return round(p / point) * point if point else p
        if is_buy:
            # BUY_LIMIT: 트리거는 bid 아래여야 함. bid 기준으로 더 아래로 밀어 실패 감소
            if price >= bid - min_dist:
                price = _round_price(bid - min_dist)
        else:
            # SELL_LIMIT: 트리거는 ask 위여야 함
            if price <= ask + min_dist:
                price = _round_price(ask + min_dist)
    # 브로커 최소 거리(stops_level) 적용: 트리거 가격 기준으로 SL/TP 조정
    sl_use, tp_use = _normalize_sl_tp_for_pending(symbol, price, sl, tp, is_buy)
    order_type = mt5.ORDER_TYPE_BUY_LIMIT if is_buy else mt5.ORDER_TYPE_SELL_LIMIT
    margin_req = mt5.order_calc_margin(order_type, symbol, volume, price)
    if margin_req is not None:
        ok_limit, err_limit = _check_margin_pct_limit(float(margin_req))
        if not ok_limit:
            return False, (err_limit or "잔액 대비 마진 한도 초과")
    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": magic,
        "comment": comment[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    if sl_use > 0:
        request["sl"] = sl_use
    if tp_use > 0:
        request["tp"] = tp_use
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        return True, f"예약 주문 성공 {price}"
    return False, (getattr(result, "comment", None) or "unknown")


def place_pending_stop(symbol: str, action: str, volume: float, price: float, sl: float = 0.0, tp: float = 0.0, magic: int = 123456, comment: str = ""):
    """예약 스탑 주문: BUY_STOP(가격 > 현재가), SELL_STOP(가격 < 현재가). 매수가보다 높은 가격에 매수/매도가보다 낮은 가격에 매도 시 사용."""
    if not init_mt5():
        return False, "연결 실패"
    volume = _volume_to_lots(volume)
    ok_vol, err_vol = _validate_volume(volume, "예약 스탑 주문")
    if not ok_vol:
        return False, err_vol
    if not mt5.symbol_select(symbol, True):
        return False, "종목 선택 실패"
    is_buy = action.upper() == "BUY"
    point, min_dist = _symbol_point_and_stops(symbol)
    ask, bid = get_market_price(symbol)
    if ask is not None and bid is not None and min_dist > 0:
        def _round_price(p: float) -> float:
            return round(p / point) * point if point else p
        if is_buy and price < ask + min_dist:
            price = _round_price(ask + min_dist)
        elif not is_buy and price > bid - min_dist:
            price = _round_price(bid - min_dist)
    sl_use, tp_use = _normalize_sl_tp_for_pending(symbol, price, sl, tp, is_buy)
    order_type = mt5.ORDER_TYPE_BUY_STOP if is_buy else mt5.ORDER_TYPE_SELL_STOP
    margin_req = mt5.order_calc_margin(order_type, symbol, volume, price)
    if margin_req is not None:
        ok_limit, err_limit = _check_margin_pct_limit(float(margin_req))
        if not ok_limit:
            return False, (err_limit or "잔액 대비 마진 한도 초과")
    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": magic,
        "comment": comment[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    if sl_use > 0:
        request["sl"] = sl_use
    if tp_use > 0:
        request["tp"] = tp_use
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        return True, f"예약 스탑 주문 성공 {price}"
    return False, (getattr(result, "comment", None) or "unknown")


def modify_pending_order_price(order_ticket: int, new_price: float) -> Tuple[bool, str]:
    """예약 주문의 트리거 가격만 변경. order_ticket으로 주문 조회 후 price만 수정."""
    if not init_mt5():
        return False, "연결 실패"
    orders = mt5.orders_get(ticket=order_ticket)
    if not orders or len(orders) == 0:
        return False, "해당 예약 주문 없음"
    order = orders[0]
    symbol = getattr(order, "symbol", None)
    if not symbol:
        return False, "심볼 없음"
    if not mt5.symbol_select(symbol, True):
        return False, "종목 선택 실패"
    info = mt5.symbol_info(symbol)
    if info is None:
        return False, "심볼 정보 없음"
    digits = getattr(info, "digits", 2)
    point = getattr(info, "point", 0.01) or (10.0 ** (-digits) if digits else 0.01)
    price_norm = round(new_price / point) * point if point else new_price
    request = {
        "action": mt5.TRADE_ACTION_MODIFY,
        "order": order_ticket,
        "symbol": symbol,
        "volume": getattr(order, "volume_initial", getattr(order, "volume_current", 0)),
        "type": order.type,
        "price": price_norm,
        "sl": getattr(order, "sl", 0) or 0,
        "tp": getattr(order, "tp", 0) or 0,
    }
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        return True, f"가격 수정 완료 {price_norm}"
    no_changes = getattr(mt5, "TRADE_RETCODE_NO_CHANGES", 10025)
    if result and result.retcode == no_changes:
        return True, "변경 없음(이미 동일)"
    return False, (getattr(result, "comment", None) or "unknown")


def cancel_pending_orders(symbol: Optional[str] = None, magic: Optional[int] = None) -> Tuple[int, List[str]]:
    """대기 오더 삭제. symbol 지정 시 해당 심볼만, magic 지정 시 해당 매직의 오더만 삭제."""
    if not init_mt5():
        return 0, ["연결 실패"]
    orders = mt5.orders_get(symbol=symbol) if symbol else mt5.orders_get()
    if orders is None:
        return 0, []
    if magic is not None:
        orders = [o for o in orders if getattr(o, "magic", 0) == magic]
    cancelled = 0
    errors: List[str] = []
    for order in orders:
        result = mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": order.ticket})
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            cancelled += 1
        else:
            errors.append(f"#{order.ticket} {getattr(result, 'comment', '')}")
    return cancelled, errors


def modify_position_sltp(ticket: int, symbol: str, sl: float, tp: float):
    if not init_mt5():
        return False, "연결 실패"
    pos = mt5.positions_get(ticket=ticket)
    if not pos:
        return False, "포지션 없음"
    info = mt5.symbol_info(symbol)
    if info is None:
        return False, "심볼 정보 없음"
    digits = getattr(info, "digits", 2)
    point = getattr(info, "point", 0.01) or (10.0 ** (-digits) if digits else 0.01)

    # 브로커 최소 거리(stops_level) 적용
    sl_adj, tp_adj = _normalize_sl_tp_for_position(symbol, int(pos[0].type), sl, tp)

    def _norm(p: float) -> float:
        if p is None or p <= 0:
            return 0.0
        return round(p / point) * point

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": symbol,
        "position": ticket,
        "sl": _norm(sl_adj) if sl_adj and sl_adj > 0 else 0.0,
        "tp": _norm(tp_adj) if tp_adj and tp_adj > 0 else 0.0,
    }
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        return True, "SL/TP 수정 완료"
    # 변경 없음(이미 동일 SL/TP) → 실패가 아니라 성공으로 간주
    no_changes_retcode = getattr(mt5, "TRADE_RETCODE_NO_CHANGES", 10025)
    if result and (result.retcode == no_changes_retcode or "No changes" in (getattr(result, "comment", None) or "")):
        return True, "변경 없음(이미 동일)"
    return False, getattr(result, "comment", "unknown")


def close_all_positions_force(symbol: Optional[str] = None):
    if not init_mt5():
        return 0, 0, ["MT5 연결 실패"]
    positions = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
    if positions is None:
        positions = []
    ok_count = 0
    fail_count = 0
    messages = []
    for pos in positions:
        ok, msg = close_market_order(pos.symbol, pos.ticket, pos.volume, magic=getattr(pos, "magic", 123456), comment="ForceClose")
        if ok:
            ok_count += 1
            messages.append(f"✅ {pos.symbol} #{pos.ticket} {msg}")
        else:
            fail_count += 1
            messages.append(f"❌ {pos.symbol} #{pos.ticket} {msg}")
    cancelled, errs = cancel_pending_orders(symbol)
    if cancelled > 0:
        messages.append(f"예약 주문 취소: {cancelled}건")
    messages.extend(errs)
    return ok_count, fail_count, messages


def shutdown_mt5() -> None:
    try:
        mt5.shutdown()
    except Exception:
        pass
    try:
        subprocess.run(["taskkill", "/F", "/IM", "terminal64.exe"], capture_output=True, timeout=10)
    except Exception:
        pass


def start_mt5() -> bool:
    if not os.path.isfile(MT5_PATH):
        return False
    try:
        subprocess.Popen([MT5_PATH], cwd=os.path.dirname(MT5_PATH), creationflags=subprocess.DETACHED_PROCESS | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        return True
    except Exception:
        return False
