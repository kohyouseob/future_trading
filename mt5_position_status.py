# -*- coding: utf-8 -*-
# import ctypes
# ctypes.windll.user32.MessageBoxW(0, "프로그램이 정상 시작되었습니다!", "알림", 1)

import io
import sys
import os
import json
import time
import psutil
from datetime import datetime, timedelta
from typing import Any, cast, Optional, Tuple
import MetaTrader5 as _mt5  # type: ignore[reportMissingImports]
mt5: Any = _mt5
import mt5_trade_utils as tr  # tr.login_mt5, tr.get_account_info 등이 포함된 모듈
from dotenv import load_dotenv

try:
    from cronjob_logger import log_cronjob
except ImportError:
    def log_cronjob(*args, **kwargs): pass

# .env 파일 로드
load_dotenv()

# 출력 인코딩 설정 (import 시 다른 모듈에서 사용할 때는 스킵 가능)
if hasattr(sys.stdout, "reconfigure"):
    try:
        cast(io.TextIOWrapper, sys.stdout).reconfigure(encoding="utf-8")
    except Exception:
        pass

def load_accounts():
    # 파일 경로 설정 (현재 스크립트와 같은 위치)
    file_path = os.path.join(os.path.dirname(__file__), "accounts.json")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

ACCOUNTS = load_accounts()
# MT5 경로: path_config.yaml → 환경변수 MT5_PATH → 기본값 (tr에서 로드)
MT5_PATH = getattr(tr, "MT5_PATH", os.environ.get("MT5_PATH", r"C:\Program Files\MetaTrader 5\terminal64.exe"))
def kill_and_restart_mt5(terminal_path):
    """
    실행 중인 terminal64.exe 프로세스를 모두 찾아 종료하고 MT5를 초기화합니다.
    """
    process_name = "terminal64.exe"
    found = False

    if mt5.initialize(path=terminal_path):
        print("🚀 MT5가 초기화 되었습니다!")
    else:
        # 1. 실행 중인 모든 프로세스 검사
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                # 프로세스 이름이 일치하는지 확인
                if proc.info['name'] == process_name:
                    print(f"⚠️ 기존 프로세스 발견 (PID: {proc.info['pid']}). 종료를 시도합니다...")
                    proc.terminate()  # 점진적 종료 시도
                    
                    # 잠시 대기 후 여전히 살아있으면 강제 종료
                    try:
                        proc.wait(timeout=3)
                    except psutil.TimeoutExpired:
                        proc.kill()
                    
                    found = True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        if found:
            print("✅ 모든 기존 프로세스가 정리되었습니다.")
            time.sleep(2)  # 시스템이 리소스를 완전히 해제할 시간을 줍니다.
        else:
            print("🔎 실행 중인 기존 프로세스가 없습니다.")

    # 2. MT5 초기화 시도
    if not mt5.initialize(path=terminal_path):
        print(f"❌ MT5 초기화 실패: {mt5.last_error()}")
        return False
    
    print("🚀 MT5가 성공적으로 시작되었습니다!")
    return True

def init_mt5():
    """MT5 초기화"""
    if not mt5.initialize(MT5_PATH):
        print(f"MT5 initialization failed: {mt5.last_error()}")
        return False
    print(f"MT5 connected: {mt5.terminal_info().name}")
    return True

def send_telegram_message(message):
    """텔레그램 메시지 전송 (telegram_sender_utils 사용)"""
    from telegram_sender_utils import send_telegram_msg
    send_telegram_msg(message)


def get_daily_pnl_from_8am() -> Tuple[Optional[float], Optional[float]]:
    """오늘 08:00(KST/로컬)부터 현재까지 거래한 일간 수익금과 수익률(잔액 대비 %) 반환.
    반환: (일간 수익금, 일간 수익률%) 또는 (None, None) 실패 시."""
    try:
        now = datetime.now()
        today_8am = now.replace(hour=8, minute=0, second=0, microsecond=0)
        if now < today_8am:
            today_8am = today_8am - timedelta(days=1)
        deals = mt5.history_deals_get(today_8am, now)
        if deals is None:
            return None, None
        # 거래(매수/매도)만 합산, 입출금(DEAL_TYPE_BALANCE=2) 제외
        DEAL_TYPE_BALANCE = getattr(mt5, "DEAL_TYPE_BALANCE", 2)
        daily_profit = 0.0
        for d in deals:
            if getattr(d, "type", 2) == DEAL_TYPE_BALANCE:
                continue
            daily_profit += getattr(d, "profit", 0) + getattr(d, "commission", 0) + getattr(d, "swap", 0)
        acc = tr.get_account_info()
        if not acc or not acc.get("balance"):
            return round(daily_profit, 2), None
        balance = float(acc["balance"])
        daily_pct = (daily_profit / balance * 100.0) if balance > 0 else None
        return round(daily_profit, 2), round(daily_pct, 2) if daily_pct is not None else None
    except Exception:
        return None, None

def get_position_status():
    """현재 로그인된 계좌의 포지션 현황 수집"""
    account_info = tr.get_account_info()
    if not account_info:
        return None
    
    # 열린 포지션 조회
    positions = mt5.positions_get()
    position_list = []
    
    if positions:
        for pos in positions:
            margin = pos.price_open * pos.volume
            if pos.symbol == 'XAUUSD':
                margin *= 100
            elif 'NAS' in pos.symbol:
                margin *= 1
            
            roi = (pos.profit / margin * 100) if margin > 0 else 0
            
            position_list.append({
                'ticket': pos.ticket,
                'symbol': pos.symbol,
                'type': 'BUY' if pos.type == 0 else 'SELL',
                'volume': pos.volume,
                'open_price': pos.price_open,
                'current_price': pos.price_current,
                'profit': pos.profit,
                'roi': roi,
                'swap': pos.swap
            })
    
    status = {
        'account': account_info,
        'positions': position_list,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    daily_profit, daily_pct = get_daily_pnl_from_8am()
    status['daily_profit'] = daily_profit
    status['daily_profit_pct'] = daily_pct
    return status

def format_status_message(status):
    """수집된 데이터를 텔레그램용 텍스트로 변환"""
    acc = status['account']
    positions = status['positions']
    
    msg = f"📊 **MT5 계좌 현황 [{acc['login']}]**\n"
    msg += f"🕐 {status['timestamp']}\n\n"
    msg += f"💰 **계좌 정보**\n"
    msg += f"• 잔액: ${acc['balance']:,.2f}\n"
    msg += f"• 평가금: ${acc['equity']:,.2f}\n"
    msg += f"• 미실현 P/L: ${acc['profit']:+,.2f}\n"
    msg += f"• 마진 레벨: {acc['margin_level']:.1f}%\n"
    daily_profit = status.get("daily_profit")
    daily_pct = status.get("daily_profit_pct")
    if daily_profit is not None:
        msg += f"• 일간 수익금(8시~): ${daily_profit:+,.2f}\n"
    if daily_pct is not None:
        msg += f"• 일간 수익률(8시~): {daily_pct:+.2f}%\n"
    msg += "\n"
    
    if positions:
        msg += f"📈 **열린 포지션** ({len(positions)}개)\n"
        for pos in positions:
            emoji = "🟢" if pos['profit'] >= 0 else "🔴"
            msg += f"\n{emoji} **{pos['symbol']}** {pos['type']} ({pos['volume']}랏)\n"
            msg += f"   • 수익: ${pos['profit']:+,.2f} ({pos['roi']:+.2f}%)\n"
    else:
        msg += "📈 **열린 포지션**: 없음\n"
    return msg

def send_status_telegram_current_account() -> bool:
    """현재 MT5에 연결된 계좌 기준으로 포지션 현황을 수집해 텔레그램 전송. 오픈 포지션이 있을 때만 전송."""
    status = get_position_status()
    if not status:
        return False
    if not status.get("positions"):
        return True  # 포지션 없음 → 전송 생략 (성공으로 간주)
    message = format_status_message(status)
    send_telegram_message(message)
    return True


def main():
    # 에러 로그 설정 (스크립트 단독 실행 시에만)
    log_path = os.path.join(os.path.dirname(__file__), "error_log.txt")
    try:
        sys.stderr = open(log_path, "a", encoding="utf-8")
    except Exception:
        pass
    # 1. 주말 체크: 일요일 전체 미실행, 토요일은 7시부터 미실행 (토요일 0~6시는 실행)
    now = datetime.now()
    if now.weekday() == 6:  # 일요일
        return
    if now.weekday() == 5 and now.hour >= 7:  # 토요일 7시 이후
        return
    print(f"=== 다중 계좌 포지션 조회 시작 ({len(ACCOUNTS)}건) ===")
    
    if kill_and_restart_mt5(MT5_PATH):
        # 여기에 매매 로직 추가
        print(f"접속 계좌: {mt5.account_info().login}")

    # 2. 계좌별 순회: 오픈 포지션이 있는 계좌만 메시지 수집 후 한 번에 전송
    messages = []
    for acc_config in ACCOUNTS:
        print(f"🔄 계좌 접속 시도: {acc_config['login']}...")
        if tr.login_mt5(acc_config['login'], acc_config['password'], acc_config['server']):
            status = get_position_status()
            if status and status.get("positions"):
                messages.append(format_status_message(status))
                acc = status['account']
                log_cronjob(f"MT5-Pos-{acc['login']}", f"✅ P/L ${acc['profit']:+,.0f}")
                print(f"✅ {acc_config['login']} 데이터 수집 완료 (포지션 {len(status['positions'])}건)")
            elif status:
                print(f"⏭️ {acc_config['login']} 오픈 포지션 없음 → 텔레그램 생략")
            else:
                print(f"❌ {acc_config['login']} 데이터 수집 실패")
        time.sleep(1)  # 계좌 전환 간 짧은 대기 (MT5 전환 안정)

    if messages:
        send_telegram_message("\n\n---\n\n".join(messages))
        print(f"📤 계좌 현황 텔레그램 1건 전송 ({len(messages)}개 계좌 포함)")

    # 모든 작업 완료 후 M주 계좌로 다시 로그인
    acc_config = ACCOUNTS[0]
    sucess = tr.login_mt5(acc_config['login'], acc_config['password'], acc_config['server'])
    if sucess:
        print(f"주계좌{acc_config['login']}로 로그인 완료")

    # 차트 패턴 알림 (발견 시 텔레그램으로 함께 전송)
    try:
        import chart_pattern_alert
        alert_messages = chart_pattern_alert.run_scan_return_alerts()
        for msg in alert_messages:
            send_telegram_message(msg)
        if alert_messages:
            print(f"✅ 차트 패턴 알림 {len(alert_messages)}건 전송 완료")
    except Exception as e:
        print(f"⚠️ 차트 패턴 알림 오류: {e}")

if __name__ == "__main__":
    main()