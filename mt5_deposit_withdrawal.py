"""
MT5 입출금일지 → Notion 자동 업로드 스크립트
매일 아침 7시에 실행하여 어제의 거래 기록을 노션에 업로드

매매일 기준: 어제 08:00 ~ 오늘 07:00 (KST)
예: 2월 3일 매매 = 2월 3일 08:00 ~ 2월 4일 07:00
"""
import os
import sys
import json

# Windows cp949 콘솔에서 이모지 출력 시 UnicodeEncodeError 방지
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
from typing import Any, cast
import MetaTrader5 as _mt5  # type: ignore[reportMissingImports]
mt5: Any = _mt5
import requests
from datetime import datetime, timedelta
from collections import defaultdict
try:
    from cronjob_logger import log_cronjob
except ImportError:
    def log_cronjob(job_label: str, message: str, is_error: bool = False) -> None:
        """cronjob_logger 없을 때 no-op."""
        pass

import mt5_trade_utils as tr  # tr.login_mt5, tr.get_account_info 등이 포함된 모듈
from dotenv import load_dotenv
from telegram_sender_utils import send_telegram_msg

# .env 파일 로드
load_dotenv()
# 설정
NOTION_JOURNAL_DB = "304d522f-bcca-8065-859d-c44a176dec8d"
NOTION_API_VERSION = "2025-09-03"
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_FILE = os.path.join(_SCRIPT_DIR, "accounts.json")

def load_accounts():
    # v2/accounts.json 우선, 없으면 v1/accounts.json 사용
    path = ACCOUNTS_FILE
    if not os.path.isfile(path):
        path = os.path.join(_SCRIPT_DIR, "..", "v1", "accounts.json")
    if not os.path.isfile(path):
        print("오류: 계좌 설정 파일을 찾을 수 없습니다.")
        print(f"  v2 사용: {ACCOUNTS_FILE}")
        print('  형식: [{"login": "번호", "password": "비밀번호", "server": "서버명"}, ...]')
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

ACCOUNTS = load_accounts()

def send_telegram_message(message):
    send_telegram_msg(message)

def format_status_message(balance, amount_sum, trading_date):
    """수집된 데이터를 텔레그램용 텍스트로 변환"""
    
    msg = f"📊 **MT5 입출금 {trading_date}]**\n"
    msg += f"💰 **계좌 정보**\n"
    msg += f"• 잔액: ${balance:,.2f}\n"
    msg += f"• 입출금: ${amount_sum:,.2f}\n"

    return msg

def load_notion_key():
    """노션 API 키 로드"""
    key_path = os.path.expanduser("~/.config/notion/api_key")
    if os.path.exists(key_path):
        with open(key_path, 'r') as f:
            return f.read().strip()
    return os.environ.get('NOTION_API_KEY')

def get_account_info():
    # if not init_mt5():
    #     return None
    
    account = mt5.account_info()
    if account is None:
        return None

    return {
        "login": account.login,
        "balance": account.balance,
        "equity": account.equity,
        "profit": account.profit,
        "margin": account.margin,
        "free_margin": account.margin_free,  # 이름을 'free_margin'으로 통일
        "margin_level": account.margin_level if account.margin > 0 else 0,
        "leverage": account.leverage,
        "server": account.server,
        "currency": account.currency
    }
    
    return summary

def get_trading_day_range():
    """매매일 기준 시간 범위 계산"""
    now = datetime.now()
    today_7am = now.replace(hour=7, minute=0, second=0, microsecond=0)
    yesterday_8am = today_7am - timedelta(hours=23)
    trading_date = (now - timedelta(days=1)).date()
    return yesterday_8am, today_7am, trading_date

def get_mt5_history(start_time, end_time):
    balance_sum = 0
    amount_sum = 0
    for acc_config in ACCOUNTS:
        if tr.login_mt5(acc_config['login'], acc_config['password'], acc_config['server']):
            account_info = tr.get_account_info()
            if not account_info:
                continue
            else:
                balance_sum += account_info['balance']  
                
                history_deals = mt5.history_deals_get(start_time, end_time)
                if len(history_deals) == 0:
                    print("이력이 없습니다.", mt5.last_error())
                else:
                    print(f"조회된 전체 이력 수: {len(history_deals)}개")
                    
                    print("\n--- [입출금 내역 상세] ---")
                    for deal in history_deals:
                        # [핵심] 딜 타입이 DEAL_TYPE_BALANCE(2)인 것만 필터링
                        if deal.type == mt5.DEAL_TYPE_BALANCE:
                            deal_time = datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d %H:%M:%S')
                            amount = deal.profit # 입출금 금액은 profit 필드에 기록됨
                            comment = deal.comment
                            if (0 < amount and amount < 1) or (amount > -1 and amount < 0): 
                                continue
                            type_str = "입금" if amount > 0 else "출금"
                            print(f"[{deal_time}] {type_str}: ${abs(amount):,.2f} | 메모: {comment}")
                            amount_sum += amount
    # MT5 종료
    mt5.shutdown()
    return balance_sum, amount_sum

def format_for_notion(balance, amount_sum, trading_date):
    """노션 매매일지용 데이터 포맷"""
    records = []

    record = {
        'Date': trading_date.strftime('%Y.%m.%d'),
        'Amt_USD': amount_sum,
        'Balance_USD': balance,
    }
    records.append(record)
    
    return records

# def create_summary(records, trading_date):
#     """매매성과요약 데이터 생성 (종목별 집계)"""
#     summary = defaultdict(lambda: {
#         'count': 0,
#         'profit': 0,
#         'margin': 0,
#         'types': set()
#     })
    
#     for r in records:
#         symbol = r['symbol']
#         summary[symbol]['count'] += 1
#         summary[symbol]['profit'] += r['profit']
#         summary[symbol]['margin'] += r['margin']
#         summary[symbol]['types'].add(r['type'])
    
#     result = []
#     for symbol, data in summary.items():
#         roi = data['profit'] / data['margin'] if data['margin'] > 0 else 0
#         types = '/'.join(sorted(data['types']))
#         result.append({
#             'symbol': symbol,
#             'date': trading_date.strftime('%Y-%m-%d'),
#             'type': types,
#             'count': data['count'],
#             'profit': round(data['profit'], 2),
#             'margin': round(data['margin'], 2),
#             'roi': round(roi, 4),
#         })
    
#     return result

def upload_to_notion(balance, amount_sum, trading_date):
    """
    지정한 3개 필드(Date, Amt_USD, Balance_USD)만 노션에 업로드
    """
    api_key = os.getenv("NOTION_API_KEY")
    if not api_key:
        print("❌ 오류: NOTION_API_KEY가 설정되지 않았습니다.")
        return False

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28" 
    }

    # 업로드할 필드만 정의 (노션의 필드명과 대소문자까지 일치해야 함)
    properties = {
        "Date": {
            "title": [
                {
                    "text": {
                        "content": trading_date.strftime('%Y.%m.%d')
                    }
                }
            ]
        },
        "Amt_USD": {
            "number": float(round(amount_sum * -1, 2))
        },
        "Balance_USD": {
            "number": float(round(balance, 2))
        }
    }

    payload = {
        "parent": {"database_id": NOTION_JOURNAL_DB},
        "properties": properties
    }

    try:
        response = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ 노션 업로드 성공: {trading_date}")
            return True
        else:
            # 에러 발생 시 상세 이유 출력
            error_data = response.json()
            print(f"❌ 노션 업로드 실패 ({response.status_code})")
            print(f"사유: {error_data.get('message', '알 수 없는 오류')}")
            return False
            
    except Exception as e:
        print(f"❌ API 요청 중 예외 발생: {e}")
        return False


# def upload_summary(summary_records):
#     """매매성과요약 업로드"""
#     api_key = load_notion_key()
#     if not api_key:
#         return 0
    
#     headers = {
#         "Authorization": f"Bearer {api_key}",
#         "Content-Type": "application/json",
#         "Notion-Version": NOTION_API_VERSION
#     }
    
#     success = 0
#     for record in summary_records:
#         properties = {
#             "Symbol": {"title": [{"text": {"content": record['symbol']}}]},
#             "날짜": {"date": {"start": record['date']}},
#             "Type": {"rich_text": [{"text": {"content": record['type']}}]},
#             "Number of Trade": {"number": record['count']},
#             "Profit": {"number": record['profit']},
#             "Margin": {"number": record['margin']},
#             "ROI": {"number": record['roi']},
#         }
        
#         data = {"parent": {"database_id": NOTION_SUMMARY_DB}, "properties": properties}
#         response = requests.post("https://api.notion.com/v1/pages", headers=headers, json=data)
        
#         if response.status_code == 200:
#             success += 1
#             print(f"[Summary] {record['symbol']} | Trades:{record['count']} P/L:{record['profit']} ROI:{record['roi']*100:.2f}%")
#         else:
#             print(f"[FAIL] Summary {record['symbol']} - {response.status_code}: {response.text[:100]}")
    
#     return success

def main():
    print("=== MT5 deposit withraw -> Notion Upload ===")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    send_telegram_message(f"🟡 **MT5 입출금일지** 실행 시작\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 이미 실행 중인 MT5에 연결 (다른 프로그램과 동일하게 shutdown/재시작 없음)
    if not tr.init_mt5():
        msg = "❌ **MT5 입출금일지** MT5 연결 실패. 터미널 실행 및 로그인 확인."
        print(msg)
        send_telegram_message(msg)
        send_telegram_message(f"⏹ **MT5 입출금일지** 실행 종료\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log_cronjob("MT5-입출금", msg, is_error=True)
        return

    try:
        start_time, end_time, trading_date = get_trading_day_range()
        balance, amount_sum = get_mt5_history(start_time, end_time)

        if balance is None:
            msg = "❌ **MT5 입출금일지** MT5 데이터 가져오기 실패"
            send_telegram_message(msg)
            send_telegram_message(f"⏹ **MT5 입출금일지** 실행 종료\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            log_cronjob("MT5-입출금", msg, is_error=True)
            return

        # 입출금 유무와 관계없이 잔액 정보를 노션에 업로드
        success = upload_to_notion(balance, amount_sum, trading_date)

        if success:
            report = f"📊 **MT5 입출금 기록 ({trading_date})**\n"
            report += f"• 합계 잔액: ${balance:,.2f}\n"
            report += f"• 총 입출금: ${amount_sum:,.2f}"
            if amount_sum == 0:
                report += "\n_(입출금 없음, 잔액만 기록)_"
            send_telegram_message(report)
            log_cronjob("MT5-입출금", f"✅ 업로드 완료 | 잔액 ${balance:,.2f} | 입출금 ${amount_sum:,.2f}")
            send_telegram_message(f"✅ **MT5 입출금일지** 실행 완료\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            msg = "❌ **MT5 입출금일지** 노션 업로드 API 오류"
            send_telegram_message(msg)
            log_cronjob("MT5-입출금", msg, is_error=True)
            send_telegram_message(f"⏹ **MT5 입출금일지** 실행 종료 (오류)\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 모든 작업 완료 후 M주 계좌로 다시 로그인
        acc_config = ACCOUNTS[0]
        sucess = tr.login_mt5(acc_config['login'], acc_config['password'], acc_config['server'])
        if sucess:
            print(f"주계좌{acc_config['login']}로 로그인 완료")

    except Exception as e:
        error_msg = f"❌ **MT5 입출금일지** 시스템 오류: {str(e)}"
        print(error_msg)
        send_telegram_message(error_msg)
        log_cronjob("MT5-입출금", error_msg, is_error=True)
        send_telegram_message(f"⏹ **MT5 입출금일지** 실행 종료 (예외)\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    # finally:
    #     mt5.shutdown()

if __name__ == "__main__":
    main()
