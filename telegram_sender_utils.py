# -*- coding: utf-8 -*-
"""
공통 텔레그램 전송 유틸. 토큰/채팅ID는 .env에서만 읽음 (코드에 하드코딩하지 않음).
"""
import os

import requests

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_SCRIPT_DIR, ".env"))
except ImportError:
    pass

# .env: TELEGRAM_TOKEN (또는 BOT_TOKEN), TELEGRAM_CHAT_ID (또는 MY_USER_ID) — 반드시 .env에 설정
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("MY_USER_ID") or "").strip()


def send_telegram_msg(message: str, parse_mode: str = "Markdown") -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"텔레그램 전송 에러: {e}")
        return False


def send_telegram_photo(caption: str, photo_path: str, parse_mode: str = "Markdown") -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not os.path.isfile(photo_path):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(photo_path, "rb") as f:
            r = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": parse_mode},
                files={"photo": f},
                timeout=15,
            )
        return r.status_code == 200
    except Exception as e:
        print(f"텔레그램 사진 전송 에러: {e}")
        return False
