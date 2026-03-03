# -*- coding: utf-8 -*-
"""
공통 텔레그램 전송 유틸. 토큰/채팅ID는 .env 우선, 없으면 v1과 동일한 기본값 사용.
"""
import os

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# .env: BOT_TOKEN 또는 TELEGRAM_TOKEN, MY_USER_ID 또는 TELEGRAM_CHAT_ID (v1과 동일 기본값)
TELEGRAM_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or "8581294950:AAEghfHoxBe3KjY1zk-g7vQiqceZi5M-hkg"
TELEGRAM_CHAT_ID = os.getenv("MY_USER_ID") or os.getenv("TELEGRAM_CHAT_ID") or "7786981408"


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
