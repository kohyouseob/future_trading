# -*- coding: utf-8 -*-
"""KTR 랏수 조회 (ktrlots 로직·동일손실 역산). v2 독립 실행용."""
from typing import Optional, Dict, Any

CONTRACT_BY_SYMBOL = {"NAS100": 1.0, "XAUUSD": 100.0}


def calc_ktr_lots_local(
    balance: float,
    risk_pct: float,
    n_intervals: float,
    ktr_value: float,
    symbol: str,
) -> Dict[str, float]:
    if balance <= 0 or risk_pct <= 0 or ktr_value <= 0 or n_intervals < 1:
        return {k: 0.0 for k in ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"]}
    sym = (symbol or "").strip().upper().rstrip("+")
    contract = CONTRACT_BY_SYMBOL.get(sym, 1.0) or 1.0
    risk_amount = balance * (risk_pct / 100.0)
    n = max(1, min(int(n_intervals + 0.5), 10))
    risk_per_leg = risk_amount / n
    ordinals = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"]
    # 모든 심볼 소수 둘째 자리까지 (브로커 Invalid volume 방지)
    result = {}
    for i in range(1, n + 1):
        distance = (n - i + 1) * ktr_value
        if distance <= 0:
            result[ordinals[i - 1]] = 0.0
        else:
            raw = risk_per_leg / (contract * distance)
            result[ordinals[i - 1]] = round(raw, 2)
    for j in range(len(result), 10):
        result[ordinals[j]] = 0.0
    return result


def get_ktrlots_lots(
    balance: float,
    risk: float,
    n_intervals: float,
    ktr_value: float,
    symbol: str,
    headless: bool = True,
    use_local: bool = True,
) -> Optional[Dict[str, float]]:
    """use_local=True(기본): 동일손실 역산으로 로컬 계산."""
    if use_local:
        return calc_ktr_lots_local(balance, risk, n_intervals, ktr_value, symbol)
    try:
        import re
        import time
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options
        XPATH_MAP = {"NAS100": "/html/body/div/div[3]/div[3]/div[1]/div/div[2]/div[3]/div/button[1]", "XAUUSD": "/html/body/div/div[3]/div[3]/div[1]/div/div[2]/div[3]/div/button[2]"}
        BASE_XPATH = "/html/body/div/div[3]/div[3]/div[2]/div/div[2]/div/div[1]/div[2]/div[{}]/span[2]"
        opts = Options()
        opts.add_argument("--disable-blink-features=AutomationControlled")
        if headless:
            opts.add_argument("--headless")
        driver = webdriver.Chrome(options=opts)
        driver.get("https://www.ktrlots.com/")
        time.sleep(2)
        wait = WebDriverWait(driver, 10)
        for elem_id, val in [("margin", balance), ("risk", risk), ("intervals", n_intervals), ("ktr", ktr_value)]:
            el = wait.until(EC.presence_of_element_located((By.ID, elem_id)))
            el.clear()
            el.send_keys(str(val))
        if symbol in XPATH_MAP:
            wait.until(EC.element_to_be_clickable((By.XPATH, XPATH_MAP[symbol]))).click()
        time.sleep(1)
        ordinals = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th"]
        results = {}
        for i, key in enumerate(ordinals, 1):
            try:
                el = wait.until(EC.visibility_of_element_located((By.XPATH, BASE_XPATH.format(i))))
                m = re.search(r"(\d+\.?\d*)", el.text.strip())
                results[key] = round(float(m.group(1)), 2) if m else 0.0
            except Exception:
                results[key] = 0.0
        driver.quit()
        return results
    except Exception:
        return None
