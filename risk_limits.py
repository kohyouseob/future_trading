# -*- coding: utf-8 -*-
"""
일일/주간 리스크 상한: 일일 최대 손실, 일일 최대 진입 횟수, 심볼당 동시 포지션 제한.
config.risk_limits 사용.
"""
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional

try:
    import pytz
    KST = pytz.timezone("Asia/Seoul")
except Exception:
    KST = None


def _today_kst() -> str:
    if KST:
        return datetime.now(KST).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def _week_start_kst() -> str:
    if KST:
        now = datetime.now(KST)
        week_start = now - timedelta(days=now.weekday())
        return week_start.strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


class RiskLimits:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        c = config or {}
        self.enabled = c.get("enabled", True)
        self.daily_max_loss_pct = float(c.get("daily_max_loss_pct", 5.0))
        self.daily_max_entries = int(c.get("daily_max_entries", 10))
        self.symbol_max_positions = int(c.get("symbol_max_positions", 1))
        self.weekly_max_loss_pct = float(c.get("weekly_max_loss_pct", 0.0))  # 0 = 비활성
        self._daily_entries: Dict[str, int] = {}  # date -> count
        self._daily_loss_pct: Dict[str, float] = {}  # date -> cumulative loss %
        self._weekly_loss_pct: Dict[str, float] = {}  # week_start -> cumulative loss %

    def can_enter(
        self,
        symbol: str,
        current_positions_count: int,
        balance: float,
        equity: float,
        get_daily_closed_pnl_pct: Optional[Callable[[], Optional[float]]] = None,
        get_weekly_closed_pnl_pct: Optional[Callable[[], Optional[float]]] = None,
    ) -> tuple[bool, str]:
        """
        신규 진입 가능 여부. get_daily_closed_pnl_pct() -> 오늘 청산 누적 손익을 계정 대비 %로.
        get_weekly_closed_pnl_pct() -> 이번 주 청산 누적 손익 %.
        Returns (allowed, reason).
        """
        if not self.enabled:
            return True, ""
        if current_positions_count >= self.symbol_max_positions:
            return False, f"심볼당 최대 포지션 {self.symbol_max_positions}건 초과"
        today = _today_kst()
        entry_count = self._daily_entries.get(today, 0)
        if entry_count >= self.daily_max_entries:
            return False, f"일일 최대 진입 횟수 {self.daily_max_entries}회 도달"
        if balance <= 0:
            return True, ""
        daily_loss_pct = self.daily_max_loss_pct
        if daily_loss_pct > 0 and get_daily_closed_pnl_pct:
            pnl_pct = get_daily_closed_pnl_pct()
            if pnl_pct is not None and pnl_pct <= -daily_loss_pct:
                return False, f"일일 손실 한도 {-daily_loss_pct}% 초과 (현재 {pnl_pct:.2f}%)"
        if self.weekly_max_loss_pct > 0 and get_weekly_closed_pnl_pct:
            pnl_pct = get_weekly_closed_pnl_pct()
            if pnl_pct is not None and pnl_pct <= -self.weekly_max_loss_pct:
                return False, f"주간 손실 한도 {-self.weekly_max_loss_pct}% 초과 (현재 {pnl_pct:.2f}%)"
        return True, ""

    def record_entry(self) -> None:
        """진입 실행 시 호출해 일일 진입 횟수 증가."""
        if not self.enabled:
            return
        today = _today_kst()
        self._daily_entries[today] = self._daily_entries.get(today, 0) + 1

    def set_daily_loss_pct(self, pct: float) -> None:
        """오늘 청산 누적 손익 % (외부에서 계산 후 설정 가능)."""
        today = _today_kst()
        self._daily_loss_pct[today] = pct

    def set_weekly_loss_pct(self, pct: float) -> None:
        """이번 주 청산 누적 손익 %."""
        week = _week_start_kst()
        self._weekly_loss_pct[week] = pct
