from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
import pandas as pd
from live_data.trade_buffer import TradeBuffer

# ---------- שעון חלונות כללי (ניתן להחלפה תוך כדי ריצה) ----------
class WindowClock:
    def __init__(self, interval_sec: int):
        self.interval_sec = int(interval_sec)
        self.t0: Optional[pd.Timestamp] = None
        self.t1: Optional[pd.Timestamp] = None

    @staticmethod
    def _floor_to_interval(ts: pd.Timestamp, interval_sec: int) -> pd.Timestamp:
        """Floor a timestamp to the interval boundary and return a UTC-aware pd.Timestamp."""
        if ts is None:
            ts = pd.Timestamp.utcnow().tz_localize("UTC")
        if ts.tz is None:
            ts = ts.tz_localize("UTC")
        epoch = int(ts.timestamp())
        base = epoch - (epoch % interval_sec)
        return pd.to_datetime(base, unit="s", utc=True)

    def ensure_started(self, now_ts: pd.Timestamp) -> None:
        if self.t0 is None:
            self.t0 = self._floor_to_interval(now_ts, self.interval_sec)
            self.t1 = self.t0 + pd.Timedelta(seconds=self.interval_sec)

    def advance_until(self, ts: pd.Timestamp) -> int:
        """Advance window boundaries until ts is before the current t1; return how many windows moved."""
        if self.t0 is None:
            self.ensure_started(ts)
            return 0
        moved = 0
        while ts >= self.t1:
            self.t0 = self.t1
            self.t1 = self.t0 + pd.Timedelta(seconds=self.interval_sec)
            moved += 1
        return moved

    def set_interval(self, interval_sec: int, now_ts: Optional[pd.Timestamp] = None) -> None:
        self.interval_sec = int(interval_sec)
        ref = now_ts or pd.Timestamp.utcnow().tz_localize("UTC")
        self.t0 = self._floor_to_interval(ref, self.interval_sec)
        self.t1 = self.t0 + pd.Timedelta(seconds=self.interval_sec)

# ---------- אגרגטור רב-פעמי (חותך כל חלון מה-Buffer) ----------
@dataclass
class CloseResult:
    t0: pd.Timestamp
    t1: pd.Timestamp
    df_chunk: pd.DataFrame  # RAW trades בחלון

class ReusableAggregator:
    """סכין רב-פעמית: בכל מעבר חלון חותך מה-Buffer את [t0,t1) ומחזיר את הצ'אנק."""
    def __init__(self, buffer: TradeBuffer, symbol: Optional[str], interval_sec: int):
        self.buffer = buffer
        self.symbol = symbol
        self.clock = WindowClock(interval_sec)

    def on_trade(self, ts: pd.Timestamp) -> list[CloseResult]:
        """
        מקבל timestamp של הטרייד האחרון, מקדם חלונות לפי הצורך,
        ומחזיר רשימת CloseResult (ייתכן יותר מאחד אם דילגנו על כמה חלונות).
        """
        # ensure we work with UTC-aware timestamps
        ts = pd.to_datetime(ts, utc=True)
        self.clock.ensure_started(ts)

        closed: list[CloseResult] = []
        moved = self.clock.advance_until(ts)
        for _ in range(moved):
            # after advance, self.clock.t0 is the new window start; the window
            # that just closed is the previous interval [t0_prev, t0)
            t0, t1 = self.clock.t0, self.clock.t1
            t0_prev = t0 - pd.Timedelta(seconds=self.clock.interval_sec)
            df_chunk = self.buffer.slice(t0_prev, t0, self.symbol)
            # ensure returned t0/t1 are UTC-aware
            closed.append(CloseResult(t0=pd.to_datetime(t0_prev, utc=True), t1=pd.to_datetime(t0, utc=True), df_chunk=df_chunk))

        return closed

    def force_close_current(self) -> CloseResult:
        """סגירה כפויה (למשל לפני כיבוי) של החלון הנוכחי עד עכשיו."""
        if self.clock.t0 is None:
            now = pd.Timestamp.utcnow().tz_localize("UTC")
            self.clock.ensure_started(now)
        t0, t1 = self.clock.t0, self.clock.t1
        df_chunk = self.buffer.slice(t0, t1, self.symbol)
        # מקדמים לחלון הבא כדי שהסכין ימשיך לעבוד אחרי force-close
        self.clock.t0 = t1
        self.clock.t1 = t1 + pd.Timedelta(seconds=self.clock.interval_sec)
        return CloseResult(t0=pd.to_datetime(t0, utc=True), t1=pd.to_datetime(t1, utc=True), df_chunk=df_chunk)

    def set_interval(self, interval_sec: int, now_ts: Optional[pd.Timestamp] = None) -> None:
        """שינוי אינטרוול בזמן אמת (30s/60s/3600s) – הסכין נשארת אותה סכין."""
        self.clock.set_interval(interval_sec, now_ts)
