# graphs/live_candles.py
# גרף נרות לייב (Matplotlib) — מצייר נר רץ ומתעדכן, וסוגר נרות כשנסגר חלון.
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Dict, List
import time

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

@dataclass
class Candle:
    t0: pd.Timestamp
    o: float
    h: float
    l: float
    c: float
    v: float
    closed: bool = False  # אם הנר נסגר סופית

class LiveCandleChart:
    def __init__(self, interval_sec: int, last_n: int = 120, title: str = "Live Candles", redraw_interval: float = 0.25):
        self.interval_sec = interval_sec
        self.last_n = last_n
        self.title = title
        self.redraw_interval = redraw_interval
        self._candles: Deque[Candle] = deque(maxlen=last_n)
        self._index: Dict[pd.Timestamp, Candle] = {}   # גישה מהירה לפי t0
        self._last_draw = 0.0

        # Matplotlib
        plt.ion()
        self._fig, self._ax = plt.subplots(figsize=(11, 5))
        self._ax.set_title(self.title)
        self._ax.set_xlabel("Time")
        self._ax.set_ylabel("Price")
        self._ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self._ax.grid(True, alpha=0.25)

    # --- API לשימוש מה-main ---

    def update_live(self, t0: pd.Timestamp, price: float, size: float = 0.0) -> None:
        """
        מעדכן/יוצר את הנר של חלון t0 בזמן אמת: אם קיים → מעדכן H/L/C (+V),
        אם לא קיים → יוצר נר פתוח עם O=H=L=C=price.
        """
        c = self._index.get(t0)
        if c is None:
            c = Candle(t0=t0, o=price, h=price, l=price, c=price, v=float(size), closed=False)
            self._push(c)
        else:
            if price > c.h: c.h = price
            if price < c.l: c.l = price
            c.c = price
            c.v += float(size) if size else 0.0

        self._redraw_throttled()

    def finalize_candle(self, t0: pd.Timestamp, o: float, h: float, l: float, c: float, v: float) -> None:
        """
        סוגר את הנר של t0 עם הערכים הסופיים מהאגרגטור.
        אם לא קיים — יוצר ומסמן כ-closed.
        """
        existing = self._index.get(t0)
        if existing is None:
            candle = Candle(t0=t0, o=o, h=h, l=l, c=c, v=v, closed=True)
            self._push(candle)
        else:
            existing.o, existing.h, existing.l, existing.c, existing.v = o, h, l, c, v
            existing.closed = True

        self._redraw(force=True)

    # --- עזר פנימי ---

    def _push(self, c: Candle) -> None:
        self._candles.append(c)
        self._index[c.t0] = c

    def _redraw_throttled(self) -> None:
        now = time.monotonic()
        if now - self._last_draw >= self.redraw_interval:
            self._redraw()
            self._last_draw = now

    def _redraw(self, force: bool = False) -> None:
        if not self._candles and not force:
            return

        self._ax.cla()
        self._ax.set_title(self.title)
        self._ax.set_xlabel("Time")
        self._ax.set_ylabel("Price")
        self._ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self._ax.grid(True, alpha=0.25)

        # המרה לוקטורים לציור מהיר
        xs: List[float] = [mdates.date2num(c.t0.to_pydatetime()) for c in self._candles]
        if not xs:
            self._fig.canvas.draw_idle()
            plt.pause(0.001)
            return

        opens  = [c.o for c in self._candles]
        highs  = [c.h for c in self._candles]
        lows   = [c.l for c in self._candles]
        closes = [c.c for c in self._candles]
        colors = ["green" if c.c >= c.o else "red" for c in self._candles]

        # רוחב נר ביחידות ימים (Matplotlib date = days)
        w = (self.interval_sec / 86400.0) * 0.8

        # ציור פתילים
        for x, h, l, col in zip(xs, highs, lows, colors):
            self._ax.vlines(x, l, h, colors=col, linewidth=1.0, alpha=0.9)

        # ציור גוף (rectangles)
        for x, o, c, col in zip(xs, opens, closes, colors):
            y0, y1 = (o, c) if c >= o else (c, o)
            self._ax.add_patch(
                plt.Rectangle((x - w/2, y0), w, max(y1 - y0, 1e-9), fill=True, alpha=0.6, edgecolor=col, facecolor=col)
            )

        # גבולות
        self._ax.set_xlim(xs[0] - w, xs[-1] + w)
        y_min = min(lows)
        y_max = max(highs)
        pad = (y_max - y_min) * 0.05 if y_max > y_min else (y_max or 1) * 0.01
        self._ax.set_ylim(y_min - pad, y_max + pad)

        self._fig.tight_layout()
        self._fig.canvas.draw_idle()
        plt.pause(0.001)
