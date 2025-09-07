
from typing import List, Dict, Any, Tuple, Optional, Iterable
import time
import pandas as pd

try:
    import pandas as pd  # לשימוש אופציונלי בהמרת זמן
except Exception:
    pd = None

class OrderBookBuffer:
    """
    מאחסן את כל עדכוני הספר (bids/asks) כפי שהגיעו מהוובסוקט.
    לא מבצע ממוצעים/חישובים. רק שומר ומחזיר צילומים לפי צורך.
    """

    def __init__(self) -> None:
        self._updates: List[Dict[str, Any]] = []   # [{ts: float, bids: [(p,q)], asks: [(p,q)]}, ...]
        self._window_start_ts: float = time.time() # תחילת חלון לאיסוף עבור flush()

    # ---------- Utils ----------
    @staticmethod
    def _to_epoch_seconds(t: Any) -> float:
        """
        מקבל float/int שניות יוניקס, או pandas.Timestamp, ומחזיר float שניות.
        """
        if t is None:
            return time.time()
        # pandas.Timestamp with tz
        if pd is not None and isinstance(t, pd.Timestamp):
            # If naive Timestamp, treat it as UTC-localized; otherwise convert to UTC
            try:
                if t.tzinfo is None:
                    t = t.tz_localize("UTC")
                else:
                    t = t.tz_convert("UTC")
            except Exception:
                t = pd.to_datetime(t, utc=True)
            return float(t.timestamp())
        # מספר כבר בשניות
        return float(t)

    # ---------- Write ----------
    def add_update(
        self,
        bids: List[List[float]],
        asks: List[List[float]],
        ts: Optional[float] = None,
    ) -> None:
        """
        מוסיף עדכון בודד לבאפר.
        - bids/asks בפורמט: [[price, qty], ...]
        - ts הוא timestamp (float seconds). אם None → time.time().
        """
        # normalize ts: accept None, float epoch, or pandas Timestamp (naive or tz-aware)
        epoch_ts: float
        if ts is None:
            epoch_ts = time.time()
        else:
            # handle pandas Timestamp gracefully
            if pd is not None and isinstance(ts, pd.Timestamp):
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                epoch_ts = float(ts.timestamp())
            else:
                epoch_ts = float(ts)

        self._updates.append({
            "ts": epoch_ts,
            "bids": [(float(p), float(q)) for p, q in bids],
            "asks": [(float(p), float(q)) for p, q in asks],
        })

    def add_update_dict(self, update: Dict[str, Any]) -> None:
        """
        נוחות: קבלת דיקט יחיד עם מפתחות 'bids','asks','ts'.
        """
        self.add_update(
            bids=update.get("bids", []),
            asks=update.get("asks", []),
            ts=update.get("ts", None),
        )

    # ---------- Read (non-destructive) ----------
    def last_at_or_before(self, t: Any) -> Optional[Dict[str, Any]]:
        """
        מחזיר את העדכון האחרון עם ts ≤ t (לא מאפס).
        :param t: float seconds או pandas.Timestamp
        """
        t_sec = self._to_epoch_seconds(t)
        # איטרציה מהסוף – העדכון הטרי ביותר קודם
        for upd in reversed(self._updates):
            if upd["ts"] <= t_sec:
                return upd
        return None

    def slice(self, t0: Any, t1: Any) -> List[Dict[str, Any]]:
        """
        מחזיר רשימת עדכונים בטווח [t0, t1] (לא מאפס).
        """
        start = self._to_epoch_seconds(t0)
        end   = self._to_epoch_seconds(t1)
        if end < start:
            start, end = end, start
        return [u for u in self._updates if start <= u["ts"] <= end]

    def best_bid_ask(self, t: Any) -> Optional[Tuple[float, float]]:
        """
        מחזיר (best_bid_price, best_ask_price) לפי ה-snapshot האחרון ≤ t.
        אם אין עדכון מתאים → None.
        """
        snap = self.last_at_or_before(t)
        if not snap:
            return None
        bids = snap.get("bids") or []
        asks = snap.get("asks") or []
        if not bids or not asks:
            return None
        best_bid = max(p for p, q in bids)
        best_ask = min(p for p, q in asks)
        return (best_bid, best_ask)

    # ---------- Read (destructive) ----------
    def flush(self) -> Dict[str, Any]:
        """
        מחזיר צילום (snapshot) של כל העדכונים שנאספו מאז ה-window_start_ts ומאפס.
        שימושי רק כשבאמת רוצים “לרוקן” את החלון. ברירת מחדל – עדיף לא להשתמש בו בפר נר.
        """
        snapshot: Dict[str, Any] = {
            "window_start_ts": self._window_start_ts,
            "window_end_ts": time.time(),
            "updates": self._updates,  # RAW: [{ts, bids, asks}, ...]
        }
        # איפוס לחלון הבא
        self._updates = []
        self._window_start_ts = time.time()
        return snapshot

    # ---------- Maintenance ----------
    def purge_older_than(self, cutoff_ts: Any) -> int:
        """
        מוחק עדכונים ישנים יותר מ-cutoff_ts (float seconds או pandas.Timestamp).
        מחזיר כמה נמחקו.
        """
        cutoff = self._to_epoch_seconds(cutoff_ts)
        before = len(self._updates)
        self._updates = [u for u in self._updates if u["ts"] >= cutoff]
        return before - len(self._updates)