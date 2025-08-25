# live_data/trade_buffer.py
from collections import deque
import pandas as pd
from typing import Deque, Dict, Any, Optional

class TradeBuffer:
    """
    שומר רשומות RAW שמגיעות מה-WS.
    עושה De-Dup לפי trade_id אם קיים, אחרת (ts, symbol, price, size, side).
    לא מבצע חישובי אינדיקטורים כאן.
    """
    def __init__(self, maxlen: int = 200_000):
        self._maxlen = maxlen  
        self._rows: Deque[Dict[str, Any]] = deque()
        self._ids:  Deque[Any]= deque()
        self._idset = set()


    @staticmethod
    def _make_key(tr: Dict[str, Any]):
        return tr.get("id") or (tr.get("ts"), tr.get("symbol"), tr.get("price"), tr.get("size"), tr.get("side"))

    def append(self, trade: Dict[str, Any]) -> None:
        k = self._make_key(trade)
        if k in self._idset:
            return
        if len(self._rows) >= self._maxlen:
            old_key = self._ids.popleft()
            self._rows.popleft()
            self._idset.discard(old_key)
            
        self._rows.append(trade)
        self._ids.append(k)
        self._idset.add(k)
        # שמירה על סט ה-keys בגודל הבופר
       #while len(self._idset) > len(self._ids):
        #   old = self._ids.popleft()
        #   self._idset.discard(old)

    def slice(self, t0: pd.Timestamp, t1: pd.Timestamp, symbol: Optional[str] = None) -> pd.DataFrame:
        """
        מחזיר DataFrame "שטוח" של כל העסקאות בטווח [t0, t1) ובסימבול (אם צוין).
        לא מוסיף כאן time=t0. זה ייעשה באגרגטור.
        """
        rows = [
            r for r in self._rows
            if r.get("ts") is not None
            and t0 <= pd.Timestamp(r["ts"]) < t1
            and (symbol is None or r.get("symbol") == symbol)
        ]
        if not rows:
            return pd.DataFrame(columns=["ts", "symbol", "price", "size", "side", "best_bid", "best_ask", "id"])
        df = pd.DataFrame(rows)
        # תקנון טיפוסים בסיסי
        if "price" in df: df["price"] = pd.to_numeric(df["price"], errors="coerce")
        if "size"  in df: df["size"]  = pd.to_numeric(df["size"],  errors="coerce")
        return df.sort_values("ts").reset_index(drop=True)

    def purge_older_than(self, cutoff: pd.Timestamp) -> None:
        """ניקוי עדין: שומר רק רשומות מה-cutoff והלאה."""
        keep = deque()
        keep_ids = deque()
        idset = set()
        for row, k in zip(self._rows, self._ids):
            if row.get("ts") is not None and pd.Timestamp(row["ts"]) >= cutoff:
                keep.append(row); keep_ids.append(k); idset.add(k)
        self._rows, self._ids, self._idset = keep, keep_ids, idset
