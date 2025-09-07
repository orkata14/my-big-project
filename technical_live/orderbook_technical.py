# processors/orderbook_technical.py
# --------------------------------------
# עיבוד RAW של אורדרבוק מאינטרוול אחד (snapshot מהבאפר) לשורת מדדים “נקייה” לטבלה.
# ללא תלות ב-numpy. משתמש בספריות סטנדרטיות בלבד.

from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
from statistics import median
import math
import itertools

# ---------- עזר סטטיסטי בסיסי ----------

def _percentile(values: List[float], q: float) -> float:
    """
    אחוזון בשיטת Nearest-Rank (ללא numpy).
    q בטווח [0,100]. אם הרשימה ריקה → 0.0.
    """
    if not values:
        return 0.0
    if q <= 0:
        return min(values)
    if q >= 100:
        return max(values)
    arr = sorted(values)
    k = math.ceil((q / 100.0) * len(arr)) - 1
    k = max(0, min(k, len(arr)-1))
    return arr[k]

def _nlargest_by_qty(d: Dict[float, float], n: int) -> List[Tuple[float, float]]:
    """Top-N לפי כמות (qty) מדיקט price->qty. מחזיר [(price, qty), ...]"""
    if n <= 0 or not d:
        return []
    # יעיל מבלי למיין הכול כאשר n קטן
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]

# ---------- בניית תמונות מצב ----------

def _latest_state(updates: List[Dict[str, Any]]) -> Tuple[Dict[float, float], Dict[float, float]]:
    """
    מצב אחרון (last): לכל price שומר את הכמות האחרונה שנצפתה במהלך האינטרוול.
    """
    last_bids: Dict[float, float] = {}
    last_asks: Dict[float, float] = {}
    for u in updates:
        for p, q in u.get("bids", []):
            if q > 0:
                last_bids[float(p)] = float(q)
            elif p in last_bids and q <= 0:
                # אם כמות 0 → אפשר למחוק (רמת מחיר נעלמה)
                last_bids.pop(float(p), None)
        for p, q in u.get("asks", []):
            if q > 0:
                last_asks[float(p)] = float(q)
            elif p in last_asks and q <= 0:
                last_asks.pop(float(p), None)
    return last_bids, last_asks

def _peak_state(updates: List[Dict[str, Any]]) -> Tuple[Dict[float, float], Dict[float, float]]:
    """
    מצב שיא (peak): לכל price שומר את הכמות המקסימלית שנצפתה במהלך האינטרוול.
    """
    peak_bids: Dict[float, float] = {}
    peak_asks: Dict[float, float] = {}
    for u in updates:
        for p, q in u.get("bids", []):
            p = float(p); q = float(q)
            if q <= 0:
                continue
            if p not in peak_bids or q > peak_bids[p]:
                peak_bids[p] = q
        for p, q in u.get("asks", []):
            p = float(p); q = float(q)
            if q <= 0:
                continue
            if p not in peak_asks or q > peak_asks[p]:
                peak_asks[p] = q
    return peak_bids, peak_asks

# ---------- חישובי עומק (Bands סביב mid) ----------

def _compute_bands_last(
    last_bids: Dict[float, float],
    last_asks: Dict[float, float],
    bands_bps: List[int],
) -> Dict[str, float]:
    """
    מחשב עומק כמות (qty) ברצועות סביב mid.
    - bids: כל המחירים >= mid*(1 - bps/10000) ועד mid
    - asks: כל המחירים <= mid*(1 + bps/10000) ומעל/שווה mid
    מחזיר מילון עם מפתחות בסגנון depth_bids_last_10bps, depth_asks_last_10bps, ...
    אם אין mid (צד אחד חסר) → מחזיר מילון ריק.
    """
    if not last_bids or not last_asks:
        return {}

    best_bid = max(last_bids.keys())
    best_ask = min(last_asks.keys())
    if best_ask <= best_bid:
        return {}
    mid = (best_bid + best_ask) / 2.0

    out: Dict[str, float] = {}
    for bps in bands_bps:
        band = bps / 10_000.0  # 10bps = 0.001 = 0.1%
        # ב־BID מחיר תמיד <= mid. נסכם כל BID שקרוב ל-mid בטווח הבנד.
        lower_bid_cut = mid * (1.0 - band)
        depth_bids = sum(q for p, q in last_bids.items() if lower_bid_cut <= p <= mid)

        # ב־ASK מחיר תמיד >= mid. נסכם כל ASK שקרוב ל-mid בטווח הבנד.
        upper_ask_cut = mid * (1.0 + band)
        depth_asks = sum(q for p, q in last_asks.items() if mid <= p <= upper_ask_cut)

        out[f"depth_bids_last_{bps}bps"] = float(depth_bids)
        out[f"depth_asks_last_{bps}bps"] = float(depth_asks)
    return out

# ---------- זיהוי קירות (Walls) ----------

def _detect_walls(
    levels: Dict[float, float],
    k_median: float,
    perc: int,
) -> List[Tuple[float, float]]:
    """
    קיר = כמות ברמה ≥ max(k_median * median, percentile(perc)).
    מחזיר כל הרמות שנחשבות Walls, ממוינות יורד לפי qty.
    """
    if not levels:
        return []
    qtys = list(levels.values())
    med = median(qtys) if qtys else 0.0
    thr_median = k_median * med if med > 0 else 0.0
    thr_perc = _percentile(qtys, perc)
    thr = max(thr_median, thr_perc)
    walls = [(p, q) for p, q in levels.items() if q >= thr and q > 0]
    return sorted(walls, key=lambda x: x[1], reverse=True)

# ---------- טלמטריית CHURN (תנועתיות ספר) ----------

def _churn_metrics(updates: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    סופר כמה פעמים רמות מחיר בצד BID/ASK שינו את הכמות לאורך האינטרוול.
    לא בודק ספציפית +/-, רק שינוי ערך.
    """
    levels_changed_bid = 0
    levels_changed_ask = 0
    last_seen_bid: Dict[float, float] = {}
    last_seen_ask: Dict[float, float] = {}

    for u in updates:
        # ב־BID
        for p, q in u.get("bids", []):
            p = float(p); q = float(q)
            prev = last_seen_bid.get(p)
            if prev is not None and q != prev:
                levels_changed_bid += 1
            last_seen_bid[p] = q
        # ב־ASK
        for p, q in u.get("asks", []):
            p = float(p); q = float(q)
            prev = last_seen_ask.get(p)
            if prev is not None and q != prev:
                levels_changed_ask += 1
            last_seen_ask[p] = q

    return {
        "updates_count": len(updates),
        "levels_changed_bid": levels_changed_bid,
        "levels_changed_ask": levels_changed_ask,
    }

# ---------- פונקציית העיבוד הראשית ----------

def process_orderbook(
    snapshot: dict,
    N_TOP: int = 3,
    RETURN_BANDS: bool = False,
    BANDS_BPS: List[int] = [10, 25, 50, 100],
    RETURN_WALLS: bool = False,
    WALL_MEDIAN_K: float = 3.0,
    WALL_PERCENTILE: int = 97,
    RETURN_CHURN: bool = False,
) -> dict:
    """
    קלט: snapshot כפי שמוחזר מ־OrderBookBuffer.flush():
        {
          "window_start_ts": float,
          "window_end_ts": float,
          "updates": [ { "ts": float, "bids": [(p,q),...], "asks": [(p,q),...] }, ... ]
        }
    פלט: dict עם מדדים לשורה אחת בטבלה.
    """

    updates: List[Dict[str, Any]] = snapshot.get("updates", []) or []
    window_start_ts: float = float(snapshot.get("window_start_ts", 0.0))
    window_end_ts: float = float(snapshot.get("window_end_ts", 0.0))

    # אם אין עדכונים בכלל – נחזיר שורה ריקה עם מטא־דאטה
    if not updates:
        base = {
            "time_open": window_start_ts,
            "time_close": window_end_ts,
            "total_bids_last": 0.0,
            "total_asks_last": 0.0,
            "total_bids_peak": 0.0,
            "total_asks_peak": 0.0,
            "bid_notional_last": 0.0,
            "ask_notional_last": 0.0,
            "best_bid_price": None,
            "best_ask_price": None,
            "mid_price": None,
            "spread_abs": None,
            "spread_bps": None,
            "bid_ask_ratio_last": None,
            "liq_imbalance_last": None,
            "top_bids_last": [],
            "top_asks_last": [],
            "top_bids_peak": [],
            "top_asks_peak": [],
            "top_bids_last_qty_sum": 0.0,
            "top_asks_last_qty_sum": 0.0,
            "top_bids_last_notional": 0.0,
            "top_asks_last_notional": 0.0,
        }
        if RETURN_CHURN:
            base.update({"updates_count": 0, "levels_changed_bid": 0, "levels_changed_ask": 0})
        return base

    # תמונות מצב
    last_bids, last_asks = _latest_state(updates)
    peak_bids, peak_asks = _peak_state(updates)

    # סכומים
    total_bids_last = float(sum(last_bids.values()))
    total_asks_last = float(sum(last_asks.values()))
    total_bids_peak = float(sum(peak_bids.values()))
    total_asks_peak = float(sum(peak_asks.values()))

    # נומינלי על מצב אחרון
    bid_notional_last = float(sum(p * q for p, q in last_bids.items()))
    ask_notional_last = float(sum(p * q for p, q in last_asks.items()))

    # Best / Mid / Spread
    best_bid_price = max(last_bids.keys()) if last_bids else None
    best_ask_price = min(last_asks.keys()) if last_asks else None
    if best_bid_price is not None and best_ask_price is not None and best_ask_price > best_bid_price:
        mid_price = (best_bid_price + best_ask_price) / 2.0
        spread_abs = best_ask_price - best_bid_price
        spread_bps = (spread_abs / mid_price) * 10_000.0 if mid_price > 0 else None
    else:
        mid_price, spread_abs, spread_bps = None, None, None

    # Imbalance / Ratio
    if total_asks_last > 0:
        bid_ask_ratio_last = total_bids_last / total_asks_last
    else:
        bid_ask_ratio_last = None

    denom = (total_bids_last + total_asks_last)
    liq_imbalance_last = ((total_bids_last - total_asks_last) / denom) if denom > 0 else None

    # Top-N
    top_bids_last = _nlargest_by_qty(last_bids, N_TOP)
    top_asks_last = _nlargest_by_qty(last_asks, N_TOP)
    top_bids_peak = _nlargest_by_qty(peak_bids, N_TOP)
    top_asks_peak = _nlargest_by_qty(peak_asks, N_TOP)

    top_bids_last_qty_sum = float(sum(q for _, q in top_bids_last))
    top_asks_last_qty_sum = float(sum(q for _, q in top_asks_last))
    top_bids_last_notional = float(sum(p*q for p, q in top_bids_last))
    top_asks_last_notional = float(sum(p*q for p, q in top_asks_last))

    # בסיס פלט
    out: Dict[str, Any] = {
        "time_open": window_start_ts,
        "time_close": window_end_ts,

        "total_bids_last": total_bids_last,
        "total_asks_last": total_asks_last,
        "total_bids_peak": total_bids_peak,
        "total_asks_peak": total_asks_peak,

        "bid_notional_last": bid_notional_last,
        "ask_notional_last": ask_notional_last,

        "best_bid_price": best_bid_price,
        "best_ask_price": best_ask_price,
        "mid_price": mid_price,
        "spread_abs": spread_abs,
        "spread_bps": spread_bps,

        "bid_ask_ratio_last": bid_ask_ratio_last,
        "liq_imbalance_last": liq_imbalance_last,

        "top_bids_last": top_bids_last,
        "top_asks_last": top_asks_last,
        "top_bids_peak": top_bids_peak,
        "top_asks_peak": top_asks_peak,

        "top_bids_last_qty_sum": top_bids_last_qty_sum,
        "top_asks_last_qty_sum": top_asks_last_qty_sum,
        "top_bids_last_notional": top_bids_last_notional,
        "top_asks_last_notional": top_asks_last_notional,
    }

    # Bands עומק סביב mid
    if RETURN_BANDS:
        out.update(_compute_bands_last(last_bids, last_asks, BANDS_BPS))

    # Walls זיהוי קירות (על מצב אחרון)
    if RETURN_WALLS:
        bid_walls_last = _detect_walls(last_bids, WALL_MEDIAN_K, WALL_PERCENTILE)
        ask_walls_last = _detect_walls(last_asks, WALL_MEDIAN_K, WALL_PERCENTILE)
        out.update({
            "bid_walls_last": bid_walls_last,
            "ask_walls_last": ask_walls_last,
        })

    # Churn טלמטריית
    if RETURN_CHURN:
        out.update(_churn_metrics(updates))

    return out


# ---------- דוגמת שימוש מקומית (אופציונלי) ----------
# להרצה מהירה לבדיקה ידנית: הדביקו snapshot פיקטיבי והריצו את הקובץ.
if __name__ == "__main__":
    fake_snapshot = {
        "window_start_ts": 1000.0,
        "window_end_ts": 1060.0,
        "updates": [
            {"ts": 1001.0, "bids": [(99.5, 3), (99.0, 2)], "asks": [(100.5, 1.5), (101.0, 2.2)]},
            {"ts": 1030.0, "bids": [(99.5, 4.2), (98.5, 6.0)], "asks": [(100.5, 1.0), (101.5, 5.0)]},
            {"ts": 1055.0, "bids": [(99.8, 8.0)], "asks": [(100.2, 7.0), (101.0, 1.0)]},
        ]
    }
    row = process_orderbook(
        fake_snapshot,
        N_TOP=3,
        RETURN_BANDS=True,
        BANDS_BPS=[10, 25, 50],
        RETURN_WALLS=True,
        WALL_MEDIAN_K=3.0,
        WALL_PERCENTILE=97,
        RETURN_CHURN=True,
    )
    from pprint import pprint
    pprint(row)
