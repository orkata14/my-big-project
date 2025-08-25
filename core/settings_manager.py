# core/settings_manager.py
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict
from config import SETTINGS_DEFAULTS

class SettingsManager:
    """
    מנהל ההגדרות של המערכת:
    - כרגע טוען רק את ברירות המחדל מ-config.py.
    - בהמשך נרחיב לטעינת דריסות בזמן ריצה (JSON/DB) + .env.
    """
    _cached: Dict[str, Any] | None = None

    @classmethod
    def get(cls) -> Dict[str, Any]:
        """החזר את מילון ההגדרות המאוחד (כרגע: רק Defaults)."""
        if cls._cached is None:
            cls.reload()
        return cls._cached  # type: ignore

    @classmethod
    def reload(cls) -> None:
        """טען מחדש את ההגדרות (לעתיד: נצרף גם runtime/.env)."""
        cls._cached = deepcopy(SETTINGS_DEFAULTS)

def CFG(path: str, default=None):
    """
    שליפה מהירה לפי נתיב נקודות, למשל:
        CFG("io.csv_path")  →  "data/candles_BTCUSDT_30s.csv"
    """
    cfg = SettingsManager.get()
    cur: Any = cfg
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur
