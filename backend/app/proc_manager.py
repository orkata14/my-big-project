# backend/app/proc_manager.py
# גרסה יציבה ל-MVP: ניהול ריצות, לוגים ו־state.json עם תמיכה ב־UTF-8 וב־Windows

from __future__ import annotations
import os
import sys
import json
import time
import signal
import subprocess
from typing import Dict, List, Tuple
from pathlib import Path
from collections import deque

# ─────────────────────────────────────────────────────────────
# נתיבים וקבצים
# ─────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent.parent
STATE_PATH = ROOT / "state.json"
LOGS_DIR   = ROOT / "logs"

STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# עזר: קריאה/כתיבה של מצב
# ─────────────────────────────────────────────────────────────
def _load_state() -> Dict[str, dict]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_state(state: Dict[str, dict]) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def _key(symbol: str, interval: str) -> str:
    return f"{symbol}:{interval}"

def _parse_key(id_or_key: str) -> Tuple[str, str]:
    if ":" in (id_or_key or ""):
        a, b = id_or_key.split(":", 1)
        return a.strip(), b.strip()
    return (id_or_key or "").strip(), ""

# ─────────────────────────────────────────────────────────────
# בדיקת חיים של תהליך
# ─────────────────────────────────────────────────────────────
def _is_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        import psutil  # אופציונלי, אם מותקן
        return psutil.pid_exists(pid) and psutil.Process(pid).is_running()
    except Exception:
        # Fallback: ב־POSIX os.kill(pid, 0) מאמת קיום תהליך.
        try:
            if os.name != "nt":
                os.kill(pid, 0)
                return True
            else:
                # ב־Windows בלי psutil – ננסה Tasklist (best effort)
                out = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True, text=True
                )
                return str(pid) in (out.stdout or "")
        except Exception:
            return False

# ─────────────────────────────────────────────────────────────
# רשימת ריצות
# ─────────────────────────────────────────────────────────────
def list_runs() -> List[dict]:
    state = _load_state()
    runs: List[dict] = []
    for k, meta in state.items():
        pid   = meta.get("pid")
        alive = _is_alive(pid)
        row = {
            "key": k,
            "pid": pid,
            "symbol": meta.get("symbol", ""),
            "interval": meta.get("interval", ""),
            "alive": alive,
            "status": "running" if alive else "stopped",
            "log_path": meta.get("log_path", ""),
            "started_at": meta.get("started_at"),
            "cmd": meta.get("cmd", []),
        }
        runs.append(row)
    return runs

# ─────────────────────────────────────────────────────────────
# SPawn – הרצת תהליך חדש עם לוג UTF-8 וסביבת UTF-8
# ─────────────────────────────────────────────────────────────
def spawn_run(symbol: str, interval: str, bot_main_path: str) -> dict:
    # נרמול
    symbol   = (symbol or "").strip()
    interval = (interval or "").strip().lower()
    k = _key(symbol, interval)

    state = _load_state()
    # אם קיים בריצה חיה – מחזיר הקיים
    ex = state.get(k)
    if ex and _is_alive(ex.get("pid")):
        return {"ok": True, "id": k, "key": k, "pid": ex.get("pid"), "status": "running", "meta": ex}
    # אם קיים אך מת – ננקה
    if ex and not _is_alive(ex.get("pid")):
        state.pop(k, None)

    # הכנת קובץ לוגים (UTF-8)
    log_path = (LOGS_DIR / f"{symbol}-{interval}.log").resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(log_path, "a", encoding="utf-8", errors="replace", buffering=1)

    # פקודה: python <main.py> --symbol X --interval Y
    args = [sys.executable, bot_main_path, "--symbol", symbol, "--interval", interval]

    # סביבה עם UTF-8 לפייתון הבן
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    # ספריית עבודה = תיקיית main.py
    cwd = str(Path(bot_main_path).resolve().parent)

    # דגלים ל־Windows – בלי חלון
    creation = 0
    if os.name == "nt":
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            creation |= subprocess.CREATE_NO_WINDOW
        if hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
            creation |= subprocess.CREATE_NEW_PROCESS_GROUP

    # הרצה
    proc = subprocess.Popen(
        args,
        cwd=cwd,
        stdout=log_f,
        stderr=log_f,
        stdin=subprocess.DEVNULL,
        shell=False,
        creationflags=creation,
        env=env,
    )

    meta = {
        "pid": proc.pid,
        "symbol": symbol,
        "interval": interval,
        "started_at": int(time.time()),
        "cmd": args,
        "log_path": str(log_path),
    }
    state[k] = meta
    _save_state(state)

    return {"ok": True, "id": k, "key": k, "pid": proc.pid, "status": "running", "meta": meta}

# ─────────────────────────────────────────────────────────────
# עצירת ריצה לפי key או לפי (symbol, interval)
# ─────────────────────────────────────────────────────────────
def stop_run_key(id_or_key: str) -> dict:
    symbol, interval = _parse_key(id_or_key)
    return stop_run(symbol, interval)

def stop_run(symbol: str, interval: str) -> dict:
    symbol   = (symbol or "").strip()
    interval = (interval or "").strip().lower()
    k = _key(symbol, interval)

    state = _load_state()
    meta = state.get(k)
    if not meta:
        return {"ok": False, "stopped": False, "detail": "not found", "key": k}

    pid = meta.get("pid")
    stopped = False
    try:
        if pid:
            if os.name == "nt":
                # ניסיון לעצור "יפה" ואז בכוח
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.3)
                try:
                    os.kill(pid, 0)  # עדיין חי?
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass
            stopped = True
    except Exception:
        stopped = False

    # הוצאת הריצה מהמצב (state)
    state.pop(k, None)
    _save_state(state)
    return {"ok": True, "stopped": stopped, "key": k}

# ─────────────────────────────────────────────────────────────
# קריאת לוגים – שתי גרסאות:
# 1) tail_log(symbol, interval, n) : שומר תאימות (dict עם lines)
# 2) tail_log_by_key(id_or_key, n) : מחזיר list[str] בלבד (לשרת נוח)
# ─────────────────────────────────────────────────────────────
def _resolve_log_path(symbol: str, interval: str) -> Path:
    state = _load_state()
    k = _key(symbol, interval)
    meta = state.get(k)
    if meta and meta.get("log_path"):
        return Path(meta["log_path"])
    return LOGS_DIR / f"{symbol}-{interval}.log"

def tail_log(symbol: str, interval: str, n: int = 200) -> dict:
    symbol   = (symbol or "").strip()
    interval = (interval or "").strip().lower()
    log_path = _resolve_log_path(symbol, interval)

    if not log_path.exists():
        return {"ok": False, "msg": "log file not found", "path": str(log_path)}

    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            dq = deque(f, maxlen=int(n))
        lines = [ln.rstrip("\r\n") for ln in dq]
        return {"ok": True, "path": str(log_path), "lines": lines, "count": len(lines)}
    except Exception as e:
        return {"ok": False, "error": str(e), "path": str(log_path)}

def tail_log_by_key(id_or_key: str, n: int = 200) -> List[str]:
    symbol, interval = _parse_key(id_or_key)
    res = tail_log(symbol, interval, n=n)
    if isinstance(res, dict):
        return list(res.get("lines") or [])
    if isinstance(res, list):
        return res
    if isinstance(res, str):
        return res.splitlines()[-int(n):]
    return []
