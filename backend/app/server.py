from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from .proc_manager import spawn_run, list_runs, stop_run_key, tail_log
from pydantic import BaseModel, Field
from typing import Optional, Dict
from ai.analyze import router as ai_router
# מצביע אוטומטית ל־BOT\main.py (קובץ הבוט הראשי)
BOT_MAIN_PATH = str(Path(__file__).resolve().parents[2] / "main.py")
assert Path(BOT_MAIN_PATH).exists(), f"BOT_MAIN_PATH not found: {BOT_MAIN_PATH}"
print("[server] BOT_MAIN_PATH =", BOT_MAIN_PATH)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173","http://127.0.0.1:5173"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

class SpawnReq(BaseModel):
    symbol: str
    interval: str

class StopReq(BaseModel):
    id: str | None = None
    key: str | None = None

# ─── בריאות/בדיקה מהירה ───
@app.get("/status")
@app.get("/api/status")
def status():
    return {"ok": True}

# ─── SPAWN ───
@app.post("/spawn")
@app.post("/api/spawn")
def spawn(payload: SpawnReq):
    return spawn_run(payload.symbol, payload.interval, BOT_MAIN_PATH)

# ─── LIST ───
@app.get("/list")
@app.get("/api/list")
def list_active():
    return {"runs": list_runs()}

# ─── STOP ───
@app.post("/stop")
@app.post("/api/stop")
def stop(payload: StopReq):
    id_or_key = (payload.key or payload.id or "").strip()
    if not id_or_key:
        raise HTTPException(status_code=400, detail="id or key is required")
    return stop_run_key(id_or_key)

# ─── LOGS ───
@app.get("/logs")
@app.get("/api/logs")
def logs(id: str | None = None, key: str | None = None, lines: int = 200):
    id_or_key = (key or id or "").strip()
    if not id_or_key:
        raise HTTPException(status_code=400, detail="id or key is required")
    if ":" in id_or_key:
        symbol, interval = id_or_key.split(":", 1)
    else:
        symbol, interval = id_or_key, ""
    res = tail_log(symbol.strip(), interval.strip(), n=int(lines))
    out_lines = res.get("lines") if isinstance(res, dict) else (res if isinstance(res, list) else [])
    return {"id": id_or_key, "lines": list(out_lines or [])}

# ─── LATEST ───
@app.get("/latest")
@app.get("/api/latest")
def latest(symbol: str | None = None, interval: str | None = None):
    sym = (symbol or "").strip()
    itv = (interval or "").strip().lower()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    return {"ok": True, "symbol": sym, "interval": itv}
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173","http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(ai_router)