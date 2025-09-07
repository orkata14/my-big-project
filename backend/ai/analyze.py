#1) backend/ai/analyze.py — הדבק כך:
from typing import Optional, Dict
from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/ai", tags=["ai"])

class IndicatorScore(BaseModel):
    L: Optional[float] = Field(None, ge=0, le=100)
    S: Optional[float] = Field(None, ge=0, le=100)

class AnalyzePayload(BaseModel):
    symbol: str
    interval: str
    indicators: Dict[str, IndicatorScore]

class AnalyzeResponse(BaseModel):
    symbol: str
    interval: str
    long_pct: int
    short_pct: int
    recommendation: str
    breakdown: Dict[str, Dict[str, float]]

WEIGHTS = {"rsi":0.20,"ema":0.20,"vwap":0.20,"bb":0.15,"volume":0.15,"candle":0.10}
MIN_CONF = 60
MARGIN   = 15

@router.post("/analyze", response_model=AnalyzeResponse)
def ai_analyze(payload: AnalyzePayload) -> AnalyzeResponse:
    used = {k:v for k,v in payload.indicators.items() if k in WEIGHTS}
    if not used:
        return AnalyzeResponse(symbol=payload.symbol, interval=payload.interval,
                               long_pct=0, short_pct=0, recommendation="WAIT", breakdown={})
    total_w = sum(WEIGHTS[k] for k in used.keys())
    L=S=0.0
    breakdown={}
    for k, sc in used.items():
        w = WEIGHTS[k]/total_w
        l = float(sc.L or 0.0); s = float(sc.S or 0.0)
        L += w*l; S += w*s
        breakdown[k] = {"weight_used": round(w,3), "L": l, "S": s}
    Lp=int(round(L)); Sp=int(round(S))
    rec="WAIT"
    if max(Lp,Sp) >= MIN_CONF and abs(Lp-Sp) >= MARGIN:
        rec = "LONG" if Lp>Sp else "SHORT"
    return AnalyzeResponse(symbol=payload.symbol, interval=payload.interval,
                           long_pct=Lp, short_pct=Sp, recommendation=rec, breakdown=breakdown)
