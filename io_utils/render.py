# io_utils/render.py
from __future__ import annotations
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # רינדור בלי GUI
import matplotlib.pyplot as plt

def ensure_dirs(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def render_chart(
    df: pd.DataFrame,
    out_png: str,
    *,
    last_n: int = 400,
    title: str | None = None,
) -> None:
    """מצייר גרף פשוט: close + VWAP + BB + EMA12/21 אם קיימים בעמודות."""
    if df.empty:
        return
    view = df.tail(last_n)
    ensure_dirs(out_png)

    fig, ax = plt.subplots(figsize=(12, 6))
    view["close"].plot(ax=ax, label="close")

    if "vwap" in view.columns:
        view["vwap"].plot(ax=ax, label="VWAP")
    if {"bb_up","bb_mid","bb_low"}.issubset(view.columns):
        view["bb_up"].plot(ax=ax, label="BB upper", alpha=0.85)
        view["bb_mid"].plot(ax=ax, label="BB middle", alpha=0.85)
        view["bb_low"].plot(ax=ax, label="BB lower", alpha=0.85)
        ax.fill_between(view.index, view["bb_low"], view["bb_up"], alpha=0.12)
    if "ema12" in view.columns:
        view["ema12"].plot(ax=ax, label="EMA12", alpha=0.9)
    if "ema21" in view.columns:
        view["ema21"].plot(ax=ax, label="EMA21", alpha=0.9)

    ax.set_title(title or "Chart")
    ax.set_xlabel("time")
    ax.set_ylabel("price")
    ax.legend()
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close(fig)
