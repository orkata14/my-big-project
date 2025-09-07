import asyncio
import uvicorn
from app.server import app  # מייבא את ה־FastAPI עצמו
import sys, os
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

if __name__ == "__main__":
       # תיקון ל-Windows (מונע בעיות run_until_complete/סיגנלים)
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False, log_level="debug")