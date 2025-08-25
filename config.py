# config.py
# ---------
# קובץ הגדרות ברירות-מחדל (Defaults) לכל המערכת.
# הקוד הראשי והפייפליינים לא מחזיקים מספרים קשיחים – הכול מגיע מכאן.

SETTINGS_DEFAULTS = {
    # ----- ליבה -----
    "symbol": "BTCUSDT",
    "interval_sec": 30,                 # גודל נר בשניות

    # ----- קבצים/פלט -----
    # שים לב: כרגע הנתיבים “קשיחים” לפי הסימבול והאינטרבל שלמעלה.
    # אם תשנה symbol/interval_sec – עדכן גם את שני הנתיבים האלו, או שנוסיף בהמשך לוגיקה דינמית.
    "io": {
        "csv_path": "data/candles_BTCUSDT_30s.csv",
        "png_path": "data/chart_BTCUSDT_30s.png",
    },

    # ----- גרף -----
    "chart": {
        "max_points": 400,              # כמה נקודות אחרונות לציור
    },

    # ----- אינדיקטורים בסיסיים -----
    # add_all_indicators יקרא את זה (למשל EMA5/12/21, BB, VWAP)
    "indicators": {
        "ema_periods": [5, 12, 21],
        "bb_window": 20,
        "bb_num_std": 2.0,
        # "vwap_source": "close",       # אופציונלי אם תרצה לשלוט במקור VWAP
    },

    # ----- שכבת Technical -----
    # add_all_technical יקרא את זה (VWAP Tech, BB Tech, EMA Tech)
    "technical": {
        "mode": "stream",               # "stream" לנר אחרון / "batch" לכל הטבלה
        "ema_pairs": [(12, 21)],        # זוגות EMA לניתוח TECH
        "vwap_on_tol_pct": 0.02,        # אחוז סטייה להיחשב "ON VWAP"
        "bb_window": 20,
        "bb_num_std": 2.0,
        # "price_col": "close",         # אופציונלי ל-EMA Tech אם תרצה להחליף מקור מחיר
    },
}
