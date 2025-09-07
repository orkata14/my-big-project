import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../lib/api";

/* Toast קטן לשימוש פנימי */
function Toast({ open, type = "info", text, onClose }) {
  useEffect(() => {
    if (!open) return;
    const t = setTimeout(onClose, 2200);
    return () => clearTimeout(t);
  }, [open, onClose]);
  const bg =
    type === "success" ? "bg-green-600" :
    type === "error"   ? "bg-red-600"   :
                         "bg-gray-800";
  return (
    <div className={`fixed right-4 top-4 z-50 transition-opacity ${open ? "opacity-100" : "opacity-0"}`}>
      <div className={`${bg} text-white px-4 py-2 rounded-xl shadow`}>
        {text}
      </div>
    </div>
  );
}

/* ספינר קטן */
function Spinner() {
  return (
    <div className="inline-block w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin" />
  );
}

/* הוק לשמירה ב-localStorage */
function useLocalStorage(key, initialValue) {
  const [value, setValue] = useState(() => {
    try {
      const raw = localStorage.getItem(key);
      return raw != null ? JSON.parse(raw) : initialValue;
    } catch { return initialValue; }
  });
  useEffect(() => {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch {}
  }, [key, value]);
  return [value, setValue];
}

export default function Controls() {
  const [symbol, setSymbol] = useLocalStorage("symbol", "BTCUSDT");
  const [interval, setInterval_] = useLocalStorage("interval", "1m");

  const [list, setList] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [latest, setLatest] = useState(null);
  const [logs, setLogs] = useState([]);
  const [busy, setBusy] = useState(false);

  const [toastOpen, setToastOpen] = useState(false);
  const [toastText, setToastText] = useState("");
  const [toastType, setToastType] = useState("info");
  const showToast = (text, type = "info") => {
    setToastText(text); setToastType(type); setToastOpen(true);
  };

  const disabled = useMemo(() => busy, [busy]);
  const logsRef = useRef(null);

  const refresh = async () => {
    try {
      const data = await api.list();
      setList(data);
    } catch (e) {
      showToast(e.message || "list failed", "error");
    }
  };

  useEffect(() => {
    refresh();                       // טעינה ראשונה
    const t = setInterval(refresh, 5000); // רענון אוטומטי
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    // גלילה אוטומטית ללוגים בסוף
    if (logsRef.current) {
      logsRef.current.scrollTop = logsRef.current.scrollHeight;
    }
  }, [logs]);

    const doSpawn = async () => {
    const sym = (symbol || "").trim();
    const itv = (interval || "").trim();

    if (!sym || !itv) {
        showToast("מלא symbol ו־interval", "error");
        return;
    }

    setBusy(true);
    try {
        const res = await api.spawn({ symbol: sym, interval: itv });
        await refresh();
        setSelectedId(res.id || res.key || `${sym}:${itv}`); // תומך גם ב-key
        showToast(`spawned: ${res.id || res.key || `${sym}:${itv}`}`, "success");
    } catch (e) {
        showToast(`spawn failed: ${e.message}`, "error");
    } finally {
        setBusy(false);
    }
    };

  const doStop = async () => {
    if (!selectedId) return showToast("בחר מזהה מהרשימה", "error");
    if (!confirm("לעצור את התהליך הנבחר?")) return;
    setBusy(true);
    try {
      const res = await api.stop(selectedId);
      if (res.stopped) {
        showToast("stopped ✅", "success");
        await refresh();
        setSelectedId("");
      } else {
        showToast("stop failed", "error");
      }
    } catch (e) {
      showToast(`stop failed: ${e.message}`, "error");
    } finally {
      setBusy(false);
    }
  };

    const getLatest = async () => {
    const sym = (symbol || "").trim();
    const itv = (interval || "").trim().toLowerCase();

    if (!sym) {
        showToast("נא להזין SYMBOL לפני Latest", "error");
        return;
    }

    console.log("latest →", { sym, itv }); // דיבוג: מה נשלח באמת
    setBusy(true);
    try {
        const data = await api.latest(sym, itv); // שולחים גם interval מנורמל
        setLatest(data);
        showToast("latest ✓", "success");
    } catch (e) {
        showToast(`latest failed: ${e.message}`, "error");
    } finally {
        setBusy(false);
    }
    };

  const getLogs = async () => {
    if (!selectedId) return showToast("בחר מזהה מהרשימה", "error");
    setBusy(true);
    try {
      const data = await api.logs(selectedId, 250);
      setLogs(data.lines ?? []);
      showToast("logs ✓", "success");
    } catch (e) {
      showToast(`logs failed: ${e.message}`, "error");
    } finally {
      setBusy(false);
    }
  };

  const clearLogs = () => setLogs([]);

  return (
    <div className="min-h-screen w-full flex items-center justify-center bg-gray-100">
      <Toast open={toastOpen} text={toastText} type={toastType} onClose={() => setToastOpen(false)} />
      <div className="w-[980px] mx-auto p-6">
        <div className="mx-auto max-w-md rounded-2xl shadow bg-white p-6">
          <h1 className="text-xl font-bold mb-4 text-center">בקרת תהליכים</h1>

          <div className="grid grid-cols-2 gap-3 mb-4">
            <input
              className="border rounded-xl px-3 py-2"
              placeholder="SYMBOL (e.g. BTCUSDT)"
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
            />
            <input
              className="border rounded-xl px-3 py-2"
              placeholder="interval (e.g. 1m)"
              value={interval}
              onChange={(e) => setInterval_(e.target.value)}
            />
          </div>

          <div className="flex items-center gap-2 mb-4">
            <button disabled={disabled} onClick={doSpawn} className="px-4 py-2 rounded-xl bg-black text-white disabled:opacity-50">
              {busy ? <Spinner /> : "spawn"}
            </button>
            <button disabled={disabled} onClick={refresh} className="px-4 py-2 rounded-xl bg-gray-800 text-white disabled:opacity-50">
              list
            </button>
            <button disabled={disabled} onClick={doStop} className="px-4 py-2 rounded-xl bg-red-600 text-white disabled:opacity-50">
              stop
            </button>
            <button disabled={disabled} onClick={getLatest} className="px-4 py-2 rounded-xl bg-blue-600 text-white disabled:opacity-50">
              latest
            </button>
            <button disabled={disabled} onClick={getLogs} className="px-4 py-2 rounded-xl bg-indigo-600 text-white disabled:opacity-50">
              logs
            </button>
            <button disabled={disabled} onClick={clearLogs} className="ml-auto px-3 py-2 rounded-xl border">
              clear logs
            </button>
          </div>

            <div className="mb-4">
            <label className="text-sm text-gray-500">רשימת תהליכים</label>

            <div className="mt-2 max-h-56 overflow-auto grid gap-2">
                {list.length === 0 ? (
                <div className="p-3 text-sm text-gray-500 border rounded-xl">אין תהליכים</div>
                ) : (
                list.map((p) => (
                    <button
                    key={p.id}
                    type="button"
                    onClick={() => setSelectedId(p.id)}
                    className={`text-left p-3 rounded-xl border hover:bg-gray-50 transition ${
                        selectedId === p.id ? "ring-2 ring-indigo-400 bg-gray-50" : ""
                    }`}
                    >
                    <div className="font-mono text-xs mb-1">id: {p.id}</div>
                    <div className="text-sm">
                        symbol: {p.symbol} · interval: {p.interval || "-"}
                    </div>
                    <div className={`text-xs mt-1 ${p.status === "running" ? "text-green-600" : "text-gray-600"}`}>
                        status: {p.status}
                    </div>
                    </button>
                ))
                )}
            </div>

            {selectedId && (
                <div className="mt-2 text-xs text-gray-500">נבחר: {selectedId}</div>
            )}
            </div>

          <div className="mb-4">
            <label className="text-sm text-gray-500">Latest</label>
            <pre className="mt-1 p-3 bg-gray-50 border rounded-xl max-h-52 overflow-auto text-xs">
{JSON.stringify(latest, null, 2)}
            </pre>
          </div>

          <div>
            <label className="text-sm text-gray-500">Logs</label>
            <pre ref={logsRef} className="mt-1 p-3 bg-gray-50 border rounded-xl max-h-60 overflow-auto text-xs whitespace-pre-wrap">
{logs.join("\n")}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}
