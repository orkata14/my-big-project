const ENV_URL = import.meta.env.VITE_API_URL;
const BASE_URL = ENV_URL || "http://127.0.0.1:8000";

console.log("[API] BASE_URL =", BASE_URL);

async function req(path, options = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} – ${text || "Failed to fetch"}`);
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) return res.json();
  return res.text();
}

// /list מחזיר {"runs":[...]}; מנרמלים ל-[...]
function normalizeList(data) {
  const arr = Array.isArray(data) ? data : (Array.isArray(data?.runs) ? data.runs : []);
  return arr.map((r) => ({
    id: r.key || r.id || `${(r.symbol || "").trim()}:${(r.interval || "").trim()}`,
    symbol: (r.symbol || "").trim(),
    interval: (r.interval || "").trim(),
    status: typeof r.alive === "boolean" ? (r.alive ? "running" : "stopped") : (r.status || "unknown"),
    pid: r.pid ?? null,
    raw: r,
  }));
}

export const api = {
  spawn: ({ symbol, interval }) =>
    req("/spawn", {
      method: "POST",
      body: JSON.stringify({ symbol: (symbol || "").trim(), interval: (interval || "").trim() }),
    }),

  list: async () => {
    const data = await req("/list");
    return normalizeList(data);
  },

  stop: (idOrKey) =>
    req("/stop", {
      method: "POST",
      body: JSON.stringify({ id: idOrKey, key: idOrKey }),
    }),

  latest: (symbol, interval) =>
    req(`/latest?symbol=${encodeURIComponent(symbol || "")}&interval=${encodeURIComponent(interval|| "").trim().toLowerCase()}`),
  spawn: ({ symbol, interval }) =>
         req("/spawn", { method:"POST", body: JSON.stringify({ symbol:(symbol||"").trim(), interval:(interval||"").trim().toLowerCase() })}),
  logs: (idOrKey, lines = 200) => {
    const params = new URLSearchParams({ key: idOrKey || "", lines: String(lines?? 200) });
    return req(`/logs?${params.toString()}`);
  },
};