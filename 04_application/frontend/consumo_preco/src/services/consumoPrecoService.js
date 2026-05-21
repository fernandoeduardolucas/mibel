const BASE = window.API_BASE || "http://127.0.0.1:8000";

async function _get(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status} — ${path}`);
  return res.json();
}

export function fetchOverview() {
  return _get("/api/v1/consumo-preco/overview");
}

export function fetchTimeseries(group = "day") {
  return _get(`/api/v1/consumo-preco/timeseries?group=${group}`);
}

export function fetchAnalytics() {
  return _get("/api/v1/consumo-preco/analytics");
}

export function fetchDbConnection() {
  return _get("/api/v1/consumo-preco/db-connection");
}
