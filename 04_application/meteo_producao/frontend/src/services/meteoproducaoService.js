export function resolveApiBase(override) {
  if (override && override.trim()) return override.trim();
  const host = window.location.hostname || "localhost";
  return `http://${host}:8083`;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status} — ${url}`);
  return response.json();
}

export async function getDashboardData({ apiBase, startDate, endDate }) {
  const base = resolveApiBase(apiBase);

  let dailyUrl = `${base}/api/v1/meteo-producao/daily`;
  const parts = [];
  if (startDate) parts.push(`start=${startDate}`);
  if (endDate)   parts.push(`end=${endDate}`);
  if (parts.length) dailyUrl += "?" + parts.join("&");

  const [dailyRes, analyticsRes, correlationsRes] = await Promise.all([
    fetchJson(dailyUrl),
    fetchJson(`${base}/api/v1/meteo-producao/analytics`),
    fetchJson(`${base}/api/v1/meteo-producao/correlations`),
  ]);

  return {
    apiBase: base,
    daily: dailyRes.data ?? [],
    analytics: analyticsRes.data ?? {},
    correlations: correlationsRes.data ?? {},
  };
}
