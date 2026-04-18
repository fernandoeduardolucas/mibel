function withNoTrailingSlash(url) {
  return url.replace(/\/$/, "");
}

function defaultApiBase() {
  const protocol = window.location.protocol === "https:" ? "https:" : "http:";
  const hostname = window.location.hostname || "localhost";
  return `${protocol}//${hostname}:8081`;
}

export function resolveApiBase(customValue) {
  const value = customValue?.trim();
  return value ? withNoTrailingSlash(value) : defaultApiBase();
}

async function fetchJson(url) {
  const response = await fetch(url, {
    mode: "cors",
    referrerPolicy: "no-referrer",
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText} • ${body}`);
  }

  return response.json();
}

export async function getDashboardData({ apiBase, groupBy }) {
  const normalizedBase = resolveApiBase(apiBase);

  const [analyticsResponse, groupedResponse] = await Promise.all([
    fetchJson(`${normalizedBase}/api/v1/producao-consumo/analytics`),
    fetchJson(`${normalizedBase}/api/v1/producao-consumo/${groupBy}`),
  ]);

  return {
    analytics: analyticsResponse.data,
    groupedSeries: groupedResponse.data,
    apiBase: normalizedBase,
  };
}
