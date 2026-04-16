const kpiCards = document.getElementById("kpiCards");
const seriesRows = document.getElementById("seriesRows");
const statusEl = document.getElementById("status");
const apiBaseInput = document.getElementById("apiBase");
const groupBySelect = document.getElementById("groupBy");
const refreshButton = document.getElementById("refreshButton");

const numberFmt = new Intl.NumberFormat("pt-PT", { maximumFractionDigits: 2 });

function defaultApiBase() {
  const pageProtocol = window.location.protocol === "https:" ? "https:" : "http:";
  const pageHost = window.location.hostname || "127.0.0.1";
  return `${pageProtocol}//${pageHost}:8000`;
}

if (!apiBaseInput.value.trim()) {
  apiBaseInput.value = defaultApiBase();
}

function formatNumber(value) {
  return numberFmt.format(value ?? 0);
}

function formatCurrency(value) {
  return `${formatNumber(value)} €`;
}

function formatMwh(value) {
  return `${formatNumber(value)} MWh`;
}

function formatPrice(value) {
  return `${formatNumber(value)} €/MWh`;
}

function renderKpis(overview) {
  const cards = [
    ["Registos", formatNumber(overview.registos)],
    ["Consumo total", formatMwh(overview.consumo_total_mwh)],
    ["Custo estimado total", formatCurrency(overview.custo_estimado_total_eur)],
    ["Preço médio simples", formatPrice(overview.preco_medio_simples_eur_mwh)],
    ["Preço médio ponderado", formatPrice(overview.preco_medio_ponderado_eur_mwh)],
  ];

  kpiCards.innerHTML = cards
    .map(
      ([title, value]) =>
        `<article class="card"><h3>${title}</h3><p>${value}</p></article>`,
    )
    .join("");
}

function renderSeries(rows) {
  seriesRows.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${row.periodo}</td>
          <td>${formatNumber(row.consumo_mwh)}</td>
          <td>${formatNumber(row.custo_estimado_eur)}</td>
          <td>${formatNumber(row.preco_medio_simples_eur_mwh)}</td>
          <td>${formatNumber(row.preco_medio_ponderado_eur_mwh)}</td>
          <td>${row.leituras}</td>
        </tr>
      `,
    )
    .join("");
}

async function fetchJson(url) {
  const response = await fetch(url, {
    mode: "cors",
    referrerPolicy: "no-referrer",
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${errorBody}`);
  }

  return response.json();
}

function candidateBases(base) {
  const normalized = base.replace(/\/$/, "");
  const candidates = new Set([normalized]);

  let parsed;
  try {
    parsed = new URL(normalized);
  } catch {
    return [normalized];
  }

  const { protocol, hostname, port } = parsed;
  const apiPort = port || "8000";
  const pageHost = window.location.hostname;

  if (hostname === "localhost") {
    candidates.add(`${protocol}//127.0.0.1:${apiPort}`);
    candidates.add(`${protocol}//[::1]:${apiPort}`);
  } else if (hostname === "127.0.0.1") {
    candidates.add(`${protocol}//localhost:${apiPort}`);
    candidates.add(`${protocol}//[::1]:${apiPort}`);
  } else if (hostname === "::1" || hostname === "[::1]") {
    candidates.add(`${protocol}//localhost:${apiPort}`);
    candidates.add(`${protocol}//127.0.0.1:${apiPort}`);
  }

  if (pageHost && pageHost !== hostname) {
    candidates.add(`${protocol}//${pageHost}:${apiPort}`);
  }

  if (["localhost", "127.0.0.1", "::1", "[::1]"].includes(hostname)) {
    candidates.add(`${protocol}//host.docker.internal:${apiPort}`);
  }

  return [...candidates];
}

async function load() {
  const configuredBase = apiBaseInput.value.replace(/\/$/, "");
  const group = groupBySelect.value;
  statusEl.textContent = "A carregar dados...";

  const attempts = candidateBases(configuredBase);

  for (const base of attempts) {
    try {
      const [overview, series] = await Promise.all([
        fetchJson(`${base}/api/overview`),
        fetchJson(`${base}/api/timeseries?group=${group}`),
      ]);

      renderKpis(overview);
      renderSeries(series);

      if (base !== configuredBase) {
        apiBaseInput.value = base;
      }

      statusEl.textContent = `Atualizado em ${new Date().toLocaleString("pt-PT")}.`;
      return;
    } catch (error) {
      const isLastAttempt = base === attempts[attempts.length - 1];
      if (!isLastAttempt && error instanceof TypeError) {
        continue;
      }

      if (error instanceof TypeError) {
        statusEl.textContent =
          "Erro ao carregar dashboard: sem ligação à API. " +
          `Tentativas: ${attempts.join(", ")}. ` +
          "Confirme backend ativo (ex.: python3 04_application/backend/consumo_preco/server.py).";
        return;
      }

      statusEl.textContent = `Erro ao carregar dashboard: ${error.message}`;
      return;
    }
  }
}

refreshButton.addEventListener("click", load);
groupBySelect.addEventListener("change", load);

load();