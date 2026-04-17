const kpiCards = document.getElementById("kpiCards");
const seriesRows = document.getElementById("seriesRows");
const deficitRows = document.getElementById("deficitRows");
const mixRows = document.getElementById("mixRows");
const statusEl = document.getElementById("status");
const apiBaseInput = document.getElementById("apiBase");
const groupBySelect = document.getElementById("groupBy");
const refreshButton = document.getElementById("refreshButton");

const numberFmt = new Intl.NumberFormat("pt-PT", { maximumFractionDigits: 2 });
const percentFmt = new Intl.NumberFormat("pt-PT", { maximumFractionDigits: 2 });

function defaultApiBase() {
  const pageProtocol = window.location.protocol === "https:" ? "https:" : "http:";
  const pageHost = window.location.hostname || "127.0.0.1";
  return `${pageProtocol}//${pageHost}:8000`;
}

if (!apiBaseInput.value.trim()) {
  apiBaseInput.value = defaultApiBase();
}

function toFixed(value) {
  return numberFmt.format(value ?? 0);
}

function toPercent(value) {
  return `${percentFmt.format(value ?? 0)} %`;
}

function toRatio(value) {
  return value == null ? "—" : numberFmt.format(value);
}

function renderKpis(overview, analytics) {
  const deficit = analytics?.questao_defice ?? {};
  const cards = [
    ["Registos", overview.registos],
    ["Consumo total", `${toFixed(overview.consumo_total)} MWh`],
    ["Produção total", `${toFixed(overview.producao_total)} MWh`],
    ["Saldo total", `${toFixed(overview.saldo_total)} MWh`],
    ["Ratio global P/C", toRatio(overview.ratio_producao_consumo)],
    ["Horas em défice", overview.horas_defice],
    ["Horas em excedente", overview.horas_excedente],
    ["Horas missing source", overview.horas_missing_source],
    ["Dependência PRE", toPercent(overview.share_pre_percentual)],
    ["Dependência DGM", toPercent(overview.share_dgm_percentual)],
    ["Taxa de défice", toPercent(deficit.percentual_defice)],
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
          <td>${toFixed(row.consumo_total)}</td>
          <td>${toFixed(row.producao_total)}</td>
          <td>${toFixed(row.producao_pre)}</td>
          <td>${toFixed(row.producao_dgm)}</td>
          <td>${toFixed(row.saldo)}</td>
          <td>${toRatio(row.ratio_producao_consumo)}</td>
          <td>${row.defice_horas}</td>
          <td>${row.excedente_horas}</td>
          <td>${row.missing_horas}</td>
          <td>${row.leituras}</td>
        </tr>
      `,
    )
    .join("");
}

function renderDeficitTable(analytics) {
  const rows = analytics?.questao_defice?.piores_horas ?? [];
  deficitRows.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${row.timestamp}</td>
          <td>${toFixed(row.consumo_total)}</td>
          <td>${toFixed(row.producao_total)}</td>
          <td>${toFixed(row.saldo)}</td>
          <td>${toRatio(row.ratio_producao_consumo)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderMixTable(analytics) {
  const mix = analytics?.questao_dependencia_pre_dgm ?? {};
  const rows = [
    ["Produção total", `${toFixed(mix.producao_total)} MWh`],
    ["Produção PRE", `${toFixed(mix.producao_pre)} MWh`],
    ["Produção DGM", `${toFixed(mix.producao_dgm)} MWh`],
    ["Peso PRE", toPercent(mix.share_pre_percentual)],
    ["Peso DGM", toPercent(mix.share_dgm_percentual)],
  ];

  mixRows.innerHTML = rows
    .map(
      ([label, value]) => `
        <tr>
          <td>${label}</td>
          <td>${value}</td>
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
      const [overview, series, analytics] = await Promise.all([
        fetchJson(`${base}/api/overview`),
        fetchJson(`${base}/api/timeseries?group=${group}`),
        fetchJson(`${base}/api/analytics`),
      ]);

      renderKpis(overview, analytics);
      renderSeries(series);
      renderDeficitTable(analytics);
      renderMixTable(analytics);

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
        statusEl.textContent = `Erro ao carregar dashboard: sem ligação à API. Tentativas: ${attempts.join(", ")}. Confirme backend ativo (ex.: py 04_application/backend/producao_consumo/server.py).`;
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
