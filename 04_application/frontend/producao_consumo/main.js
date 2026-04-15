const kpiCards = document.getElementById("kpiCards");
const seriesRows = document.getElementById("seriesRows");
const statusEl = document.getElementById("status");
const apiBaseInput = document.getElementById("apiBase");
const groupBySelect = document.getElementById("groupBy");
const refreshButton = document.getElementById("refreshButton");

const numberFmt = new Intl.NumberFormat("pt-PT", { maximumFractionDigits: 2 });
const percentFmt = new Intl.NumberFormat("pt-PT", { maximumFractionDigits: 2 });

function toFixed(value) {
  return numberFmt.format(value ?? 0);
}

function renderKpis(overview) {
  const cards = [
    ["Registos", overview.registos],
    ["Consumo total", `${toFixed(overview.consumo_total)} MWh`],
    ["Produção total", `${toFixed(overview.producao_total)} MWh`],
    ["Saldo total", `${toFixed(overview.saldo_total)} MWh`],
    ["Cobertura", `${percentFmt.format(overview.cobertura_percentual)} %`],
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
          <td>${toFixed(row.saldo)}</td>
          <td>${row.leituras}</td>
        </tr>
      `,
    )
    .join("");
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${errorBody}`);
  }
  return response.json();
}

function candidateBases(base) {
  const normalized = base.replace(/\/$/, "");
  const candidates = [normalized];

  if (normalized.includes("localhost")) {
    candidates.push(normalized.replace("localhost", "127.0.0.1"));
  }

  return [...new Set(candidates)];
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
        statusEl.textContent = `Erro ao carregar dashboard: sem ligação à API em ${configuredBase}. Dica: se estiver em Windows/IPv6, experimente http://127.0.0.1:8000 e confirme backend ativo (ex.: py 04_application/backend/producao_consumo/server.py).`;
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
