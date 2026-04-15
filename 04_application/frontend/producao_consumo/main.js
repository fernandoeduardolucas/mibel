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

async function load() {
  const base = apiBaseInput.value.replace(/\/$/, "");
  const group = groupBySelect.value;
  statusEl.textContent = "A carregar dados...";

  try {
    const [overview, series] = await Promise.all([
      fetchJson(`${base}/api/overview`),
      fetchJson(`${base}/api/timeseries?group=${group}`),
    ]);

    renderKpis(overview);
    renderSeries(series);
    statusEl.textContent = `Atualizado em ${new Date().toLocaleString("pt-PT")}.`;
  } catch (error) {
    statusEl.textContent = `Erro ao carregar dashboard: ${error.message}`;
  }
}

refreshButton.addEventListener("click", load);
groupBySelect.addEventListener("change", load);

load();
