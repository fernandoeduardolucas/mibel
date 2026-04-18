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
  return `${pageProtocol}//${pageHost}:8081`;
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
  const apiPort = port || "8081";
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

function normalizeSeriesRow(row) {
  return {
    periodo: row.periodo,
    consumo_total: row.consumo_total ?? row.consumo_total_kwh ?? 0,
    producao_total: row.producao_total ?? row.producao_total_kwh ?? 0,
    producao_pre: row.producao_pre ?? row.producao_pre_kwh ?? 0,
    producao_dgm: row.producao_dgm ?? row.producao_dgm_kwh ?? 0,
    saldo: row.saldo ?? row.saldo_kwh ?? 0,
    ratio_producao_consumo: row.ratio_producao_consumo ?? null,
    defice_horas: row.defice_horas ?? 0,
    excedente_horas: row.excedente_horas ?? 0,
    missing_horas: row.missing_horas ?? 0,
    leituras: row.leituras ?? 0,
  };
}

function buildOverviewFromSeries(series) {
  return series.reduce(
    (acc, row) => {
      acc.registos += row.leituras ?? 0;
      acc.consumo_total += row.consumo_total ?? 0;
      acc.producao_total += row.producao_total ?? 0;
      acc.saldo_total += row.saldo ?? 0;
      acc.horas_defice += row.defice_horas ?? 0;
      acc.horas_excedente += row.excedente_horas ?? 0;
      acc.horas_missing_source += row.missing_horas ?? 0;
      acc.total_pre += row.producao_pre ?? 0;
      acc.total_dgm += row.producao_dgm ?? 0;
      return acc;
    },
    {
      registos: 0,
      consumo_total: 0,
      producao_total: 0,
      saldo_total: 0,
      horas_defice: 0,
      horas_excedente: 0,
      horas_missing_source: 0,
      total_pre: 0,
      total_dgm: 0,
    },
  );
}

function normalizeAnalytics(analytics) {
  if (analytics.questao_defice) {
    return analytics;
  }

  const deficeHoras = analytics.horas_defice ?? 0;
  const totalHoras = analytics.total_horas ?? 0;
  const ratio = totalHoras > 0 ? (deficeHoras / totalHoras) * 100 : 0;

  return {
    questao_defice: {
      horas_defice: deficeHoras,
      horas_com_dados: totalHoras,
      percentual_defice: ratio,
      piores_horas: (analytics.top_10_piores_defices ?? []).map((row) => ({
        timestamp: row.timestamp_utc,
        consumo_total: row.consumo_total_kwh ?? 0,
        producao_total: row.producao_total_kwh ?? 0,
        saldo: row.saldo_kwh ?? 0,
        ratio_producao_consumo:
          row.consumo_total_kwh > 0
            ? (row.producao_total_kwh ?? 0) / row.consumo_total_kwh
            : null,
      })),
    },
    questao_dependencia_pre_dgm: {
      producao_total: analytics.total_producao_kwh ?? 0,
      producao_pre: 0,
      producao_dgm: 0,
      share_pre_percentual: 0,
      share_dgm_percentual: 0,
    },
    questao_tendencia_desbalanceamento: {
      delta_saldo_primeiro_ultimo_mes: 0,
      serie_mensal: [],
    },
  };
}

function enrichOverviewWithShares(overview) {
  const sharePre =
    overview.producao_total > 0 ? (overview.total_pre / overview.producao_total) * 100 : 0;
  const shareDgm =
    overview.producao_total > 0 ? (overview.total_dgm / overview.producao_total) * 100 : 0;
  return {
    ...overview,
    ratio_producao_consumo:
      overview.consumo_total > 0 ? overview.producao_total / overview.consumo_total : 0,
    share_pre_percentual: sharePre,
    share_dgm_percentual: shareDgm,
  };
}

async function fetchDashboardPayload(base, group) {
  const modernGroup = group === "month" ? "monthly" : "daily";
  try {
    const [analyticsResp, groupedResp] = await Promise.all([
      fetchJson(`${base}/api/v1/producao-consumo/analytics`),
      fetchJson(`${base}/api/v1/producao-consumo/${modernGroup}`),
    ]);

    const series = (groupedResp.data ?? []).map(normalizeSeriesRow);
    const analytics = normalizeAnalytics(analyticsResp.data ?? {});
    const overview = enrichOverviewWithShares(buildOverviewFromSeries(series));
    analytics.questao_dependencia_pre_dgm = {
      producao_total: overview.producao_total,
      producao_pre: overview.total_pre,
      producao_dgm: overview.total_dgm,
      share_pre_percentual: overview.share_pre_percentual,
      share_dgm_percentual: overview.share_dgm_percentual,
    };

    return { overview, series, analytics };
  } catch (error) {
    const isNotFound =
      error instanceof Error && (error.message.includes("404") || error.message.includes("endpoint_not_found"));
    if (!isNotFound) {
      throw error;
    }
  }

  const [overview, series, analytics] = await Promise.all([
    fetchJson(`${base}/api/overview`),
    fetchJson(`${base}/api/timeseries?group=${group}`),
    fetchJson(`${base}/api/analytics`),
  ]);

  return {
    overview,
    series: (series ?? []).map(normalizeSeriesRow),
    analytics: normalizeAnalytics(analytics),
  };
}

async function load() {
  const configuredBase = apiBaseInput.value.replace(/\/$/, "");
  const group = groupBySelect.value;
  statusEl.textContent = "A carregar dados...";

  const attempts = candidateBases(configuredBase);

  for (const base of attempts) {
    try {
      const { overview, series, analytics } = await fetchDashboardPayload(base, group);

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
