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

function unwrapPayload(responseBody) {
  if (responseBody == null) {
    return null;
  }

  if (Object.prototype.hasOwnProperty.call(responseBody, "data")) {
    return responseBody.data;
  }

  return responseBody;
}

function toNumber(value) {
  if (value == null || value === "") {
    return 0;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeGroupedSeries(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows.map((row) => {
    const consumoTotal = toNumber(row.consumo_total ?? row.consumo_total_kwh);
    const producaoTotal = toNumber(row.producao_total ?? row.producao_total_kwh);

    return {
      ...row,
      consumo_total: consumoTotal,
      producao_total: producaoTotal,
      producao_pre: toNumber(row.producao_pre ?? row.producao_pre_kwh),
      producao_dgm: toNumber(row.producao_dgm ?? row.producao_dgm_kwh),
      saldo: toNumber(row.saldo ?? row.saldo_kwh),
      ratio_producao_consumo:
        row.ratio_producao_consumo ?? (consumoTotal > 0 ? producaoTotal / consumoTotal : null),
    };
  });
}

function normalizeAnalytics(rawAnalytics) {
  const analytics = rawAnalytics ?? {};
  const consumoTotal = toNumber(analytics.total_consumo ?? analytics.total_consumo_kwh);
  const producaoTotal = toNumber(analytics.total_producao ?? analytics.total_producao_kwh);

  return {
    resumo_geral: {
      registos: analytics.total_horas ?? analytics.registos ?? 0,
      consumo_total: consumoTotal,
      producao_total: producaoTotal,
      saldo_total: toNumber(analytics.saldo_total ?? analytics.saldo_total_kwh),
      ratio_producao_consumo:
        analytics.ratio_global_producao_consumo ?? analytics.ratio_producao_consumo ?? null,
    },
    questao_defice: {
      percentual_defice:
        analytics.percentual_defice ??
        (analytics.total_horas
          ? (toNumber(analytics.horas_defice) / toNumber(analytics.total_horas)) * 100
          : 0),
      piores_horas: (analytics.top_10_piores_defices ?? []).map((row) => {
        const consumoPiorHora = toNumber(row.consumo_total ?? row.consumo_total_kwh);
        const producaoPiorHora = toNumber(row.producao_total ?? row.producao_total_kwh);
        return {
          timestamp: row.timestamp ?? row.timestamp_utc,
          consumo_total: consumoPiorHora,
          producao_total: producaoPiorHora,
          saldo: toNumber(row.saldo ?? row.saldo_kwh),
          ratio_producao_consumo:
            row.ratio_producao_consumo ??
            (consumoPiorHora > 0 ? producaoPiorHora / consumoPiorHora : null),
        };
      }),
    },
    questao_dependencia_pre_dgm: {
      producao_total: producaoTotal,
      producao_pre: toNumber(analytics.total_producao_pre ?? analytics.total_producao_pre_kwh),
      producao_dgm: toNumber(analytics.total_producao_dgm ?? analytics.total_producao_dgm_kwh),
      share_pre_percentual: toNumber(
        analytics.share_pre_percentual ?? analytics.percentual_pre ?? analytics.peso_pre,
      ),
      share_dgm_percentual: toNumber(
        analytics.share_dgm_percentual ?? analytics.percentual_dgm ?? analytics.peso_dgm,
      ),
    },
  };
}

export async function getDashboardData({ apiBase, groupBy }) {
  const normalizedBase = resolveApiBase(apiBase);

  const [analyticsResponse, groupedResponse] = await Promise.all([
    fetchJson(`${normalizedBase}/api/v1/producao-consumo/analytics`),
    fetchJson(`${normalizedBase}/api/v1/producao-consumo/${groupBy}`),
  ]);

  const analyticsPayload = unwrapPayload(analyticsResponse);
  const groupedPayload = unwrapPayload(groupedResponse);
  const groupedRows = Array.isArray(groupedPayload)
    ? groupedPayload
    : groupedPayload?.series ??
      groupedPayload?.rows ??
      groupedPayload?.result ??
      groupedPayload?.items ??
      [];

  return {
    analytics: normalizeAnalytics(unwrapPayload(analyticsPayload)),
    groupedSeries: normalizeGroupedSeries(unwrapPayload(groupedRows)),
    apiBase: normalizedBase,
  };
}


function normalizePrediction(rawPrediction) {
  const prediction = rawPrediction ?? {};
  return {
    timestamp_referencia_utc: prediction.timestamp_referencia_utc ?? null,
    pred_flag_defice_t_plus_1: Number(prediction.pred_flag_defice_t_plus_1 ?? 0),
    prob_defice_t_plus_1: toNumber(prediction.prob_defice_t_plus_1),
    model_uri: prediction.model_uri ?? "",
  };
}

export async function getPredictionData({ apiBase }) {
  const normalizedBase = resolveApiBase(apiBase);
  const response = await fetchJson(
    `${normalizedBase}/api/v1/producao-consumo/predictions/next-hour`,
  );

  return {
    prediction: normalizePrediction(unwrapPayload(response)),
    apiBase: normalizedBase,
  };
}
