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

function deepUnwrapPayload(responseBody) {
  let current = responseBody;
  let guard = 0;

  while (
    current != null &&
    typeof current === "object" &&
    Object.prototype.hasOwnProperty.call(current, "data") &&
    guard < 5
  ) {
    current = unwrapPayload(current);
    guard += 1;
  }

  return current;
}

function toNumber(value) {
  if (value == null || value === "") {
    return 0;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function pickValue(source, keys, fallback = null) {
  for (const key of keys) {
    if (source?.[key] != null) {
      return source[key];
    }
  }
  return fallback;
}

function normalizeGroupedSeries(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows.map((row) => {
    const consumoTotal = toNumber(
      pickValue(row, ["consumo_total", "consumo_total_kwh", "total_consumo", "total_consumo_kwh"], 0),
    );
    const producaoTotal = toNumber(
      pickValue(
        row,
        ["producao_total", "producao_total_kwh", "total_producao", "total_producao_kwh"],
        0,
      ),
    );

    return {
      ...row,
      periodo: pickValue(row, ["periodo", "periodo_utc", "period", "chave_tempo"], "—"),
      consumo_total: consumoTotal,
      producao_total: producaoTotal,
      producao_pre: toNumber(
        pickValue(row, ["producao_pre", "producao_pre_kwh", "total_producao_pre", "total_producao_pre_kwh"], 0),
      ),
      producao_dgm: toNumber(
        pickValue(row, ["producao_dgm", "producao_dgm_kwh", "total_producao_dgm", "total_producao_dgm_kwh"], 0),
      ),
      saldo: toNumber(pickValue(row, ["saldo", "saldo_kwh", "saldo_total", "saldo_total_kwh", "total_saldo_kwh"], 0)),
      ratio_producao_consumo:
        pickValue(row, ["ratio_producao_consumo", "ratio_pc", "ratio"], null) ??
        (consumoTotal > 0 ? producaoTotal / consumoTotal : null),
      defice_horas: toNumber(pickValue(row, ["defice_horas", "horas_defice"], 0)),
      excedente_horas: toNumber(pickValue(row, ["excedente_horas", "horas_excedente"], 0)),
      leituras: toNumber(pickValue(row, ["leituras", "total_horas", "n_registos"], 0)),
    };
  });
}

function normalizeAnalytics(rawAnalytics) {
  const analytics = rawAnalytics ?? {};
  const consumoTotal = toNumber(
    pickValue(analytics, ["total_consumo", "total_consumo_kwh", "consumo_total", "consumo_total_kwh"], 0),
  );
  const producaoTotal = toNumber(
    pickValue(analytics, ["total_producao", "total_producao_kwh", "producao_total", "producao_total_kwh"], 0),
  );

  return {
    resumo_geral: {
      registos: pickValue(analytics, ["total_horas", "registos", "leituras"], 0),
      consumo_total: consumoTotal,
      producao_total: producaoTotal,
      saldo_total: toNumber(
        pickValue(analytics, ["saldo_total", "saldo_total_kwh", "total_saldo", "total_saldo_kwh"], 0),
      ),
      ratio_producao_consumo:
        pickValue(
          analytics,
          ["ratio_global_producao_consumo", "ratio_producao_consumo", "ratio_pc", "ratio"],
          null,
        ),
    },
    questao_defice: {
      percentual_defice:
        pickValue(analytics, ["percentual_defice", "taxa_defice"], null) ??
        (toNumber(pickValue(analytics, ["total_horas", "registos"], 0))
          ? (toNumber(pickValue(analytics, ["horas_defice", "defice_horas"], 0)) /
              toNumber(pickValue(analytics, ["total_horas", "registos"], 0))) *
            100
          : 0),
      piores_horas: (
        pickValue(analytics, ["top_10_piores_defices", "piores_horas", "worst_deficits"], []) ?? []
      ).map((row) => {
        const consumoPiorHora = toNumber(
          pickValue(row, ["consumo_total", "consumo_total_kwh", "total_consumo", "total_consumo_kwh"], 0),
        );
        const producaoPiorHora = toNumber(
          pickValue(row, ["producao_total", "producao_total_kwh", "total_producao", "total_producao_kwh"], 0),
        );
        return {
          timestamp: pickValue(row, ["timestamp", "timestamp_utc", "momento_utc", "periodo"], null),
          consumo_total: consumoPiorHora,
          producao_total: producaoPiorHora,
          saldo: toNumber(
            pickValue(row, ["saldo", "saldo_kwh", "saldo_total", "saldo_total_kwh", "total_saldo_kwh"], 0),
          ),
          ratio_producao_consumo:
            pickValue(row, ["ratio_producao_consumo", "ratio_pc", "ratio"], null) ??
            (consumoPiorHora > 0 ? producaoPiorHora / consumoPiorHora : null),
        };
      }),
    },
    questao_dependencia_pre_dgm: {
      producao_total: producaoTotal,
      producao_pre: toNumber(
        pickValue(
          analytics,
          ["total_producao_pre", "total_producao_pre_kwh", "producao_pre", "producao_pre_kwh"],
          0,
        ),
      ),
      producao_dgm: toNumber(
        pickValue(
          analytics,
          ["total_producao_dgm", "total_producao_dgm_kwh", "producao_dgm", "producao_dgm_kwh"],
          0,
        ),
      ),
      share_pre_percentual: toNumber(
        pickValue(analytics, ["share_pre_percentual", "percentual_pre", "peso_pre", "share_pre"], 0),
      ),
      share_dgm_percentual: toNumber(
        pickValue(analytics, ["share_dgm_percentual", "percentual_dgm", "peso_dgm", "share_dgm"], 0),
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

  const analyticsPayload = deepUnwrapPayload(analyticsResponse);
  const groupedPayload = deepUnwrapPayload(groupedResponse);
  const groupedRows = Array.isArray(groupedPayload)
    ? groupedPayload
    : groupedPayload?.series ??
      groupedPayload?.rows ??
      groupedPayload?.data ??
      groupedPayload?.data?.series ??
      groupedPayload?.data?.rows ??
      groupedPayload?.aggregates ??
      groupedPayload?.result ??
      groupedPayload?.items ??
      [];

  return {
    analytics: normalizeAnalytics(deepUnwrapPayload(analyticsPayload)),
    groupedSeries: normalizeGroupedSeries(deepUnwrapPayload(groupedRows)),
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
