import React from "react";

const WEATHER_LABELS = {
  temperature_mean_c:      "Temperatura média (°C)",
  precipitation_total_mm:  "Precipitação total (mm)",
  wind_speed_mean_ms:      "Velocidade vento (m/s)",
  radiation_mean_wm2:      "Radiação solar (W/m²)",
  cloud_cover_mean_pct:    "Nebulosidade (%)",
};

function rColor(r) {
  if (r == null) return "";
  if (r >= 0.5)  return "positive";
  if (r <= -0.5) return "negative";
  return "";
}

export function CorrelationTable({ correlations }) {
  const rows = Object.entries(WEATHER_LABELS);

  return React.createElement(
    "div", { className: "panel" },
    React.createElement("h2", null, "Correlações de Pearson (r)"),
    React.createElement(
      "div", { className: "corr-table-wrap" },
      React.createElement(
        "table", null,
        React.createElement(
          "thead", null,
          React.createElement(
            "tr", null,
            React.createElement("th", { className: "align-left" }, "Variável Meteorológica"),
            React.createElement("th", null, "vs Produção"),
            React.createElement("th", null, "vs Preço Spot"),
          ),
        ),
        React.createElement(
          "tbody", null,
          ...rows.map(([key, label]) => {
            const rowCorr = correlations[key] ?? {};
            const rProd  = rowCorr["producao_total_daily_mwh"];
            const rPreco = rowCorr["preco_spot_medio_eur_mwh"];
            return React.createElement(
              "tr", { key },
              React.createElement("td", { className: "align-left" }, label),
              React.createElement("td", { className: rColor(rProd)  }, rProd  != null ? rProd.toFixed(3)  : "—"),
              React.createElement("td", { className: rColor(rPreco) }, rPreco != null ? rPreco.toFixed(3) : "—"),
            );
          }),
        ),
      ),
    ),
  );
}
