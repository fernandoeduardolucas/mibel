import React from "react";

function KpiCard({ label, value, unit, colorClass }) {
  return React.createElement(
    "div", { className: "kpi-card" },
    React.createElement("p", { className: "kpi-label" }, label),
    React.createElement(
      "p", { className: `kpi-value ${colorClass ?? ""}` },
      value ?? "—",
      unit ? React.createElement("span", { className: "kpi-unit" }, unit) : null,
    ),
  );
}

function fmt(value, decimals = 1) {
  if (value == null) return "—";
  return Number(value).toFixed(decimals);
}

export function KpiGrid({ analytics }) {
  const cards = [
    { label: "Temperatura Média",    value: fmt(analytics.temperatura_media_c),        unit: "°C",    colorClass: "neutral" },
    { label: "Precipitação Média",   value: fmt(analytics.precipitacao_media_mm),       unit: "mm/dia", colorClass: "neutral" },
    { label: "Vento Médio",          value: fmt(analytics.vento_medio_ms),              unit: "m/s",   colorClass: "neutral" },
    { label: "Radiação Média",       value: fmt(analytics.radiacao_media_wm2, 0),       unit: "W/m²",  colorClass: "neutral" },
    { label: "Produção Média Diária",value: fmt(analytics.producao_media_diaria_mwh, 0), unit: "MWh",  colorClass: "positive" },
    { label: "Preço Médio Spot",     value: fmt(analytics.preco_medio_eur_mwh),         unit: "€/MWh", colorClass: "neutral" },
    { label: "Dias Analisados",      value: analytics.total_dias ?? "—",               unit: "dias",  colorClass: "neutral" },
  ];

  return React.createElement(
    "div", { className: "kpi-grid" },
    ...cards.map((card, i) => React.createElement(KpiCard, { key: i, ...card })),
  );
}
