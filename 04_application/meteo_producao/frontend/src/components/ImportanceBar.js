import React from "react";

const PRICE_QUINTILE_LABELS = [
  "Muito frio (Q1)",
  "Frio (Q2)",
  "Temperado (Q3)",
  "Quente (Q4)",
  "Muito quente (Q5)",
];

function computeQuintileAvgPrice(daily) {
  const valid = (daily ?? []).filter(
    (d) => d.temperature_mean_c != null && d.preco_spot_medio_eur_mwh != null,
  );
  if (valid.length < 10) return null;

  const sorted = [...valid].sort((a, b) => a.temperature_mean_c - b.temperature_mean_c);
  const size = Math.floor(sorted.length / 5);
  return Array.from({ length: 5 }, (_, i) => {
    const slice = sorted.slice(i * size, i === 4 ? undefined : (i + 1) * size);
    const avg = slice.reduce((s, d) => s + d.preco_spot_medio_eur_mwh, 0) / slice.length;
    return { label: PRICE_QUINTILE_LABELS[i], avg: Math.round(avg * 10) / 10 };
  });
}

export function ImportanceBar({ daily }) {
  const quintiles = computeQuintileAvgPrice(daily);

  if (!quintiles) {
    return React.createElement(
      "div", { className: "panel" },
      React.createElement("h2", null, "Preço por Quintil de Temperatura"),
      React.createElement("p", { style: { color: "var(--muted)", fontStyle: "italic" } }, "Sem dados suficientes."),
    );
  }

  const maxAvg = Math.max(...quintiles.map((q) => q.avg));

  return React.createElement(
    "div", { className: "panel" },
    React.createElement("h2", null, "Preço Médio por Quintil de Temperatura"),
    React.createElement(
      "div", { className: "bar-chart" },
      ...quintiles.map(({ label, avg }) =>
        React.createElement(
          "div", { key: label, className: "bar-row" },
          React.createElement("span", { className: "bar-label" }, label),
          React.createElement(
            "div", { className: "bar-track" },
            React.createElement("div", {
              className: "bar-fill",
              style: {
                width: `${(avg / maxAvg) * 100}%`,
                background: avg > maxAvg * 0.75 ? "#dc2626" : avg > maxAvg * 0.5 ? "#f59e0b" : "#1d4ed8",
              },
            }),
          ),
          React.createElement("span", { className: "bar-value" }, `${avg} €/MWh`),
        ),
      ),
    ),
  );
}
