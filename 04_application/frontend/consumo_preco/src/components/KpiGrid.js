import React from "react";

function KpiCard({ label, value, unit, color }) {
  return React.createElement("div", { className: `kpi-card kpi-${color || "blue"}` },
    React.createElement("p", { className: "kpi-label" }, label),
    React.createElement("p", { className: "kpi-value" }, value != null ? value : "—"),
    unit && React.createElement("p", { className: "kpi-unit" }, unit)
  );
}

function fmt(v, dec = 0) {
  if (v == null) return "—";
  return Number(v).toLocaleString("pt-PT", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

export default function KpiGrid({ overview }) {
  if (!overview || !overview.registos) {
    return React.createElement("p", { className: "status" }, "A carregar KPIs…");
  }
  const cards = [
    { label: "Registos horários",         value: fmt(overview.registos),                        unit: "h",     color: "blue"   },
    { label: "Consumo Total",             value: fmt(overview.consumo_total_mwh, 0),             unit: "MWh",   color: "green"  },
    { label: "Custo Estimado Total",      value: fmt(overview.custo_estimado_total_eur, 0),      unit: "€",     color: "orange" },
    { label: "Preço Médio Simples",       value: fmt(overview.preco_medio_simples_eur_mwh, 2),   unit: "€/MWh", color: "purple" },
    { label: "Preço Médio Ponderado",     value: fmt(overview.preco_medio_ponderado_eur_mwh, 2), unit: "€/MWh", color: "purple" },
    { label: "Período",
      value: overview.inicio ? overview.inicio.slice(0, 10) + " → " + overview.fim.slice(0, 10) : "—",
      unit: null, color: "blue" },
  ];
  return React.createElement("section", { className: "kpis" },
    cards.map((c, i) => React.createElement(KpiCard, { key: i, ...c }))
  );
}
