import React from "react";

function fmt(v, dec = 2) {
  if (v == null) return "—";
  return Number(v).toLocaleString("pt-PT", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

export default function TimeSeriesTable({ rows }) {
  if (!rows || rows.length === 0) {
    return React.createElement("p", { className: "status" }, "Sem dados.");
  }
  return React.createElement("div", { className: "table-wrap" },
    React.createElement("table", null,
      React.createElement("thead", null,
        React.createElement("tr", null,
          ["Período", "Consumo (MWh)", "Custo Est. (€)", "Preço Simples (€/MWh)", "Preço Ponderado (€/MWh)", "Leituras"]
            .map(h => React.createElement("th", { key: h }, h))
        )
      ),
      React.createElement("tbody", null,
        rows.map((r, i) =>
          React.createElement("tr", { key: i },
            React.createElement("td", null, r.periodo),
            React.createElement("td", { className: "num" }, fmt(r.consumo_mwh, 1)),
            React.createElement("td", { className: "num" }, fmt(r.custo_estimado_eur, 0)),
            React.createElement("td", { className: "num" }, fmt(r.preco_medio_simples_eur_mwh, 2)),
            React.createElement("td", { className: "num" }, fmt(r.preco_medio_ponderado_eur_mwh, 2)),
            React.createElement("td", { className: "num" }, r.leituras)
          )
        )
      )
    )
  );
}
