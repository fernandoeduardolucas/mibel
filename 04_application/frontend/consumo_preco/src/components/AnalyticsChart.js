import React from "react";

function fmt(v, dec = 2) {
  if (v == null) return "—";
  return Number(v).toLocaleString("pt-PT", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function ProfileTable({ title, rows, keyCol, keyLabel }) {
  if (!rows || rows.length === 0) return null;
  return React.createElement("article", { className: "analytics-block" },
    React.createElement("h3", null, title),
    React.createElement("div", { className: "table-wrap" },
      React.createElement("table", null,
        React.createElement("thead", null,
          React.createElement("tr", null,
            [keyLabel, "Consumo Médio (MWh)", "Preço Médio (€/MWh)"].map(h =>
              React.createElement("th", { key: h }, h)
            )
          )
        ),
        React.createElement("tbody", null,
          rows.map((r, i) =>
            React.createElement("tr", { key: i },
              React.createElement("td", null, r[keyCol]),
              React.createElement("td", { className: "num" }, fmt(r.consumo_medio_mwh, 2)),
              React.createElement("td", { className: "num" }, fmt(r.preco_medio_eur_mwh, 2))
            )
          )
        )
      )
    )
  );
}

const DIA_LABELS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];

export default function AnalyticsChart({ analytics }) {
  if (!analytics) {
    return React.createElement("p", { className: "status" }, "A carregar análises…");
  }

  const semanalComLabel = (analytics.perfil_semanal || []).map(r => ({
    ...r,
    dia_label: DIA_LABELS[r.dia_semana] ?? r.dia_semana,
  }));

  return React.createElement("section", { className: "analytics" },
    React.createElement(ProfileTable, {
      title: "Perfil Horário (média por hora do dia)",
      rows: analytics.perfil_horario,
      keyCol: "hora",
      keyLabel: "Hora",
    }),
    React.createElement(ProfileTable, {
      title: "Perfil Semanal (média por dia da semana)",
      rows: semanalComLabel,
      keyCol: "dia_label",
      keyLabel: "Dia",
    })
  );
}
