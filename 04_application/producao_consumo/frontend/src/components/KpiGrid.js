import React from "react";
import { formatNumber, formatPercent, formatRatio } from "../utils/formatters.js";

function buildKpis(analytics = {}) {
  const resumo = analytics.resumo_geral ?? {};
  const questaoDefice = analytics.questao_defice ?? {};
  const dependencia = analytics.questao_dependencia_pre_dgm ?? {};

  return [
    { label: "Registos", value: resumo.registos ?? 0 },
    { label: "Consumo Total", value: `${formatNumber(resumo.consumo_total)} MWh` },
    { label: "Produção Total", value: `${formatNumber(resumo.producao_total)} MWh` },
    { label: "Saldo Total", value: `${formatNumber(resumo.saldo_total)} MWh` },
    { label: "Rácio P/C", value: formatRatio(resumo.ratio_producao_consumo) },
    { label: "Taxa de Défice", value: formatPercent(questaoDefice.percentual_defice) },
    {
      label: "Dependência PRE",
      value: formatPercent(dependencia.share_pre_percentual),
    },
    {
      label: "Dependência DGM",
      value: formatPercent(dependencia.share_dgm_percentual),
    },
  ];
}

export function KpiGrid({ analytics }) {
  const cards = buildKpis(analytics);

  return React.createElement(
    "section",
    { className: "kpi-grid", "aria-label": "Indicadores-chave" },
    cards.map((card) =>
      React.createElement(
        "article",
        { className: "kpi-card", key: card.label },
        React.createElement("p", { className: "kpi-label" }, card.label),
        React.createElement("p", { className: "kpi-value" }, card.value),
      ),
    ),
  );
}
