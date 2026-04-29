import React from "react";
import { formatDateTime, formatNumber, formatPercent, formatRatio } from "../utils/formatters.js";

function deficitRows(analytics = {}) {
  return analytics.questao_defice?.piores_horas ?? [];
}

function dependenciaRows(analytics = {}) {
  const dependencia = analytics.questao_dependencia_pre_dgm ?? {};

  return [
    ["Produção Total", `${formatNumber(dependencia.producao_total)} MWh`],
    ["Produção PRE", `${formatNumber(dependencia.producao_pre)} MWh`],
    ["Produção DGM", `${formatNumber(dependencia.producao_dgm)} MWh`],
    ["Peso PRE", formatPercent(dependencia.share_pre_percentual)],
    ["Peso DGM", formatPercent(dependencia.share_dgm_percentual)],
  ];
}

function tableCell(content, className) {
  return React.createElement("td", { className }, content);
}

export function DataTables({ analytics, groupedSeries, predictionNextHour }) {
  const [currentPage, setCurrentPage] = React.useState(1);
  const PAGE_SIZE = 10;
  const worstDeficits = deficitRows(analytics);
  const dependency = dependenciaRows(analytics);
  const totalPages = Math.max(1, Math.ceil(groupedSeries.length / PAGE_SIZE));
  const safePage = Math.min(currentPage, totalPages);
  const startIndex = (safePage - 1) * PAGE_SIZE;
  const endIndex = startIndex + PAGE_SIZE;
  const paginatedSeries = groupedSeries.slice(startIndex, endIndex);

  React.useEffect(() => {
    setCurrentPage(1);
  }, [groupedSeries.length]);

  return React.createElement(
    React.Fragment,
    null,
    React.createElement(
      "section",
      { className: "grid-layout" },
      React.createElement(
        "article",
        { className: "panel" },
        React.createElement("h2", null, "Top défices horários"),
        React.createElement(
          "div",
          { className: "table-wrapper" },
          React.createElement(
            "table",
            null,
            React.createElement(
              "thead",
              null,
              React.createElement(
                "tr",
                null,
                React.createElement("th", null, "Momento"),
                React.createElement("th", null, "Consumo"),
                React.createElement("th", null, "Produção"),
                React.createElement("th", null, "Saldo"),
                React.createElement("th", null, "Rácio P/C"),
              ),
            ),
            React.createElement(
              "tbody",
              null,
              worstDeficits.map((row, index) =>
                React.createElement(
                  "tr",
                  { key: `${row.timestamp}-${index}` },
                  tableCell(formatDateTime(row.timestamp), "align-left"),
                  tableCell(formatNumber(row.consumo_total)),
                  tableCell(formatNumber(row.producao_total)),
                  tableCell(formatNumber(row.saldo)),
                  tableCell(formatRatio(row.ratio_producao_consumo)),
                ),
              ),
            ),
          ),
        ),
      ),
      React.createElement(
        "article",
        { className: "panel" },
        React.createElement("h2", null, "Dependência PRE/DGM"),
        React.createElement(
          "div",
          { className: "table-wrapper" },
          React.createElement(
            "table",
            null,
            React.createElement(
              "tbody",
              null,
              dependency.map(([label, value]) =>
                React.createElement(
                  "tr",
                  { key: label },
                  tableCell(label, "align-left"),
                  tableCell(value),
                ),
              ),
            ),
          ),
        ),
      ),
      React.createElement(PredictionModelCard, { predictionNextHour }),
    ),
    React.createElement(
      "section",
      { className: "panel" },
      React.createElement("h2", null, "Série temporal agregada"),
      React.createElement(
        "div",
        { className: "table-wrapper" },
        React.createElement(
          "table",
          null,
          React.createElement(
            "thead",
            null,
            React.createElement(
              "tr",
              null,
              React.createElement("th", null, "Período"),
              React.createElement("th", null, "Consumo"),
              React.createElement("th", null, "Produção"),
              React.createElement("th", null, "Saldo"),
              React.createElement("th", null, "Rácio P/C"),
              React.createElement("th", null, "Horas défice"),
              React.createElement("th", null, "Horas excedente"),
              React.createElement("th", null, "Leituras"),
            ),
          ),
          React.createElement(
            "tbody",
            null,
            paginatedSeries.map((row, index) =>
              React.createElement(
                "tr",
                { key: `${row.periodo}-${index}` },
                tableCell(row.periodo, "align-left"),
                tableCell(formatNumber(row.consumo_total)),
                tableCell(formatNumber(row.producao_total)),
                tableCell(formatNumber(row.saldo)),
                tableCell(formatRatio(row.ratio_producao_consumo)),
                tableCell(row.defice_horas ?? 0),
                tableCell(row.excedente_horas ?? 0),
                tableCell(row.leituras ?? 0),
              ),
            ),
          ),
        ),
      ),
      React.createElement(
        "div",
        { className: "pagination-controls" },
        React.createElement(
          "button",
          { type: "button", onClick: () => setCurrentPage((page) => Math.max(1, page - 1)), disabled: safePage === 1 },
          "Anterior",
        ),
        React.createElement("span", null, `Página ${safePage} de ${totalPages}`),
        React.createElement(
          "button",
          { type: "button", onClick: () => setCurrentPage((page) => Math.min(totalPages, page + 1)), disabled: safePage === totalPages },
          "Próxima",
        ),
      ),
    ),
  );
}

function PredictionModelCard({ predictionNextHour }) {
  return React.createElement(
    "article",
    { className: "panel" },
    React.createElement("h2", null, "Modelos de IA (t+1h)"),
    React.createElement(
      "div",
      { className: "table-wrapper" },
      React.createElement(
        "table",
        null,
        React.createElement(
          "tbody",
          null,
          React.createElement("tr", null, tableCell("Timestamp referência", "align-left"), tableCell(predictionNextHour?.timestamp_referencia_utc ?? "—")),
          React.createElement("tr", null, tableCell("Predição défice", "align-left"), tableCell(predictionNextHour?.pred_flag_defice_t_plus_1 ? "Sim" : "Não")),
          React.createElement("tr", null, tableCell("Probabilidade défice", "align-left"), tableCell(formatPercent((predictionNextHour?.prob_defice_t_plus_1 ?? 0) * 100))),
          React.createElement("tr", null, tableCell("Modelo", "align-left"), tableCell(predictionNextHour?.model_uri ?? "—")),
        ),
      ),
    ),
  );
}
