import React from "react";
import { formatDateTime, formatNumber, formatPercent, formatRatio } from "../utils/formatters.js";

const GROUPED_SERIES_PAGE_SIZE = 10;

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

export function DataTables({ analytics, groupedSeries }) {
  const worstDeficits = deficitRows(analytics);
  const dependency = dependenciaRows(analytics);
  const [groupedSeriesPage, setGroupedSeriesPage] = React.useState(1);

  const totalPages = Math.max(1, Math.ceil(groupedSeries.length / GROUPED_SERIES_PAGE_SIZE));
  const safePage = Math.min(groupedSeriesPage, totalPages);
  const pageStart = (safePage - 1) * GROUPED_SERIES_PAGE_SIZE;
  const pageEnd = pageStart + GROUPED_SERIES_PAGE_SIZE;
  const groupedSeriesPageRows = groupedSeries.slice(pageStart, pageEnd);

  React.useEffect(() => {
    setGroupedSeriesPage(1);
  }, [groupedSeries]);

  React.useEffect(() => {
    if (groupedSeriesPage > totalPages) {
      setGroupedSeriesPage(totalPages);
    }
  }, [groupedSeriesPage, totalPages]);

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
            groupedSeriesPageRows.map((row, index) =>
              React.createElement(
                "tr",
                { key: `${row.periodo}-${pageStart + index}` },
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
          {
            type: "button",
            disabled: safePage <= 1,
            onClick: () => setGroupedSeriesPage((page) => Math.max(1, page - 1)),
          },
          "Anterior",
        ),
        React.createElement("span", null, `Página ${safePage} de ${totalPages}`),
        React.createElement(
          "button",
          {
            type: "button",
            disabled: safePage >= totalPages,
            onClick: () => setGroupedSeriesPage((page) => Math.min(totalPages, page + 1)),
          },
          "Seguinte",
        ),
      ),
    ),
  );
}
