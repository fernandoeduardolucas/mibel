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

function toCsvCell(value) {
  const escaped = String(value ?? "").replace(/"/g, "\"\"");
  return `"${escaped}"`;
}

function buildGroupedSeriesCsv(groupedSeries = []) {
  const headers = [
    "periodo",
    "consumo_total",
    "producao_total",
    "saldo",
    "ratio_producao_consumo",
    "defice_horas",
    "excedente_horas",
    "leituras",
  ];

  const lines = [
    headers.join(","),
    ...groupedSeries.map((row) =>
      [
        row.periodo,
        row.consumo_total,
        row.producao_total,
        row.saldo,
        row.ratio_producao_consumo,
        row.defice_horas ?? 0,
        row.excedente_horas ?? 0,
        row.leituras ?? 0,
      ]
        .map(toCsvCell)
        .join(","),
    ),
  ];

  return lines.join("\n");
}

function exportGroupedSeriesCsv(groupedSeries = []) {
  if (!groupedSeries.length) {
    return;
  }

  const csvContent = buildGroupedSeriesCsv(groupedSeries);
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  const now = new Date().toISOString().replace(/[:]/g, "-");

  link.href = url;
  link.download = `serie-temporal-agregada-${now}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export function DataTables({ analytics, groupedSeries }) {
  const worstDeficits = deficitRows(analytics);
  const dependency = dependenciaRows(analytics);

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
      React.createElement(
        "div",
        { className: "panel-header" },
        React.createElement("h2", null, "Série temporal agregada"),
        React.createElement(
          "button",
          {
            type: "button",
            className: "secondary-button",
            onClick: () => exportGroupedSeriesCsv(groupedSeries),
            disabled: !groupedSeries.length,
          },
          "Exportar CSV",
        ),
      ),
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
            groupedSeries.length
              ? groupedSeries.map((row, index) =>
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
                )
              : React.createElement(
                  "tr",
                  null,
                  React.createElement(
                    "td",
                    { className: "empty-state", colSpan: 8 },
                    "Sem dados para a série temporal agregada.",
                  ),
                ),
          ),
        ),
      ),
    ),
  );
}
