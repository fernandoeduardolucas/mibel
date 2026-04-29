import React from "react";
import { GROUP_OPTIONS } from "../models/analyticsQuestionsModel.js";
import { getDashboardData, resolveApiBase } from "../services/producaoConsumoService.js";
import { DataTables } from "./DataTables.js";
import { KpiGrid } from "./KpiGrid.js";
import { QuestionCards } from "./QuestionCards.js";

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

  const rows = groupedSeries.map((row) =>
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
      .map((value) => `"${String(value ?? "").replace(/"/g, '""')}"`)
      .join(","),
  );

  return [headers.join(","), ...rows].join("\n");
}

function exportGroupedSeriesCsv(groupedSeries = []) {
  if (!groupedSeries.length) {
    return;
  }

  const csv = buildGroupedSeriesCsv(groupedSeries);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `serie-temporal-agregada-${new Date().toISOString().replace(/[:]/g, "-")}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function Header() {
  return React.createElement(
    "header",
    { className: "hero" },
    React.createElement("p", { className: "badge" }, "Dashboard configurável"),
    React.createElement("h1", null, "Produção vs Consumo Energético"),
    React.createElement(
      "p",
      { className: "subtitle" },
      "Painel analítico profissional com foco em défice, dependência de fontes e tendência de equilíbrio operacional.",
    ),
  );
}

function Controls({ state, onChange, onRefresh, onExport, loading, exportDisabled }) {
  return React.createElement(
    "section",
    { className: "panel controls" },
    React.createElement(
      "label",
      { className: "field" },
      React.createElement("span", null, "Base da API"),
      React.createElement("input", {
        type: "text",
        value: state.apiBase,
        onChange: (event) => onChange("apiBase", event.target.value),
        placeholder: "http://localhost:8081",
      }),
    ),
    React.createElement(
      "label",
      { className: "field" },
      React.createElement("span", null, "Granularidade"),
      React.createElement(
        "select",
        {
          value: state.groupBy,
          onChange: (event) => onChange("groupBy", event.target.value),
        },
        GROUP_OPTIONS.map((option) =>
          React.createElement("option", { key: option.value, value: option.value }, option.label),
        ),
      ),
    ),
    React.createElement(
      "button",
      {
        type: "button",
        className: "primary-button",
        onClick: onRefresh,
        disabled: loading,
      },
      loading ? "A atualizar..." : "Atualizar dashboard",
    ),
    React.createElement(
      "button",
      {
        type: "button",
        className: "secondary-button",
        onClick: onExport,
        disabled: loading || exportDisabled,
      },
      "⬇ Exportar CSV",
    ),
  );
}

export function DashboardApp() {
  const [filters, setFilters] = React.useState({
    apiBase: resolveApiBase(""),
    groupBy: "daily",
  });
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const [lastUpdated, setLastUpdated] = React.useState("");
  const [analytics, setAnalytics] = React.useState({});
  const [groupedSeries, setGroupedSeries] = React.useState([]);

  const loadData = React.useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      const dashboardData = await getDashboardData(filters);
      setAnalytics(dashboardData.analytics ?? {});
      setGroupedSeries(dashboardData.groupedSeries ?? []);
      setFilters((previous) => ({ ...previous, apiBase: dashboardData.apiBase }));
      setLastUpdated(new Date().toLocaleString("pt-PT"));
    } catch (requestError) {
      setError(`Falha ao carregar dados: ${requestError.message}`);
    } finally {
      setLoading(false);
    }
  }, [filters]);

  React.useEffect(() => {
    loadData();
  }, [loadData]);

  function updateFilter(key, value) {
    setFilters((previous) => ({ ...previous, [key]: value }));
  }

  return React.createElement(
    "main",
    { className: "dashboard-shell" },
    React.createElement(Header),
    React.createElement(Controls, {
      state: filters,
      onChange: updateFilter,
      onRefresh: loadData,
      onExport: () => exportGroupedSeriesCsv(groupedSeries),
      loading,
      exportDisabled: !groupedSeries.length,
    }),
    React.createElement(QuestionCards),
    React.createElement(KpiGrid, { analytics }),
    React.createElement(DataTables, { analytics, groupedSeries }),
    React.createElement(
      "footer",
      { className: "status-bar" },
      error
        ? React.createElement("p", { className: "error" }, error)
        : React.createElement(
            "p",
            { className: "ok" },
            lastUpdated ? `Última atualização: ${lastUpdated}` : "Sem atualizações ainda.",
          ),
    ),
  );
}
