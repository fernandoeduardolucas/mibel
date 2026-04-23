import React from "react";
import { GROUP_OPTIONS } from "../models/analyticsQuestionsModel.js";
import {
  getDashboardData,
  getPredictionData,
  resolveApiBase,
} from "../services/producaoConsumoService.js";
import { DataTables } from "./DataTables.js";
import { KpiGrid } from "./KpiGrid.js";
import { QuestionCards } from "./QuestionCards.js";

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

function Controls({ state, onChange, onRefresh, loading }) {
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
  );
}

function PredictionPanel({ prediction }) {
  const probability = Number(prediction?.prob_defice_t_plus_1 ?? 0) * 100;
  const isDefice = Number(prediction?.pred_flag_defice_t_plus_1 ?? 0) === 1;
  const statusLabel = isDefice ? "Défice provável" : "Sem défice provável";

  return React.createElement(
    "section",
    { className: "panel" },
    React.createElement("h2", null, "Previsão ML (próxima hora)"),
    React.createElement(
      "p",
      null,
      `Estado previsto: ${statusLabel} • Probabilidade de défice: ${probability.toFixed(1)}%`,
    ),
    React.createElement(
      "p",
      { className: "subtitle" },
      `Referência: ${prediction?.timestamp_referencia_utc ?? "n/d"} • Modelo: ${prediction?.model_uri ?? "n/d"}`,
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
  const [prediction, setPrediction] = React.useState({});

  const loadData = React.useCallback(async () => {
    setLoading(true);
    setError("");

    try {
      const [dashboardData, predictionData] = await Promise.all([
        getDashboardData(filters),
        getPredictionData(filters),
      ]);
      setAnalytics(dashboardData.analytics ?? {});
      setGroupedSeries(dashboardData.groupedSeries ?? []);
      setPrediction(predictionData.prediction ?? {});
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
      loading,
    }),
    React.createElement(PredictionPanel, { prediction }),
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
