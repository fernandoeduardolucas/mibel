import React from "react";
import { getDashboardData, resolveApiBase } from "../services/meteoproducaoService.js";
import { KpiGrid } from "./KpiGrid.js";
import { CorrelationTable } from "./CorrelationTable.js";
import { ScatterChart } from "./ScatterChart.js";
import { TimeSeriesChart } from "./TimeSeriesChart.js";
import { ImportanceBar } from "./ImportanceBar.js";

function Header() {
  return React.createElement(
    "header", { className: "hero" },
    React.createElement("p", { className: "badge" }, "DP-03 · Meteorologia vs Produção"),
    React.createElement("h1", null, "Meteorologia, Produção e Impacto no Preço"),
    React.createElement(
      "p", { className: "subtitle" },
      "Correlação entre variáveis climáticas, produção elétrica nacional e preço spot day-ahead MIBEL Portugal.",
    ),
  );
}

function Controls({ state, onChange, onRefresh, loading }) {
  return React.createElement(
    "section", { className: "panel controls" },
    React.createElement(
      "label", { className: "field" },
      React.createElement("span", null, "Base da API"),
      React.createElement("input", {
        type: "text",
        value: state.apiBase,
        onChange: (e) => onChange("apiBase", e.target.value),
        placeholder: "http://localhost:8083",
      }),
    ),
    React.createElement(
      "label", { className: "field" },
      React.createElement("span", null, "Data início"),
      React.createElement("input", {
        type: "date",
        value: state.startDate,
        onChange: (e) => onChange("startDate", e.target.value),
      }),
    ),
    React.createElement(
      "label", { className: "field" },
      React.createElement("span", null, "Data fim"),
      React.createElement("input", {
        type: "date",
        value: state.endDate,
        onChange: (e) => onChange("endDate", e.target.value),
      }),
    ),
    React.createElement(
      "button", {
        type: "button",
        className: "primary-button",
        onClick: onRefresh,
        disabled: loading,
      },
      loading ? "A atualizar..." : "Atualizar",
    ),
  );
}

export function DashboardApp() {
  const [filters, setFilters] = React.useState({
    apiBase: resolveApiBase(""),
    startDate: "",
    endDate: "",
  });
  const [loading, setLoading]       = React.useState(false);
  const [error, setError]           = React.useState("");
  const [lastUpdated, setLastUpdated] = React.useState("");
  const [daily, setDaily]           = React.useState([]);
  const [analytics, setAnalytics]   = React.useState({});
  const [correlations, setCorr]     = React.useState({});

  const loadData = React.useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const result = await getDashboardData(filters);
      setDaily(result.daily ?? []);
      setAnalytics(result.analytics ?? {});
      setCorr(result.correlations ?? {});
      setFilters((prev) => ({ ...prev, apiBase: result.apiBase }));
      setLastUpdated(new Date().toLocaleString("pt-PT"));
    } catch (err) {
      setError(`Falha ao carregar dados: ${err.message}`);
    } finally {
      setLoading(false);
    }
  }, [filters]);

  React.useEffect(() => { loadData(); }, []);

  function updateFilter(key, value) {
    setFilters((prev) => ({ ...prev, [key]: value }));
  }

  return React.createElement(
    "main", { className: "dashboard-shell" },
    React.createElement(Header),
    React.createElement(Controls, { state: filters, onChange: updateFilter, onRefresh: loadData, loading }),
    React.createElement(KpiGrid, { analytics }),
    React.createElement(
      "div", { className: "grid-2col" },
      React.createElement(ScatterChart, {
        daily,
        xKey: "radiation_mean_wm2",
        yKey: "producao_total_daily_mwh",
        title: "Radiação Solar vs Produção Diária",
        xLabel: "Radiação média (W/m²)",
        yLabel: "Produção (MWh)",
      }),
      React.createElement(ScatterChart, {
        daily,
        xKey: "wind_speed_mean_ms",
        yKey: "producao_total_daily_mwh",
        title: "Velocidade do Vento vs Produção Diária",
        xLabel: "Vento médio (m/s)",
        yLabel: "Produção (MWh)",
      }),
    ),
    React.createElement(
      "div", { className: "grid-2col" },
      React.createElement(TimeSeriesChart, {
        daily,
        lines: [
          { key: "temperature_mean_c",     label: "Temperatura (°C)",    color: "#f59e0b" },
          { key: "preco_spot_medio_eur_mwh", label: "Preço spot (€/MWh)", color: "#dc2626" },
        ],
        title: "Temperatura vs Preço Spot ao Longo do Tempo",
      }),
      React.createElement(TimeSeriesChart, {
        daily,
        lines: [
          { key: "wind_speed_mean_ms",         label: "Vento (m/s)",         color: "#0ea5e9" },
          { key: "producao_total_daily_mwh",   label: "Produção (MWh)",      color: "#15803d" },
        ],
        title: "Vento vs Produção ao Longo do Tempo",
      }),
    ),
    React.createElement(
      "div", { className: "grid-2col" },
      React.createElement(CorrelationTable, { correlations }),
      React.createElement(ImportanceBar, { daily }),
    ),
    React.createElement(
      "footer", { className: "status-bar" },
      error
        ? React.createElement("p", { className: "error" }, error)
        : React.createElement(
            "p", { className: "ok" },
            lastUpdated ? `Última atualização: ${lastUpdated}` : "Sem atualizações ainda.",
          ),
    ),
  );
}
