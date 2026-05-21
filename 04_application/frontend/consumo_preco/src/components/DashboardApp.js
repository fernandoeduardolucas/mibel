import React, { useEffect, useState } from "react";
import KpiGrid from "./KpiGrid.js";
import TimeSeriesTable from "./TimeSeriesTable.js";
import AnalyticsChart from "./AnalyticsChart.js";
import { fetchOverview, fetchTimeseries, fetchAnalytics } from "../services/consumoPrecoService.js";

export default function DashboardApp() {
  const [overview,   setOverview]   = useState(null);
  const [timeseries, setTimeseries] = useState([]);
  const [analytics,  setAnalytics]  = useState(null);
  const [group,      setGroup]      = useState("day");
  const [error,      setError]      = useState(null);
  const [loading,    setLoading]    = useState(true);

  async function load(g) {
    setLoading(true);
    setError(null);
    try {
      const [ov, ts, an] = await Promise.all([
        fetchOverview(),
        fetchTimeseries(g),
        fetchAnalytics(),
      ]);
      setOverview(ov);
      setTimeseries(ts);
      setAnalytics(an);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(group); }, []);

  function handleGroup(e) {
    const g = e.target.value;
    setGroup(g);
    load(g);
  }

  return React.createElement("div", { className: "container" },

    React.createElement("header", { className: "header" },
      React.createElement("h1", null, "DP-02 — Consumo vs Preço MIBEL"),
      React.createElement("p", null, "Análise nacional do consumo elétrico face ao preço horário day-ahead (MIBEL Portugal).")
    ),

    React.createElement("section", { className: "controls" },
      React.createElement("label", { htmlFor: "groupBy" }, "Agregação"),
      React.createElement("select", { id: "groupBy", value: group, onChange: handleGroup },
        React.createElement("option", { value: "day" },   "Diária"),
        React.createElement("option", { value: "month" }, "Mensal")
      ),
      React.createElement("button", { onClick: () => load(group), disabled: loading },
        loading ? "A carregar…" : "Atualizar"
      )
    ),

    error && React.createElement("p", { className: "status error" }, `Erro: ${error}`),

    React.createElement(KpiGrid, { overview }),

    React.createElement("section", { className: "section" },
      React.createElement("h2", null, "Série Temporal"),
      React.createElement(TimeSeriesTable, { rows: timeseries })
    ),

    React.createElement("section", { className: "section" },
      React.createElement("h2", null, "Padrões Analíticos"),
      React.createElement(AnalyticsChart, { analytics })
    )
  );
}
