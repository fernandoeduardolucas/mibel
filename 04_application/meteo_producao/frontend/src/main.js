import React from "react";
import { createRoot } from "react-dom/client";
import { DashboardApp } from "./components/DashboardApp.js";

createRoot(document.getElementById("root")).render(
  React.createElement(React.StrictMode, null, React.createElement(DashboardApp)),
);
