import React from "react";
import { createRoot } from "react-dom/client";
import DashboardApp from "./components/DashboardApp.js";

const root = createRoot(document.getElementById("root"));
root.render(
  React.createElement(React.StrictMode, null,
    React.createElement(DashboardApp, null)
  )
);
