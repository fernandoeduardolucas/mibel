import React from "https://esm.sh/react@18.3.1";
import { createRoot } from "https://esm.sh/react-dom@18.3.1/client";
import { DashboardApp } from "./components/DashboardApp.js";

createRoot(document.getElementById("root")).render(
  React.createElement(React.StrictMode, null, React.createElement(DashboardApp)),
);
