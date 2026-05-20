import React from "react";

function linearRegression(points) {
  const n = points.length;
  if (n < 2) return null;
  const sumX  = points.reduce((s, p) => s + p[0], 0);
  const sumY  = points.reduce((s, p) => s + p[1], 0);
  const sumXY = points.reduce((s, p) => s + p[0] * p[1], 0);
  const sumX2 = points.reduce((s, p) => s + p[0] * p[0], 0);
  const denom = n * sumX2 - sumX * sumX;
  if (denom === 0) return null;
  const m = (n * sumXY - sumX * sumY) / denom;
  const b = (sumY - m * sumX) / n;
  return { m, b };
}

export function ScatterChart({ daily, xKey, yKey, title, xLabel, yLabel }) {
  const W = 420, H = 220, PAD = 38;

  const points = (daily ?? [])
    .map((d) => [d[xKey], d[yKey]])
    .filter(([x, y]) => x != null && y != null);

  if (points.length < 3) {
    return React.createElement(
      "div", { className: "panel" },
      React.createElement("h2", null, title),
      React.createElement("p", { style: { color: "var(--muted)", fontStyle: "italic" } }, "Sem dados suficientes."),
    );
  }

  const xs = points.map((p) => p[0]);
  const ys = points.map((p) => p[1]);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;

  const toSvg = ([x, y]) => [
    PAD + ((x - minX) / rangeX) * (W - PAD * 2),
    H - PAD - ((y - minY) / rangeY) * (H - PAD * 2),
  ];

  const reg = linearRegression(points);
  let trendLine = null;
  if (reg) {
    const [x1, y1] = toSvg([minX, reg.m * minX + reg.b]);
    const [x2, y2] = toSvg([maxX, reg.m * maxX + reg.b]);
    trendLine = React.createElement("line", {
      key: "trend",
      x1, y1, x2, y2,
      stroke: "#dc2626",
      strokeWidth: 1.5,
      strokeDasharray: "5,3",
      opacity: 0.8,
    });
  }

  return React.createElement(
    "div", { className: "panel" },
    React.createElement("h2", null, title),
    React.createElement(
      "div", { className: "scatter-wrap" },
      React.createElement(
        "svg", {
          className: "scatter",
          viewBox: `0 0 ${W} ${H}`,
          preserveAspectRatio: "xMidYMid meet",
        },
        // Axes
        React.createElement("line", { x1: PAD, y1: H - PAD, x2: W - PAD, y2: H - PAD, stroke: "var(--border)", strokeWidth: 1 }),
        React.createElement("line", { x1: PAD, y1: PAD, x2: PAD, y2: H - PAD, stroke: "var(--border)", strokeWidth: 1 }),
        // Axis labels
        React.createElement("text", { x: W / 2, y: H - 4, textAnchor: "middle", fontSize: 10, fill: "var(--muted)" }, xLabel),
        React.createElement("text", { x: 10, y: H / 2, textAnchor: "middle", fontSize: 10, fill: "var(--muted)", transform: `rotate(-90,10,${H / 2})` }, yLabel),
        // Trend line
        trendLine,
        // Points
        ...points.map(([x, y], i) => {
          const [cx, cy] = toSvg([x, y]);
          return React.createElement("circle", { key: i, cx, cy, r: 3, fill: "#1d4ed8", opacity: 0.55 });
        }),
      ),
    ),
  );
}
