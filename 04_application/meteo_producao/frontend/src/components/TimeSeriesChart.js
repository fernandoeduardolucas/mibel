import React from "react";

export function TimeSeriesChart({ daily, lines, title }) {
  const W = 420, H = 160, PAD = { top: 14, right: 14, bottom: 28, left: 44 };

  if (!daily || daily.length < 2) {
    return React.createElement(
      "div", { className: "panel" },
      React.createElement("h2", null, title),
      React.createElement("p", { style: { color: "var(--muted)", fontStyle: "italic" } }, "Sem dados suficientes."),
    );
  }

  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;
  const n = daily.length;

  const seriesSvg = lines.map(({ key, label, color }) => {
    const vals = daily.map((d) => d[key]);
    const valid = vals.filter((v) => v != null);
    if (valid.length < 2) return null;

    const minV = Math.min(...valid);
    const maxV = Math.max(...valid);
    const rangeV = maxV - minV || 1;

    const pts = vals
      .map((v, i) => {
        if (v == null) return null;
        const x = PAD.left + (i / (n - 1)) * innerW;
        const y = PAD.top + innerH - ((v - minV) / rangeV) * innerH;
        return `${x},${y}`;
      })
      .filter(Boolean)
      .join(" ");

    return React.createElement("polyline", {
      key,
      points: pts,
      fill: "none",
      stroke: color,
      strokeWidth: 1.8,
      opacity: 0.9,
    });
  });

  // X-axis tick labels (first, mid, last)
  const tickIdxs = [0, Math.floor((n - 1) / 2), n - 1];
  const xTicks = tickIdxs.map((i) => {
    const d = daily[i];
    const x = PAD.left + (i / (n - 1)) * innerW;
    return React.createElement("text", {
      key: i,
      x,
      y: H - 4,
      textAnchor: "middle",
      fontSize: 9,
      fill: "var(--muted)",
    }, (d.data_dia ?? "").slice(0, 7));
  });

  // Legend
  const legend = lines.map(({ key, label, color }) =>
    React.createElement(
      "span", { key, style: { display: "inline-flex", alignItems: "center", gap: 4, marginRight: 12, fontSize: 12, color: "var(--muted)" } },
      React.createElement("span", { style: { display: "inline-block", width: 20, height: 3, background: color, borderRadius: 2 } }),
      label,
    ),
  );

  return React.createElement(
    "div", { className: "panel" },
    React.createElement("h2", null, title),
    React.createElement("div", { style: { marginBottom: 6 } }, ...legend),
    React.createElement(
      "div", { className: "sparkline-wrap" },
      React.createElement(
        "svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "xMidYMid meet", className: "sparkline" },
        React.createElement("line", { x1: PAD.left, y1: PAD.top, x2: PAD.left, y2: H - PAD.bottom, stroke: "var(--border)", strokeWidth: 1 }),
        React.createElement("line", { x1: PAD.left, y1: H - PAD.bottom, x2: W - PAD.right, y2: H - PAD.bottom, stroke: "var(--border)", strokeWidth: 1 }),
        ...seriesSvg.filter(Boolean),
        ...xTicks,
      ),
    ),
  );
}
