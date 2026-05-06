const decimalFormat = new Intl.NumberFormat("pt-PT", { maximumFractionDigits: 2 });

export function formatNumber(value) {
  return decimalFormat.format(value ?? 0);
}

export function formatPercent(value) {
  return `${decimalFormat.format(value ?? 0)}%`;
}

export function formatRatio(value) {
  if (value == null) {
    return "—";
  }
  return decimalFormat.format(value);
}

export function formatDateTime(value) {
  if (!value) {
    return "—";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString("pt-PT", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}
