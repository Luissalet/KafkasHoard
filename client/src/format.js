// Dates, relative times and small helpers. es-ES by default, en-GB when English is chosen.
export const locale = (lang) => (lang === "en" ? "en-GB" : "es-ES");

export function num(value, digits = 0, lang = "es") {
  if (value === null || value === undefined || value === "" || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString(locale(lang), { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function money(value, currency, lang = "es") {
  if (value === null || value === undefined || value === "" || Number.isNaN(Number(value))) return "—";
  try {
    return new Intl.NumberFormat(locale(lang), { style: "currency", currency: currency || "EUR", useGrouping: "always" }).format(Number(value));
  } catch {
    return `${num(value, 2, lang)} ${currency || ""}`.trim();
  }
}

export function clock(ts, lang) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString(locale(lang), { weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }).replace(",", "");
}

export function shortClock(ts, lang) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString(locale(lang), { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}

// "YYYY-MM-DD" -> local Date at noon (avoids timezone drift).
export function isoDate(iso) {
  if (!iso || !/^\d{4}-\d{2}-\d{2}/.test(iso)) return null;
  const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
  return new Date(y, m - 1, d, 12);
}

// "lun 5 oct"
export function dayLabel(iso, lang, { weekday = true, year = false } = {}) {
  const d = iso instanceof Date ? iso : isoDate(iso);
  if (!d) return "";
  const opts = { day: "numeric", month: "short" };
  if (weekday) opts.weekday = "short";
  if (year) opts.year = "numeric";
  return d.toLocaleDateString(locale(lang), opts).replace(/[,.]/g, "").replace(/\s+/g, " ");
}

// "5–7 oct" or "30 sep – 2 oct"
export function windowLabel(from, to, lang) {
  const a = isoDate(from);
  const b = isoDate(to);
  if (!a || !b) return dayLabel(from || to, lang, { weekday: false });
  const fmt = (d, opts) => d.toLocaleDateString(locale(lang), opts).replace(/[,.]/g, "");
  if (a.getMonth() === b.getMonth() && a.getFullYear() === b.getFullYear()) {
    return `${a.getDate()}–${fmt(b, { day: "numeric", month: "short" })}`;
  }
  return `${fmt(a, { day: "numeric", month: "short" })} – ${fmt(b, { day: "numeric", month: "short" })}`;
}

export function daysBetween(fromIso, toIso) {
  const a = isoDate(fromIso);
  const b = isoDate(toIso);
  if (!a || !b) return null;
  return Math.round((b - a) / 86_400_000);
}

// "hace 5 min" / "dentro de 2 h"
export function rel(ts, lang, nowMs = Date.now()) {
  if (!ts) return "—";
  const diff = ts * 1000 - nowMs;
  const abs = Math.abs(diff);
  const formatter = new Intl.RelativeTimeFormat(locale(lang), { numeric: "auto", style: "short" });
  if (abs < 45_000) return formatter.format(0, "second");
  const units = [["day", 86_400_000], ["hour", 3_600_000], ["minute", 60_000]];
  for (const [unit, size] of units) {
    if (abs >= size || unit === "minute") return formatter.format(Math.round(diff / size), unit);
  }
  return "—";
}

export function safeUrl(url) {
  return typeof url === "string" && /^https?:\/\//i.test(url) ? url : null;
}

export function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url || "";
  }
}

export const splitList = (text) => (text || "").split(/[\n,;]+/).map((s) => s.trim()).filter(Boolean);

export function duration(ms) {
  if (ms === null || ms === undefined || Number.isNaN(Number(ms))) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 90_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms / 60_000)} min`;
}

// "hoy", "mañana", "en 5 días", "hace 3 días" from a day count.
export function daysText(days, t) {
  if (days === null || days === undefined) return "";
  if (days === 0) return t("today");
  if (days === 1) return t("tomorrow");
  if (days === -1) return t("yesterday");
  return days > 0 ? t("in_days", { n: days }) : t("days_ago", { n: -days });
}

export function fileSize(bytes) {
  if (!bytes) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}
