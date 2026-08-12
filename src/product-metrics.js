export const DAY_MS = 24 * 60 * 60 * 1000;

export function asDate(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function asNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function period(days = 30, now = new Date()) {
  const parsed = Math.min(365, Math.max(1, Math.floor(asNumber(days, 30))));
  const currentStart = new Date(now.getTime() - parsed * DAY_MS);
  const previousStart = new Date(currentStart.getTime() - parsed * DAY_MS);
  return { days: parsed, now, currentStart, previousStart };
}

export function inRange(value, start, end) {
  const candidate = asDate(value);
  return Boolean(candidate && candidate >= start && candidate < end);
}

export function percent(numerator, denominator) {
  return denominator ? Math.round((numerator / denominator) * 1000) / 10 : 0;
}

export function change(current, previous) {
  if (!previous) return current ? 100 : 0;
  return Math.round(((current - previous) / previous) * 1000) / 10;
}

export function median(values) {
  const numbers = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!numbers.length) return null;
  const middle = Math.floor(numbers.length / 2);
  return numbers.length % 2 ? numbers[middle] : (numbers[middle - 1] + numbers[middle]) / 2;
}

/**
 * Plugin surfaces that are not tools.
 *
 * The plugin stamps a `tool` on every event, defaulting to the surface the user
 * is currently on, so session, heartbeat and analytics-plumbing events all
 * carry one of these. They are navigation chrome and a game, not products, and
 * must never appear on the tools page or in tool totals.
 */
export const NON_TOOL_SURFACES = new Set([
  "dashboard",
  "collapsed-dashboard",
  "profile",
  "wayfall-game",
  "liquid-glass",
  "unattributed",
  "unknown",
]);

export function normalizeToolCode(code, featureCode = null) {
  const raw = String(code || featureCode || "unattributed").trim().toLowerCase();
  // "frame-gallery" is the code the plugin emits; "frame_gallery" is what usage
  // reservations record. Omitting the hyphenated form split one product into
  // two dashboard rows and double counted its users and jobs.
  if (raw === "frame_gallery" || raw === "frame-gallery" || raw === "frames" || raw === "frames-to-pdf") {
    return { key: "frames-to-pdf", label: "Frames to PDF", feature: featureCode || null };
  }
  if (raw === "unit_converter" || raw === "unit-converter") {
    return { key: "unit-converter", label: "Unit Converter", feature: featureCode || null };
  }
  if (["pdf", "psd", "eps", "ai", "import-tool", "file-importer"].includes(raw)) {
    return { key: "file-importer", label: "File Importer", feature: raw };
  }
  const known = {
    palettable: "Palettable",
    "icon-library": "Icon Library",
    "html-to-design": "HTML to Design",
    "comment-summarizer": "Comment Summarizer",
    "comment-summariser": "Comment Summarizer",
  };
  if (known[raw]) return { key: raw.replace("summariser", "summarizer"), label: known[raw], feature: featureCode || null };
  if (raw === "unattributed") return { key: raw, label: "Unattributed", feature: null };
  const label = raw.replaceAll(/[_-]+/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
  return { key: raw, label, feature: featureCode || null };
}

export function loginAt(session) {
  return asDate(session.completedAt || session.createdAt);
}

export function successfulSession(session) {
  return session?.completed === true && Boolean(session?.user) && Boolean(loginAt(session));
}

export function reservationAt(reservation) {
  return asDate(reservation.committedAt || reservation.updatedAt || reservation.createdAt);
}

export function terminalReservation(reservation) {
  return ["committed", "released", "expired", "compensated"].includes(reservation?.status);
}
