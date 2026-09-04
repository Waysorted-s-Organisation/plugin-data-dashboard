export const $ = (id) => document.getElementById(id);
export const escapeHtml = (value) => String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]);
export const number = (value) => value === null || value === undefined ? "Unavailable" : new Intl.NumberFormat().format(Number(value));
export const percent = (value) => value === null || value === undefined ? "Unavailable" : `${Number(value).toFixed(1)}%`;
export const money = (paise) => paise === null || paise === undefined ? "Unavailable" : new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR", maximumFractionDigits: 0 }).format(Number(paise) / 100);
export const duration = (ms) => ms === null || ms === undefined ? "Unavailable" : ms < 60000 ? `${Math.round(ms / 1000)} sec` : ms < 3600000 ? `${Math.round(ms / 60000)} min` : `${(ms / 3600000).toFixed(1)} hr`;
export const dateTime = (value) => value ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "Unavailable";
export const relative = (value) => {
  if (!value) return "Unavailable";
  const seconds = Math.round((new Date(value).getTime() - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (Math.abs(seconds) < 3600) return formatter.format(Math.round(seconds / 60), "minute");
  if (Math.abs(seconds) < 86400) return formatter.format(Math.round(seconds / 3600), "hour");
  return formatter.format(Math.round(seconds / 86400), "day");
};

export async function json(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" }, cache: "no-store" });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}

export function mountSidebar(active) {
  const sidebar = $("sidebar");
  if (!sidebar) return;
  const workspace = document.querySelector("main.workspace");
  if (workspace && !workspace.id) workspace.id = "workspace";
  if (!document.querySelector(".skip-link")) document.body.insertAdjacentHTML("afterbegin", '<a class="skip-link" href="#workspace">Skip to content</a>');
  const links = [
    ["summary", "/summary.html", "⌂", "Summary"],
    ["users", "/users.html", "◎", "Users"],
    ["tools", "/tools.html", "◇", "Tools"],
    ["journey", "/journey.html", "↗", "User Journey"],
    ["credits", "/credits.html", "◉", "Credits & Billing"],
    ["attribution", "/attribution.html", "⌁", "Attribution"],
    ["newsletter", "/newsletter.html", "✉", "Newsletter"],
    ["feedback", "/feedback.html", "♡", "Feedback & Requests"],
    ["health", "/data-health.html", "●", "Data Health"],
  ];
  sidebar.innerHTML = `<div class="brand"><span class="brand-mark">W</span><span>Waysorted</span></div><nav aria-label="Operations navigation">${links.map(([key, href, icon, label]) => `<a ${key === active ? 'class="active" aria-current="page"' : ""} href="${href}"><span>${icon}</span>${label}</a>`).join("")}</nav><div class="sidebar-footer"><span class="health-dot" id="sidebarHealthDot"></span><span id="sidebarHealthText">Checking data</span></div>`;
  json("/api/operations/data-health").then((health) => {
    $("sidebarHealthDot").className = `health-dot ${health.components?.backendDatabase?.status === "healthy" ? "good" : "bad"}`;
    $("sidebarHealthText").textContent = health.components?.telemetry?.status === "healthy" ? "All live sources current" : "Core data live · behavior partial";
  }).catch(() => { $("sidebarHealthDot").className = "health-dot bad"; $("sidebarHealthText").textContent = "Data unavailable"; });
}

export function setupMobileMenu() {
  const toggle = $("menuToggle");
  if (!toggle) return;
  if (!toggle.getAttribute("aria-label")) toggle.setAttribute("aria-label", "Open navigation");
  toggle.setAttribute("aria-controls", "sidebar");
  toggle.setAttribute("aria-expanded", "false");
  toggle.addEventListener("click", () => {
    const open = $("sidebar")?.classList.toggle("open") || false;
    toggle.setAttribute("aria-expanded", String(open));
  });
}

export function showError(error) {
  const notice = $("globalNotice");
  if (!notice) return;
  notice.hidden = false; notice.textContent = error?.message || "This information is unavailable right now.";
}

export function metricCard(label, value, detail, delta = null, href = null) {
  const deltaText = delta === null ? "" : `<span class="metric-delta ${delta > 0 ? "up" : delta < 0 ? "down" : "flat"}">${delta > 0 ? "+" : ""}${Number(delta).toFixed(1)}% vs previous</span>`;
  return `<article class="metric-card">${href ? `<a class="metric-link" href="${href}">View details →</a>` : ""}<p>${escapeHtml(label)}</p><strong>${escapeHtml(value)}</strong><small>${escapeHtml(detail)}</small>${deltaText}</article>`;
}

export function updateStamp(value) {
  if ($("lastUpdated")) $("lastUpdated").textContent = `Updated ${relative(value)}`;
}
