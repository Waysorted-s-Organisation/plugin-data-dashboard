const $ = (id) => document.getElementById(id);
const state = { page: 1, pageSize: 25, search: "", source: "all", days: "7" };
let searchTimer = null;

function escapeHtml(value) { return String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]); }
function number(value) { return new Intl.NumberFormat("en-IN").format(Number(value || 0)); }
function dateLabel(value) { return value ? new Intl.DateTimeFormat("en-IN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "—"; }
function human(value) { return String(value || "—").replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase()); }
async function json(url) { const response = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" }); const body = await response.json().catch(() => ({})); if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`); return body; }
function metric(label, value, copy) { return `<article class="metric"><p class="metric-label">${escapeHtml(label)}</p><strong class="metric-value">${escapeHtml(value)}</strong><span class="metric-change">${escapeHtml(copy)}</span></article>`; }
function params() { return new URLSearchParams({ page: state.page, pageSize: state.pageSize, search: state.search, source: state.source, days: state.days }); }

function render(data) {
  $("activityKpis").innerHTML = [metric("Authenticated users", number(data.summary.users), "Unique users in this range"), metric("Successful logins", number(data.summary.logins), "Linked authentication sessions"), metric("With credited usage", number(data.summary.withCreditActivity), "Has recorded completed tool usage")].join("");
  $("activityBody").innerHTML = data.items.length ? data.items.map((row) => `<tr><td><strong>${escapeHtml(row.name || "Unnamed user")}</strong><small>${escapeHtml(row.email || "No email")}</small></td><td>${dateLabel(row.lastLoginAt)}</td><td>${number(row.loginCount)}</td><td><span class="status-chip blue">${escapeHtml(human(row.latestSource || "otp"))}</span></td><td>${row.latestCreditTool ? escapeHtml(human(row.latestCreditTool)) : "No credited usage"}</td><td>${dateLabel(row.latestCreditAt)}</td></tr>`).join("") : `<tr><td colspan="6"><div class="empty-state">No successful users match this range and filter.</div></td></tr>`;
  const p = data.pagination; $("activityPager").innerHTML = `<span>${p.total ? `${(p.page - 1) * p.pageSize + 1}–${Math.min(p.page * p.pageSize, p.total)} of ${p.total}` : "0 users"}</span><div><button class="button secondary" data-page="${p.page - 1}" ${p.page <= 1 ? "disabled" : ""}>Previous</button> <button class="button secondary" data-page="${p.page + 1}" ${p.page >= p.pages ? "disabled" : ""}>Next</button></div>`;
  $("activityPager").querySelectorAll("button[data-page]").forEach((button) => button.addEventListener("click", () => { state.page = Number(button.dataset.page); load(); }));
  $("sourceStatus").textContent = "Live authentication sessions"; $("sourceStatus").className = "status-chip good"; $("lastUpdated").textContent = `Updated ${dateLabel(data.asOf)}`;
}

async function load() {
  $("globalNotice").hidden = true; $("activityBody").innerHTML = `<tr><td colspan="6"><div class="empty-state">Loading activity…</div></td></tr>`;
  try { render(await json(`/api/operations/activity/recent-users?${params()}`)); }
  catch (error) { $("sourceStatus").textContent = "Data unavailable"; $("sourceStatus").className = "status-chip bad"; $("globalNotice").hidden = false; $("globalNotice").textContent = `${error.message}. No placeholder activity is shown.`; $("activityKpis").innerHTML = metric("Recent activity", "Unavailable", "Check backend database configuration"); $("activityBody").innerHTML = `<tr><td colspan="6"><div class="empty-state error-text">Activity unavailable.</div></td></tr>`; }
}
async function health() { try { const data = await json("/api/operations/health"); $("sidebarHealthDot").className = "health-dot good"; $("sidebarHealthText").textContent = `${number(data.backendDatabase.users)} users connected`; } catch { $("sidebarHealthDot").className = "health-dot bad"; $("sidebarHealthText").textContent = "Backend unavailable"; } }
function bind() {
  $("refreshAll").addEventListener("click", () => Promise.all([load(), health()]));
  $("activitySearch").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.search = $("activitySearch").value.trim(); state.page = 1; load(); }, 300); });
  [["rangeDays", "days"], ["sourceFilter", "source"]].forEach(([element, key]) => $(element).addEventListener("change", () => { state[key] = $(element).value; state.page = 1; load(); }));
  $("pageSize").addEventListener("change", () => { state.pageSize = Number($("pageSize").value); state.page = 1; load(); });
  $("menuToggle").addEventListener("click", () => { const open = $("sidebar").classList.toggle("open"); $("menuToggle").setAttribute("aria-expanded", String(open)); });
}
bind(); load(); health();
