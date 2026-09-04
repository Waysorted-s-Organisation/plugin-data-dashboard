import { money, mountSidebar, metricCard } from "./intelligence-shared.js";
mountSidebar("credits");
const $ = (id) => document.getElementById(id);
const state = { page: 1, pageSize: 25, search: "", tool: "all", walletStatus: "all", subscriptionStatus: "all", lowCredit: false, sort: "email", days: "30" };
let searchTimer = null;

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]);
}
function number(value) { return new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 }).format(Number(value || 0)); }
function dateLabel(value) { return value ? new Intl.DateTimeFormat("en-IN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "—"; }
function human(value) { return String(value || "—").replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase()); }
function setNotice(message = "") { $("globalNotice").hidden = !message; $("globalNotice").textContent = message; }

async function json(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}

function metric(label, value, description) {
  return `<article class="metric"><p class="metric-label">${escapeHtml(label)}</p><strong class="metric-value">${escapeHtml(value)}</strong><span class="metric-change">${escapeHtml(description)}</span></article>`;
}

function renderOverview(data) {
  const s = data.summary;
  $("creditKpis").innerHTML = [
    metric("Billing wallets", number(s.wallets), `${number(s.walletsMissing)} not initialized`),
    metric("Available credits", number(s.totalAvailableCredits), "Current wallet balance"),
    metric("Held credits", number(s.totalHeldCredits), "Reserved, not yet completed"),
    metric("Spent in range", number(s.creditsSpentInRange), `${number(s.completedUsesInRange)} completed uses`),
    metric("Low-credit users", number(s.lowCreditUsers), `At or below ${data.threshold}`),
  ].join("");
  $("thresholdExplanation").textContent = `${data.threshold} credits or lower.`;
  $("toolCoverage").textContent = data.dataQuality.unattributedUses ? `${data.dataQuality.unattributedUses} unattributed` : "Fully attributed";
  $("toolCoverage").className = `status-chip ${data.dataQuality.unattributedUses ? "warn" : "good"}`;
  renderTools(data.tools || []);
  const current = $("toolFilter").value;
  $("toolFilter").innerHTML = `<option value="all">All tools</option>${(data.tools || []).map((row) => `<option value="${escapeHtml(row.tool)}">${escapeHtml(row.toolLabel || human(row.tool))}</option>`).join("")}`;
  $("toolFilter").value = Array.from($("toolFilter").options).some((o) => o.value === current) ? current : "all";
  $("sourceStatus").textContent = "Live Waysorted database";
  $("sourceStatus").className = "status-chip good";
  $("lastUpdated").textContent = `Updated ${dateLabel(data.asOf)}`;
}

function renderTools(tools) {
  if (!tools.length) { $("toolBars").innerHTML = `<div class="empty-state">No completed credit-consuming activity in this range.</div>`; return; }
  const max = Math.max(...tools.map((row) => Number(row.creditsSpent || 0)), 1);
  $("toolBars").innerHTML = tools.slice(0, 12).map((row) => `<div class="tool-bar-row"><div><strong>${escapeHtml(row.toolLabel || human(row.tool))}</strong><span>${number(row.completedUses)} use${row.completedUses === 1 ? "" : "s"}${row.compensatedUses ? ` · ${number(row.compensatedUses)} refunded` : ""} · ${number(row.userCount)} user${row.userCount === 1 ? "" : "s"}</span></div><div class="tool-bar-track"><i style="width:${Math.max(2, Number(row.creditsSpent || 0) / max * 100)}%"></i></div><b>${number(row.creditsSpent)}</b></div>`).join("");
}

function userParams() {
  const params = new URLSearchParams({ page: state.page, pageSize: state.pageSize, search: state.search, tool: state.tool, walletStatus: state.walletStatus, subscriptionStatus: state.subscriptionStatus, lowCredit: String(state.lowCredit), sort: state.sort });
  return params;
}

function renderUsers(data) {
  const rows = data.items || [];
  $("usersBody").innerHTML = rows.length ? rows.map((row) => `<tr data-id="${escapeHtml(row.id)}" tabindex="0"><td><strong>${escapeHtml(row.name || "Unnamed user")}</strong><small>${escapeHtml(row.email || "No email")}</small></td><td>${row.availableCredits === null ? "—" : number(row.availableCredits)}</td><td>${row.heldCredits === null ? "—" : number(row.heldCredits)}</td><td>${row.lifetimeSpentCredits === null ? "—" : number(row.lifetimeSpentCredits)}</td><td><span class="status-chip neutral">${escapeHtml(human(row.subscriptionStatus || "inactive"))}</span><small>${escapeHtml(human(row.subscriptionPlanCode))}</small></td><td>${row.topTool ? `${escapeHtml(row.topTool.toolLabel || human(row.topTool.tool))}<small>${number(row.topTool.creditsSpent)} credits</small>` : "—"}</td><td>${dateLabel(row.latestCreditAt)}</td><td><span class="status-chip ${row.walletStatus === "initialized" ? "good" : "warn"}">${row.walletStatus === "initialized" ? "Initialized" : "Not initialized"}</span></td></tr>`).join("") : `<tr><td colspan="8"><div class="empty-state">No users match these filters.</div></td></tr>`;
  const p = data.pagination;
  $("usersPager").innerHTML = `<span>${p.total ? `${(p.page - 1) * p.pageSize + 1}–${Math.min(p.page * p.pageSize, p.total)} of ${p.total}` : "0 users"}</span><div><button class="button secondary" data-page="${p.page - 1}" ${p.page <= 1 ? "disabled" : ""}>Previous</button> <button class="button secondary" data-page="${p.page + 1}" ${p.page >= p.pages ? "disabled" : ""}>Next</button></div>`;
  $("usersBody").querySelectorAll("tr[data-id]").forEach((row) => { const open = () => openDrawer(row.dataset.id); row.addEventListener("click", open); row.addEventListener("keydown", (event) => { if (["Enter", " "].includes(event.key)) { event.preventDefault(); open(); } }); });
  $("usersPager").querySelectorAll("button[data-page]").forEach((button) => button.addEventListener("click", () => { state.page = Number(button.dataset.page); loadUsers(); }));
}

async function loadUsers() {
  $("usersBody").innerHTML = `<tr><td colspan="8"><div class="empty-state">Loading users…</div></td></tr>`;
  try { renderUsers(await json(`/api/operations/credits/users?${userParams()}`)); }
  catch (error) { $("usersBody").innerHTML = `<tr><td colspan="8"><div class="empty-state error-text">${escapeHtml(error.message)}</div></td></tr>`; }
}

async function loadOverview() {
  $("creditKpis").innerHTML = ""; $("toolBars").innerHTML = ""; setNotice();
  try { renderOverview(await json(`/api/operations/credits/overview?days=${encodeURIComponent(state.days)}`)); }
  catch (error) { $("sourceStatus").textContent = "Data unavailable"; $("sourceStatus").className = "status-chip bad"; setNotice(`${error.message}. Values are intentionally not replaced with zeroes.`); $("creditKpis").innerHTML = metric("Credits", "Unavailable", "Check backend database configuration"); $("toolBars").innerHTML = `<div class="empty-state">Tool consumption unavailable.</div>`; }
}

async function openDrawer(userId) {
  $("creditDrawer").classList.add("open"); $("creditDrawer").setAttribute("aria-hidden", "false"); $("drawerBackdrop").hidden = false; $("drawerBody").innerHTML = `<div class="empty-state">Loading credit profile…</div>`;
  try {
    const data = await json(`/api/operations/credits/users/${encodeURIComponent(userId)}?days=${encodeURIComponent(state.days)}`);
    const u = data.user; $("drawerTitle").textContent = u.name || u.email || "User";
    $("drawerBody").innerHTML = `<div class="profile-hero"><span class="avatar">${escapeHtml((u.name || u.email || "U").slice(0, 2).toUpperCase())}</span><div><strong>${escapeHtml(u.name || "Unnamed user")}</strong><small>${escapeHtml(u.email || "No email")}</small></div></div><section class="profile-section"><h3>Wallet</h3><div class="definition-grid"><div><span>Available</span><strong>${u.availableCredits === null ? "Not initialized" : number(u.availableCredits)}</strong></div><div><span>Held</span><strong>${u.heldCredits === null ? "—" : number(u.heldCredits)}</strong></div><div><span>Lifetime spent</span><strong>${u.lifetimeSpentCredits === null ? "—" : number(u.lifetimeSpentCredits)}</strong></div><div><span>Subscription</span><strong>${escapeHtml(human(u.subscriptionStatus || "inactive"))}</strong></div></div></section><section class="profile-section"><h3>Completed usage in range</h3>${data.tools.length ? `<div class="rank-list">${data.tools.map((row) => `<div class="rank-item"><span>${escapeHtml(row.toolLabel || human(row.tool))}<small>${number(row.completedUses)} uses</small></span><strong>${number(row.creditsSpent)} credits</strong></div>`).join("")}</div>` : `<div class="empty-state">No completed usage in this range.</div>`}</section><section class="profile-section"><h3>Recent ledger</h3>${data.ledger.length ? `<div class="ledger-list">${data.ledger.map((row) => `<div><span class="ledger-delta ${row.deltaCredits < 0 ? "negative" : row.deltaCredits > 0 ? "positive" : ""}">${row.deltaCredits > 0 ? "+" : ""}${number(row.deltaCredits)}</span><p><strong>${escapeHtml(human(row.reason))}</strong><small>${escapeHtml(human(row.tool))} · ${dateLabel(row.createdAt)}</small></p></div>`).join("")}</div>` : `<div class="empty-state">No ledger entries.</div>`}</section>`;
  } catch (error) { $("drawerBody").innerHTML = `<div class="empty-state error-text">${escapeHtml(error.message)}</div>`; }
}
function closeDrawer() { $("creditDrawer").classList.remove("open"); $("creditDrawer").setAttribute("aria-hidden", "true"); $("drawerBackdrop").hidden = true; }
async function refresh() { await Promise.all([loadOverview(), loadUsers(), loadHealth(), loadCommercial()]); }
async function loadHealth() { try { const health = await json("/api/operations/health"); $("sidebarHealthDot").className = "health-dot good"; $("sidebarHealthText").textContent = `${number(health.backendDatabase.wallets)} wallets connected`; } catch { $("sidebarHealthDot").className = "health-dot bad"; $("sidebarHealthText").textContent = "Backend unavailable"; } }

async function loadCommercial() {
  try {
    const data = await json(`/api/operations/commercial?days=${encodeURIComponent(state.days)}`);
    $("commercialMetrics").innerHTML = metricCard("Confirmed gross revenue", money(data.summary.grossRevenuePaise), data.summary.revenueStatus === "confirmed" ? "Captured payments only" : "No confirmed captured payments") + metricCard("Processed refunds", money(data.summary.refundsPaise), "Processed refunds only") + metricCard("Net revenue", money(data.summary.netRevenuePaise), "Captured payments minus processed refunds") + metricCard("Zero-credit users", number(data.summary.zeroCreditUsers), "Initialized wallets at zero");
    const rank = (object) => Object.entries(object || {}).map(([key, value]) => `<p><strong>${escapeHtml(key.replaceAll("_", " "))}</strong><span>${number(value)}</span></p>`).join("") || '<div class="empty-state">No records in this period.</div>';
    $("purchaseStatus").innerHTML = rank(data.purchaseAttempts);
    $("starterStatus").innerHTML = rank(data.starterGrants);
  } catch (error) {
    $("commercialMetrics").innerHTML = metricCard("Commercial data", "Unavailable", error.message || "Could not load billing health");
  }
}

function bind() {
  $("refreshAll").addEventListener("click", refresh); $("rangeDays").addEventListener("change", () => { state.days = $("rangeDays").value; loadOverview(); loadCommercial(); });
  $("userSearch").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.search = $("userSearch").value.trim(); state.page = 1; loadUsers(); }, 300); });
  [["toolFilter", "tool"], ["walletFilter", "walletStatus"], ["subscriptionFilter", "subscriptionStatus"], ["sortFilter", "sort"]].forEach(([element, key]) => $(element).addEventListener("change", () => { state[key] = $(element).value; state.page = 1; loadUsers(); }));
  $("lowCreditFilter").addEventListener("change", () => { state.lowCredit = $("lowCreditFilter").checked; state.page = 1; loadUsers(); }); $("pageSize").addEventListener("change", () => { state.pageSize = Number($("pageSize").value); state.page = 1; loadUsers(); });
  $("closeDrawer").addEventListener("click", closeDrawer); $("drawerBackdrop").addEventListener("click", closeDrawer); document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeDrawer(); });
  $("menuToggle").addEventListener("click", () => { const open = $("sidebar").classList.toggle("open"); $("menuToggle").setAttribute("aria-expanded", String(open)); });
}
bind(); refresh();
