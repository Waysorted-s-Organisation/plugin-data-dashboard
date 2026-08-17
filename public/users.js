import { $, dateTime, escapeHtml, json, metricCard, mountSidebar, number, relative, setupMobileMenu, showError, updateStamp } from "./intelligence-shared.js";

mountSidebar("users");
setupMobileMenu();

const state = { page: 1, facetsLoaded: false };
let timer;
const filterIds = ["segment", "country", "source", "tool", "wallet", "subscription", "newsletter", "rangeDays", "pageSize"];
const incoming = new URLSearchParams(location.search);
state.autoOpen = incoming.get("user");

function filterValue(id) {
  if (state.facetsLoaded || !["country", "source", "tool", "subscription", "newsletter"].includes(id)) return $(id).value;
  return incoming.get(id) || $(id).value;
}

function params() {
  return new URLSearchParams({
    days: $("rangeDays").value,
    page: state.page,
    pageSize: $("pageSize").value,
    search: $("search").value,
    segment: $("segment").value,
    country: filterValue("country"),
    source: filterValue("source"),
    tool: filterValue("tool"),
    wallet: $("wallet").value,
    subscription: filterValue("subscription"),
    newsletter: filterValue("newsletter"),
  });
}

function chip(value, tone = "neutral") {
  return `<span class="status-chip ${tone}">${escapeHtml(String(value).replaceAll("_", " "))}</span>`;
}

function fillSelect(id, values, label = (value) => value) {
  const select = $(id);
  const selected = select.value;
  for (const value of values) {
    const key = typeof value === "object" ? value.key : value;
    if ([...select.options].some((option) => option.value === key)) continue;
    const option = document.createElement("option");
    option.value = key;
    option.textContent = label(value);
    select.append(option);
  }
  if ([...select.options].some((option) => option.value === selected)) select.value = selected;
}

function mountFacets(facets) {
  if (state.facetsLoaded) return;
  fillSelect("country", facets.countries || []);
  fillSelect("source", facets.sources || [], (value) => String(value).toUpperCase());
  fillSelect("tool", facets.tools || [], (value) => value.label);
  fillSelect("subscription", facets.subscriptions || [], (value) => String(value).replaceAll("_", " "));
  fillSelect("newsletter", facets.newsletterStatuses || [], (value) => String(value).replaceAll("_", " "));
  for (const id of ["country", "source", "tool", "subscription", "newsletter"]) {
    const value = incoming.get(id);
    if (value && [...$(id).options].some((option) => option.value === value)) $(id).value = value;
  }
  state.facetsLoaded = true;
}

async function load() {
  try {
    const query = params();
    history.replaceState(null, "", `${location.pathname}?${query}`);
    const data = await json(`/api/operations/users?${query}`);
    $("globalNotice").hidden = true;
    mountFacets(data.facets || {});
    const counts = data.summary.segmentCounts || {};
    $("userMetrics").innerHTML = metricCard("Users shown", number(data.pagination.total), "Matching the current filters")
      + metricCard("Activated", number(counts.activated || 0), "Completed a credited tool job")
      + metricCard("Returning", number(counts.returning || 0), "Active on two or more days")
      + metricCard("Needs activation", number(counts.not_activated || 0), "No completed credited job");
    $("usersBody").innerHTML = data.items.map((row) => `<tr ${row.anonymous ? "" : `data-user-id="${row.id}"`} tabindex="0"><td><strong>${row.anonymous ? "Signed-out visitor" : escapeHtml(row.name || "Unnamed user")}</strong><small>${row.anonymous ? escapeHtml(String(row.id).slice(0, 22)) : escapeHtml(row.email)}</small></td><td>${chip(row.lifecycleStage)}</td><td>${relative(row.lastActiveAt || row.lastLoginAt)}<small>${escapeHtml(row.lastActiveSource === "plugin" ? "plugin activity" : row.lastActiveSource === "tool job" ? "tool job" : (row.latestLoginSource || "No successful login"))}</small></td><td>${number(row.completedJobs)}</td><td>${row.topTool ? `${escapeHtml(row.topTool.label)}${row.topTool.credited === false ? '<small>no credits charged</small>' : ""}` : row.pluginActivityAvailable === false ? "Activity unavailable" : "No tool use recorded"}</td><td>${row.anonymous ? "&mdash;" : row.availableCredits === null ? '<span class="status-chip warn">Wallet missing</span>' : number(row.availableCredits)}</td><td>${row.anonymous ? "&mdash;" : escapeHtml(row.newsletter?.status || "Not linked")}</td></tr>`).join("") || '<tr><td colspan="7"><div class="empty-state">No users match these filters.</div></td></tr>';
    $("pager").innerHTML = `<button class="button secondary" id="prev" ${data.pagination.page <= 1 ? "disabled" : ""}>Previous</button><span>Page ${data.pagination.page} of ${data.pagination.pages}</span><button class="button secondary" id="next" ${data.pagination.page >= data.pagination.pages ? "disabled" : ""}>Next</button>`;
    $("prev")?.addEventListener("click", () => { state.page--; load(); });
    $("next")?.addEventListener("click", () => { state.page++; load(); });
    document.querySelectorAll("[data-user-id]").forEach((row) => {
      const open = () => openUser(row.dataset.userId);
      row.addEventListener("click", open);
      row.addEventListener("keydown", (event) => { if (event.key === "Enter") open(); });
    });
    if (state.autoOpen) { const userId = state.autoOpen; state.autoOpen = null; openUser(userId); }
    updateStamp(data.asOf);
  } catch (error) { showError(error); }
}

function timeline(rows, empty) {
  return rows.length ? `<div class="timeline">${rows.join("")}</div>` : `<div class="empty-state">${escapeHtml(empty)}</div>`;
}

async function openUser(id) {
  try {
    const data = await json(`/api/operations/users/${id}`);
    $("drawerTitle").textContent = data.user.name || data.user.email;
    const jobs = data.reservations.slice(0, 16).map((row) => `<div><span class="timeline-dot ${row.status}"></span><p><strong>${escapeHtml(row.label)} · ${escapeHtml(row.status)}</strong><small>${escapeHtml(row.featureCode || row.rawToolCode || "General")} · ${number(row.credits)} credits · ${dateTime(row.occurredAt)}</small></p></div>`);
    const logins = data.sessions.slice(0, 10).map((row) => `<div><span class="timeline-dot committed"></span><p><strong>${escapeHtml(row.source)}</strong><small>${escapeHtml(row.country || "Unknown country")} · ${dateTime(row.completedAt)}</small></p></div>`);
    const purchases = data.purchases.slice(0, 8).map((row) => `<div><span class="timeline-dot ${row.status === "captured" ? "committed" : "released"}"></span><p><strong>${escapeHtml(row.productCode || row.kind || "Purchase")} · ${escapeHtml(row.status)}</strong><small>${dateTime(row.createdAt)}</small></p></div>`);
    const voice = [...data.feedback.map((row) => ({ label: `Feedback${(row.score ?? row.rating) !== null && (row.score ?? row.rating) !== undefined ? ` · ${row.score ?? row.rating}/${row.scale || 5}` : ""}`, detail: row.type || row.path || row.tool || row.source, at: row.createdAt })), ...data.featureRequests.map((row) => ({ label: row.title, detail: `Request · ${row.status || "unknown"}`, at: row.createdAt }))].sort((a, b) => new Date(b.at) - new Date(a.at)).slice(0, 8).map((row) => `<div><span class="timeline-dot"></span><p><strong>${escapeHtml(row.label)}</strong><small>${escapeHtml(row.detail || "Customer voice")} · ${dateTime(row.at)}</small></p></div>`);
    $("drawerBody").innerHTML = `<div class="profile-hero"><strong>${escapeHtml(data.user.email)}</strong><p>${data.user.segments.map((value) => chip(value)).join(" ")}</p></div><div class="detail-grid"><div><span>Joined</span><strong>${dateTime(data.user.joinedAt)}</strong></div><div><span>Lifecycle</span><strong>${escapeHtml(data.user.lifecycleStage)}</strong></div><div><span>Available credits</span><strong>${data.billing ? number(data.billing.availableCredits) : "Wallet not initialized"}</strong></div><div><span>Newsletter</span><strong>${escapeHtml(data.newsletter?.subscriber?.status || "Not linked")}</strong></div><div><span>Subscription</span><strong>${escapeHtml(data.billing?.subscriptionStatus || "No active subscription")}</strong></div><div><span>Lifetime credit spend</span><strong>${data.billing ? number(data.billing.lifetimeSpentCredits) : "Unavailable"}</strong></div></div><h3>Credited tool timeline</h3>${timeline(jobs, "No credited tool jobs.")}<h3>Successful logins</h3>${timeline(logins, "No successful login.")}<h3>Purchases and billing</h3>${timeline(purchases, "No purchase attempts.")}<h3>Feedback and requests</h3>${timeline(voice, "No matched feedback or feature requests.")}<h3>Favorites</h3><p>${data.user.favorites.length ? data.user.favorites.map((value) => chip(value)).join(" ") : "No favorites saved."}</p><div class="notice coverage-notice"><strong>Coverage:</strong> ${escapeHtml(data.coverage.message)}</div>`;
    $("userDrawer").classList.add("open");
    $("userDrawer").setAttribute("aria-hidden", "false");
    $("drawerBackdrop").hidden = false;
  } catch (error) { showError(error); }
}

function close() {
  $("userDrawer").classList.remove("open");
  $("userDrawer").setAttribute("aria-hidden", "true");
  $("drawerBackdrop").hidden = true;
}

$("closeDrawer").addEventListener("click", close);
$("drawerBackdrop").addEventListener("click", close);
document.addEventListener("keydown", (event) => { if (event.key === "Escape") close(); });
for (const id of filterIds) $(id).addEventListener("change", () => { state.page = 1; load(); });
$("search").addEventListener("input", () => { clearTimeout(timer); timer = setTimeout(() => { state.page = 1; load(); }, 300); });
$("refreshAll").addEventListener("click", load);

for (const id of ["segment", "wallet", "rangeDays", "pageSize"]) {
  const value = incoming.get(id === "rangeDays" ? "days" : id);
  if (value && [...$(id).options].some((item) => item.value === value)) $(id).value = value;
}
$("search").value = incoming.get("search") || "";
load();
