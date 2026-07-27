import { mountSidebar } from "./intelligence-shared.js";
mountSidebar("newsletter");
const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));

const state = {
  view: "overview",
  loaded: new Set(),
  overview: null,
  health: null,
  automations: [],
  templates: { broadcast: [], automation: [] },
  campaigns: [],
  campaignPage: 1,
  audiencePage: 1,
  audiencePages: 1,
  audiencePreview: null,
  wizardStep: 1,
  draftCampaign: null,
  testApproved: false,
  chart: null,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function number(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function percent(value) {
  return `${Number(value || 0).toFixed(1)}%`;
}

function dateTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function relativeTime(value) {
  if (!value) return "—";
  const ms = Date.now() - new Date(value).getTime();
  if (!Number.isFinite(ms)) return "—";
  const minutes = Math.round(ms / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function statusChip(status, label = null) {
  const value = String(status || "unknown").toLowerCase();
  const tone = ["healthy", "active", "enabled", "sent", "configured", "subscribed"].includes(value)
    ? "good"
    : ["failed", "unavailable", "misconfigured", "bounced"].includes(value)
      ? "bad"
      : ["degraded", "stale", "paused", "warning"].includes(value)
        ? "warn"
        : value === "draft" ? "blue" : "neutral";
  return `<span class="status-chip ${tone}">${escapeHtml(label || value.replaceAll("_", " "))}</span>`;
}

function showToast(message, error = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.style.background = error ? "#a60010" : "#1d1d1f";
  toast.hidden = false;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => { toast.hidden = true; }, 3600);
}

function showNotice(message = "") {
  const notice = $("#globalNotice");
  notice.textContent = message;
  notice.hidden = !message;
}

async function api(path, options = {}) {
  const response = await fetch(`/api/newsletter${path}`, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(payload.error || payload.message || `Request failed (${response.status})`);
    error.status = response.status;
    error.payload = payload;
    throw error;
  }
  return payload;
}

function handleError(error, context = "panel") {
  console.warn(`${context} failed`, error);
  const message = error?.message || "Something went wrong.";
  showNotice(`${context}: ${message}`);
  showToast(message, true);
}

function metric(label, value, change, suffix = "") {
  const delta = Number(change || 0);
  const tone = delta > 0 ? "up" : delta < 0 ? "down" : "";
  const arrow = delta > 0 ? "↑" : delta < 0 ? "↓" : "→";
  return `<article class="metric"><p class="metric-label">${escapeHtml(label)}</p><strong class="metric-value">${escapeHtml(value)}${suffix}</strong><span class="metric-change ${tone}">${arrow} ${Math.abs(delta).toFixed(1)}% vs previous period</span></article>`;
}

function setLastUpdated() {
  $("#lastUpdated").textContent = `Updated ${new Intl.DateTimeFormat(undefined, { timeStyle: "medium" }).format(new Date())}`;
}

function syncViewToUrl(view) {
  const url = new URL(location.href);
  url.hash = view;
  history.replaceState(null, "", url);
}

async function setView(view, updateUrl = true) {
  if (!$("#view-" + view)) view = "overview";
  state.view = view;
  $$(".view").forEach((section) => { section.hidden = section.id !== `view-${view}`; });
  $$(".section-nav button").forEach((button) => {
    const active = button.dataset.view === view;
    button.classList.toggle("active", active);
    if (active) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  if (updateUrl) syncViewToUrl(view);
  showNotice();
  try {
    await loadView(view);
    state.loaded.add(view);
    setLastUpdated();
  } catch (error) {
    handleError(error, `${view} panel`);
  }
}

async function loadView(view) {
  if (view === "overview") return loadOverview();
  if (view === "automations") return loadAutomations();
  if (view === "campaigns") return loadCampaigns();
  if (view === "audience") return loadAudience();
  if (view === "templates") return loadTemplates(true);
  if (view === "analytics") return loadAnalytics();
}

function renderAutomationCard(item) {
  const sent = item.sent ?? item.delivery_statuses?.sent ?? 0;
  const qualifying = item.checkout_triggers
    ?? item.low_credit_triggers
    ?? item.enrollments
    ?? 0;
  const qualifyingLabel = item.journey_key.startsWith("n2")
    ? "Triggers"
    : item.journey_key.startsWith("n3")
      ? "Checkouts"
      : "Enrollments";
  const operatingRule = [
    item.threshold_credits !== undefined
      ? `Threshold ${number(item.threshold_credits)}`
      : "",
    item.cooldown_days ? `${number(item.cooldown_days)}-day cooldown` : "",
    item.delay_minutes ? `${number(item.delay_minutes)}-minute delay` : "",
  ].filter(Boolean).join(" · ");
  return `<article class="surface automation-card">
    <div class="panel-head"><div><p class="overline">${escapeHtml(item.trigger)}</p><h3>${escapeHtml(item.name)}</h3></div>${statusChip(item.enabled ? "enabled" : "disabled")}</div>
    <p>${escapeHtml(item.description || "")}</p>
    <div class="automation-stats"><div><strong>${number(qualifying)}</strong><span>${qualifyingLabel}</span></div><div><strong>${number(sent)}</strong><span>Sent</span></div><div><strong>${percent(item.open_rate)}</strong><span>Open rate</span></div><div><strong>${percent(item.ctr)}</strong><span>CTR</span></div></div>
    <p class="footnote">Rollout: ${escapeHtml(item.rollout_mode || "—")}${operatingRule ? ` · ${escapeHtml(operatingRule)}` : ""}</p>
  </article>`;
}

function applyDispatcherState(overview) {
  const sendOption = $('#deliveryMode option[value="send"]');
  const scheduleOption = $('#deliveryMode option[value="schedule"]');
  const controls = overview?.scheduled_dispatcher;
  if (!sendOption || !scheduleOption || !controls) return;
  sendOption.disabled = !controls.send_enabled;
  sendOption.textContent = controls.send_enabled
    ? "Send now after confirmation"
    : "Send unavailable — safety locked";
  scheduleOption.disabled = !controls.enabled;
  scheduleOption.textContent = controls.enabled
    ? "Schedule after confirmation"
    : "Schedule unavailable — safety locked";
}

async function loadOverview() {
  const days = $("#rangeDays").value;
  const [overview, health] = await Promise.all([
    api(`/overview?days=${days}`),
    api("/system-health"),
  ]);
  state.overview = overview;
  state.health = health;
  applyDispatcherState(overview);
  const kpis = overview.kpis || {};
  $("#overviewKpis").innerHTML = [
    metric("Emails sent", number(kpis.sent?.value), kpis.sent?.change_percent),
    metric("Open rate", Number(kpis.open_rate?.value || 0).toFixed(1), kpis.open_rate?.change_percent, "%"),
    metric("Click rate", Number(kpis.click_rate?.value || 0).toFixed(1), kpis.click_rate?.change_percent, "%"),
    metric("Active audience", number(kpis.active_audience?.value), kpis.active_audience?.change_percent),
  ].join("");

  const attention = overview.attention || [];
  $("#attentionList").innerHTML = attention.map((item) => `<div class="attention-item ${escapeHtml(item.severity)}"><span class="attention-icon">${item.severity === "safe" ? "✓" : item.severity === "warning" ? "!" : "i"}</span><div><strong>${escapeHtml(item.title)}</strong><p>${escapeHtml(item.action)}</p></div></div>`).join("") || '<div class="empty-state">Nothing requires action right now.</div>';

  const components = health.components || {};
  const labels = { database: "Database", redis: "Redis", background_jobs: "Background jobs", email_provider: "Email provider", dispatcher: "Dispatcher" };
  $("#healthGrid").innerHTML = Object.entries(components).map(([key, item]) => `<div class="health-item"><span>${escapeHtml(labels[key] || key)}</span><strong>${statusChip(item.status)}</strong>${item.last_heartbeat_at ? `<small>${escapeHtml(relativeTime(item.last_heartbeat_at))}</small>` : ""}</div>`).join("");
  $("#overviewAutomations").innerHTML = [
    { summary: overview.journeys.n1, name: "N1 · Onboarding & Activation", trigger: "New account activated", description: "Four-step onboarding sequence: immediate, day 1, day 3 and day 7." },
    { summary: overview.journeys.n2, name: "N2 · Low Credits", trigger: "Credits at or below threshold", description: "Immediate account-relevant reminder for qualified loyalty users." },
    { summary: overview.journeys.n3, name: "N3 · Purchase Retention", trigger: "Checkout incomplete after two hours", description: "One delayed reminder that is cancelled when the matching purchase completes." },
  ].filter((item) => item.summary).map((item) => renderAutomationCard({
    ...item.summary,
    name: item.name,
    trigger: item.trigger,
    description: item.description,
  })).join("");

  const symbols = { sent: "↗", opened: "◉", clicked: "↗", enrolled: "+", failed: "!", skipped: "–", cancelled: "×", bounced: "!", unsubscribed: "–" };
  $("#activityFeed").innerHTML = (overview.recent_activity || []).map((item) => `<div class="activity-item"><span class="activity-symbol">${symbols[item.type] || "•"}</span><div><p><strong>${escapeHtml(item.type.replaceAll("_", " "))}</strong> · ${escapeHtml(item.journey_key || item.source || "Newsletter")}</p><small>${escapeHtml(item.step_key || (item.campaign_id ? `Campaign ${item.campaign_id}` : ""))}</small></div><time>${escapeHtml(relativeTime(item.occurred_at))}</time></div>`).join("") || '<div class="empty-state">No recent newsletter activity.</div>';

  const healthTone = health.status === "healthy" ? "good" : health.status === "degraded" ? "warn" : "bad";
  $("#integrationHealth").className = `status-chip ${healthTone}`;
  $("#integrationHealth").textContent = health.status === "healthy" ? "All systems operational" : `Integration ${health.status}`;
  $("#sidebarHealthDot").className = `health-dot ${healthTone}`;
  $("#sidebarHealthText").textContent = health.status === "healthy" ? "Systems operational" : `Systems ${health.status}`;
}

async function loadTemplates(render = false) {
  const data = await api("/content/templates?kind=all&include_content=true");
  state.templates = data;
  $("#campaignTemplate").innerHTML = '<option value="">Choose a template</option>' + data.broadcast.filter((item) => item.is_active).map((item) => `<option value="${item.id}">${escapeHtml(item.name)} · v${item.version}</option>`).join("");
  if (!render) return;
  const card = (item, readOnly = false) => `<article class="template-card"><div class="panel-head"><h4>${escapeHtml(item.name)}</h4>${statusChip(item.status)}</div><p>${escapeHtml(item.description || item.journey_name || "")}</p><p><strong>${escapeHtml(item.subject || `Version ${item.version}`)}</strong></p><p>${(item.variables || []).slice(0, 7).map((value) => `<span class="tag">${escapeHtml(value)}</span>`).join("") || "No variables"}</p><div class="template-actions"><button class="button secondary" data-template-preview="${item.kind}:${item.id}" type="button">Preview</button>${readOnly ? "" : `<button class="text-button" data-template-edit="${item.id}" type="button">New version</button><button class="text-button" data-template-clone="${item.id}" type="button">Clone</button><button class="text-button" data-template-retire="${item.id}" type="button">Retire</button>`}</div></article>`;
  $("#broadcastTemplateGrid").innerHTML = data.broadcast.filter((item) => item.is_active).map((item) => card(item)).join("") || '<div class="empty-state">No broadcast templates. Run the starter content seed or create one here.</div>';
  $("#automationTemplateGrid").innerHTML = data.automation.map((item) => card(item, true)).join("") || '<div class="empty-state">No automation templates found.</div>';
  bindTemplateActions();
}

async function loadAutomations() {
  const [automations] = await Promise.all([api("/automations"), loadTemplates(false)]);
  state.automations = automations.automations || [];
  $("#automationCards").innerHTML = state.automations.map(renderAutomationCard).join("");
  $("#automationTemplatesBody").innerHTML = state.templates.automation.map((item) => `<tr><td><strong>${escapeHtml(item.journey_name)}</strong><small>${escapeHtml(item.step_key)}</small></td><td>${escapeHtml(item.subject || "—")}</td><td>${(item.variables || []).slice(0, 5).map((value) => `<span class="tag">${escapeHtml(value)}</span>`).join("")}</td><td>${statusChip(item.status)}</td><td><button class="text-button" data-template-preview="automation:${item.id}">Preview</button></td></tr>`).join("");
  bindTemplateActions();
}

async function loadCampaigns(page = state.campaignPage) {
  if (!state.overview) {
    state.overview = await api(`/overview?days=${$("#rangeDays").value}`);
    applyDispatcherState(state.overview);
  }
  if (!state.templates.broadcast.length) await loadTemplates(false);
  state.campaignPage = page;
  const params = new URLSearchParams({
    page: String(page),
    per_page: "25",
    status: $("#campaignStatus").value,
    search: $("#campaignSearch").value.trim(),
  });
  const data = await api(`/campaigns?${params}`);
  state.campaigns = data.campaigns;
  $("#campaignsBody").innerHTML = data.campaigns.map((item) => `<tr data-id="${item.id}"><td><strong>${escapeHtml(item.name)}</strong><small>${escapeHtml(item.subject)}</small></td><td>${statusChip(item.status)}</td><td>${number(item.total_recipients)}</td><td>${number(item.sent_count)}</td><td>${item.test_send?.fingerprint_valid ? statusChip("healthy", "Approved") : statusChip("draft", "Required")}</td><td>${escapeHtml(dateTime(item.created_at))}</td></tr>`).join("") || '<tr><td colspan="6" class="empty-state">No campaigns found. Create a draft when your content is ready.</td></tr>';
  renderPager($("#campaignPager"), page, data.pages, data.total, (next) => loadCampaigns(next).catch((error) => handleError(error, "campaign list")));
  $$('[data-id]', $("#campaignsBody")).forEach((row) => row.addEventListener("click", () => openCampaignDetail(Number(row.dataset.id)).catch((error) => handleError(error, "campaign detail"))));
}

async function openCampaignDetail(id) {
  const data = await api(`/campaigns/${id}`);
  const item = data.campaign;
  $("#campaignDetailTitle").textContent = item.name;
  $("#campaignDetailBody").innerHTML = `<div class="review-grid"><div class="review-item"><span>Status</span><strong>${escapeHtml(item.status)}</strong></div><div class="review-item"><span>Recipients</span><strong>${number(item.total_recipients)}</strong></div><div class="review-item"><span>Template</span><strong>${escapeHtml(item.template_name || "—")}</strong></div><div class="review-item"><span>Owner test</span><strong>${item.test_send?.fingerprint_valid ? "Approved" : "Required"}</strong></div><div class="review-item"><span>Sent</span><strong>${number(item.sent_count)}</strong></div><div class="review-item"><span>Failed</span><strong>${number(item.failed_count)}</strong></div></div><p><strong>Subject:</strong> ${escapeHtml(item.subject)}</p>${item.status === "draft" ? `<div class="confirmation-box"><label>Recipient count<input id="detailConfirmCount" inputmode="numeric" value="${item.total_recipients}" /></label><label>Confirmation phrase<input id="detailConfirmPhrase" autocomplete="off" placeholder="SEND ${item.total_recipients}" /></label><p>Any content, audience, subscriber-status or suppression change invalidates the owner test.</p></div>` : ""}`;
  const buttons = ['<button class="button secondary" id="detailClose" type="button">Close</button>'];
  if (item.status === "draft") {
    buttons.push('<button class="button secondary" id="detailTest" type="button">Send owner test</button>');
    const sendEnabled = Boolean(state.overview?.scheduled_dispatcher?.send_enabled);
    buttons.push(`<button class="button danger" id="detailSend" type="button" ${item.test_send?.fingerprint_valid && sendEnabled ? "" : "disabled"}>${sendEnabled ? "Send campaign" : "Bulk send safety locked"}</button>`);
  }
  $("#campaignDetailActions").innerHTML = buttons.join("");
  if (!$("#campaignDetailDialog").open) {
    $("#campaignDetailDialog").showModal();
  }
  $("#detailClose").addEventListener("click", () => $("#campaignDetailDialog").close());
  $("#detailTest")?.addEventListener("click", async () => {
    try {
      await api(`/campaigns/${id}/test-send`, { method: "POST", body: "{}" });
      showToast("Owner test sent. Verify it before delivery.");
      await openCampaignDetail(id);
    } catch (error) {
      handleError(error, "campaign test send");
    }
  });
  $("#detailSend")?.addEventListener("click", async () => {
    try {
      await api(`/campaigns/${id}/actions`, {
        method: "POST",
        body: JSON.stringify({
          action: "send",
          confirm_recipient_count: Number($("#detailConfirmCount").value),
          confirmation_phrase: $("#detailConfirmPhrase").value,
        }),
      });
      $("#campaignDetailDialog").close();
      showToast("Campaign send queued.");
      await loadCampaigns();
    } catch (error) {
      handleError(error, "campaign send");
    }
  });
}

function renderPager(container, page, pages, total, onPage) {
  const safePages = Math.max(1, Number(pages || 1));
  container.innerHTML = `<span>${number(total)} records · Page ${page} of ${safePages}</span><span><button class="button secondary" data-page="${page - 1}" ${page <= 1 ? "disabled" : ""}>Previous</button> <button class="button secondary" data-page="${page + 1}" ${page >= safePages ? "disabled" : ""}>Next</button></span>`;
  $$('[data-page]', container).forEach((button) => button.addEventListener("click", () => onPage(Number(button.dataset.page))));
}

async function loadAudience(page = state.audiencePage) {
  state.audiencePage = page;
  const params = new URLSearchParams({
    page: String(page),
    per_page: $("#audiencePageSize").value,
    status: $("#audienceStatus").value,
    tag: $("#audienceTag").value,
    search: $("#audienceSearch").value.trim(),
  });
  const data = await api(`/subscribers?${params}`);
  state.audiencePages = data.pages;
  const currentTag = $("#audienceTag").value;
  $("#audienceTag").innerHTML = '<option value="all">All tags</option>' + (data.available_tags || []).map((tag) => `<option value="${escapeHtml(tag)}">${escapeHtml(tag)}</option>`).join("");
  if (["all", ...(data.available_tags || [])].includes(currentTag)) $("#audienceTag").value = currentTag;
  $("#audienceKpis").innerHTML = [
    metric("Total", number(data.counts.total), 0),
    metric("Active", number(data.counts.active), 0),
    metric("Unsubscribed", number(data.counts.unsubscribed), 0),
    metric("Bounced", number(data.counts.bounced), 0),
  ].join("");
  $("#audienceBody").innerHTML = data.subscribers.map((item) => `<tr data-subscriber-id="${item.id}"><td><strong>${escapeHtml(item.name || item.email)}</strong><small>${escapeHtml(item.email)}</small></td><td>${statusChip(item.status)}</td><td>${(item.tags || []).map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("") || "—"}</td><td>${escapeHtml(dateTime(item.created_at))}</td><td><button class="text-button" type="button">View profile</button></td></tr>`).join("") || '<tr><td colspan="5" class="empty-state">No customers match these filters.</td></tr>';
  renderPager($("#audiencePager"), page, data.pages, data.total, (next) => loadAudience(next).catch((error) => handleError(error, "audience")));
  $$('[data-subscriber-id]').forEach((row) => row.addEventListener("click", () => openCustomer(Number(row.dataset.subscriberId))));
  persistAudienceFilters();
}

function persistAudienceFilters() {
  const url = new URL(location.href);
  url.searchParams.set("audience_status", $("#audienceStatus").value);
  url.searchParams.set("audience_size", $("#audiencePageSize").value);
  url.searchParams.set("audience_tag", $("#audienceTag").value);
  if ($("#audienceSearch").value.trim()) url.searchParams.set("audience_search", $("#audienceSearch").value.trim());
  else url.searchParams.delete("audience_search");
  history.replaceState(null, "", url);
}

async function openCustomer(id) {
  const drawer = $("#customerDrawer");
  const backdrop = $("#drawerBackdrop");
  drawer.classList.add("open");
  drawer.setAttribute("aria-hidden", "false");
  backdrop.hidden = false;
  $("#customerDrawerBody").innerHTML = '<div class="skeleton-lines"></div>';
  try {
    const data = await api(`/customers/${id}`);
    const sub = data.subscriber;
    const profile = data.notification_profile;
    const billing = data.billing || {};
    const credit = data.credit_activity || {};
    const coverage = data.data_coverage || {};
    $("#customerDrawerTitle").textContent = sub.name || sub.email;
    $("#customerDrawerBody").innerHTML = `<div class="profile-hero"><div class="avatar">${escapeHtml((sub.name || sub.email).slice(0, 2).toUpperCase())}</div><div><strong>${escapeHtml(sub.name || "Unnamed customer")}</strong><div>${escapeHtml(sub.email)}</div>${statusChip(sub.status)}</div></div>
      <section class="profile-section"><h3>Identity & communication</h3><div class="definition-grid"><div><span>Profile ID</span><strong>${escapeHtml(profile?.id || "Not linked")}</strong></div><div><span>Waysorted user ID</span><strong>${escapeHtml(profile?.external_user_id || "Not available")}</strong></div><div><span>Consent</span><strong>${escapeHtml(JSON.stringify(profile?.consent || {}))}</strong></div><div><span>Suppression</span><strong>${data.suppressions.length ? escapeHtml(data.suppressions.map((item) => item.reason).join(", ")) : "None"}</strong></div></div><p>${(data.preferences || []).map((item) => `<span class="tag">${escapeHtml(item.category)}: ${escapeHtml(item.status)}</span>`).join("") || "No explicit preferences recorded."}</p></section>
      <section class="profile-section"><h3>Billing & credit activity</h3><p>${escapeHtml(coverage.message || "No matching Waysorted billing data.")}</p><div class="definition-grid"><div><span>Wallet</span><strong>${escapeHtml(billing.walletStatus === "initialized" ? "Initialized" : "Not initialized")}</strong></div><div><span>Available credits</span><strong>${billing.availableCredits === null || billing.availableCredits === undefined ? "Unavailable" : number(billing.availableCredits)}</strong></div><div><span>Held credits</span><strong>${billing.heldCredits === null || billing.heldCredits === undefined ? "—" : number(billing.heldCredits)}</strong></div><div><span>Lifetime spent</span><strong>${billing.lifetimeSpentCredits === null || billing.lifetimeSpentCredits === undefined ? "—" : number(billing.lifetimeSpentCredits)}</strong></div><div><span>Subscription</span><strong>${escapeHtml(billing.subscriptionStatus || "Inactive")}</strong></div><div><span>Latest credited usage</span><strong>${escapeHtml(dateTime(credit.latestAt))}</strong></div></div><p>${(credit.tools || []).slice(0, 8).map((item) => `<span class="tag">${escapeHtml(item.tool)} · ${number(item.creditsSpent)} credits</span>`).join("") || "No completed credit-consuming activity."}</p></section>
      <section class="profile-section"><h3>Automations</h3>${(data.enrollments || []).map((item) => `<div class="attention-item"><span class="attention-icon">✉</span><div><strong>${escapeHtml(item.journey_name)}</strong><p>${escapeHtml(item.status)} · ${escapeHtml(item.current_step || item.exit_reason || "No current step")}</p></div></div>`).join("") || '<p class="footnote">No journey enrollment.</p>'}</section>
      <section class="profile-section"><h3>Recent email history</h3>${[...(data.deliveries || []).map((item) => ({ label: `${item.journey_key || "Automation"} · ${item.step_key}`, status: item.status, at: item.sent_at || item.scheduled_at })), ...(data.broadcast_history || []).map((item) => ({ label: item.campaign_name || `Campaign ${item.campaign_id}`, status: item.status, at: item.sent_at }))].sort((a,b) => String(b.at).localeCompare(String(a.at))).slice(0, 20).map((item) => `<div class="activity-item"><span class="activity-symbol">✉</span><div><p><strong>${escapeHtml(item.label)}</strong></p><small>${escapeHtml(item.status)}</small></div><time>${escapeHtml(relativeTime(item.at))}</time></div>`).join("") || '<p class="footnote">No delivery history.</p>'}</section>`;
  } catch (error) {
    $("#customerDrawerBody").innerHTML = `<div class="notice error">${escapeHtml(error.message)}</div><button class="button secondary" id="retryCustomer">Retry</button>`;
    $("#retryCustomer")?.addEventListener("click", () => openCustomer(id));
  }
}

function closeCustomer() {
  $("#customerDrawer").classList.remove("open");
  $("#customerDrawer").setAttribute("aria-hidden", "true");
  $("#drawerBackdrop").hidden = true;
}

async function loadAnalytics() {
  const data = await api(`/analytics?days=${$("#rangeDays").value}&bucket=day`);
  $("#analyticsKpis").innerHTML = [
    metric("Emails sent", number(data.total_sent), data.comparisons.sent.change_percent),
    metric("Open rate", Number(data.open_rate).toFixed(1), data.comparisons.open_rate.change_percent, "%"),
    metric("Click rate", Number(data.click_rate).toFixed(1), data.comparisons.click_rate.change_percent, "%"),
    metric("Failed", number(data.total_failed), data.comparisons.failed.change_percent),
  ].join("");
  if (state.chart) state.chart.destroy();
  state.chart = new Chart($("#deliveryChart"), {
    type: "line",
    data: {
      labels: data.series.map((item) => item.date),
      datasets: [
        { label: "Sent", data: data.series.map((item) => item.sent), borderColor: "#0071e3", backgroundColor: "rgba(0,113,227,.1)", tension: .3 },
        { label: "Opened", data: data.series.map((item) => item.opened), borderColor: "#248a3d", tension: .3 },
        { label: "Clicked", data: data.series.map((item) => item.clicked), borderColor: "#8e44ad", tension: .3 },
        { label: "Failed", data: data.series.map((item) => item.failed), borderColor: "#d70015", tension: .3 },
      ],
    },
    options: { responsive: true, maintainAspectRatio: false, interaction: { intersect: false, mode: "index" }, plugins: { legend: { position: "bottom" } }, scales: { y: { beginAtZero: true, ticks: { precision: 0 } } } },
  });
  $("#journeyAnalyticsBody").innerHTML = (data.journey_breakdown || []).map((item) => `<tr><td>${escapeHtml(item.name)}</td><td>${number(item.sent)}</td><td>${percent(item.open_rate)}</td><td>${percent(item.click_rate)}</td></tr>`).join("") || '<tr><td colspan="4">No journey sends in this range.</td></tr>';
  $("#stepAnalyticsBody").innerHTML = (data.step_breakdown || []).filter((item) => item.journey_key === "n1_onboarding_activation").map((item) => `<tr><td>${escapeHtml(item.step_key)}</td><td>${number(item.sent)}</td><td>${number(item.opened)}</td><td>${number(item.clicked)}</td><td>${percent(item.open_rate)}</td><td>${percent(item.click_rate)}</td></tr>`).join("") || '<tr><td colspan="6">No N1 sends in this range.</td></tr>';
  $("#failureReasons").innerHTML = (data.failure_reasons || []).map((item) => `<div class="rank-item"><span>${escapeHtml(item.reason)}</span><strong>${number(item.count)}</strong></div>`).join("") || '<div class="empty-state">No failures in this range.</div>';
  $("#trackingNote").textContent = data.tracking_note || "";
}

function selectedBroadcastTemplate() {
  const id = Number($("#campaignTemplate").value);
  return state.templates.broadcast.find((item) => item.id === id);
}

function commonVariables() {
  return new Set(["email", "name", "first_name", "display_name", "unsubscribe_url"]);
}

function renderPersonalizationFields() {
  const template = selectedBroadcastTemplate();
  const variables = (template?.variables || []).filter((value) => !commonVariables().has(value));
  $("#personalizationFields").innerHTML = variables.map((variable) => `<label>${escapeHtml(variable.replaceAll("_", " "))}<input data-variable="${escapeHtml(variable)}" placeholder="${escapeHtml(variable)}" /></label>`).join("") || '<p class="footnote">This template has no campaign-specific variables.</p>';
}

function personalization() {
  const values = {};
  $$('[data-variable]', $("#personalizationFields")).forEach((input) => {
    if (input.value.trim()) values[input.dataset.variable] = input.value.trim();
  });
  const advanced = $("#advancedPersonalization").value.trim();
  if (advanced) Object.assign(values, JSON.parse(advanced));
  return values;
}

function audiencePayload() {
  const source = $("#audienceSource").value;
  const payload = { recipient_source: source };
  if (source === "tags") payload.tags = $("#audienceTags").value.split(",").map((item) => item.trim()).filter(Boolean);
  if (source === "custom") payload.recipients = $("#audienceEmails").value.split(/[\n,]/).map((item) => item.trim()).filter(Boolean);
  return payload;
}

function draftPayload() {
  return {
    name: $("#campaignName").value.trim(),
    subject: $("#campaignSubject").value.trim(),
    template_id: Number($("#campaignTemplate").value),
    send_mode: "draft",
    global_personalization: personalization(),
    ...audiencePayload(),
  };
}

async function refreshCampaignPreview() {
  const payload = draftPayload();
  if (!payload.template_id) throw new Error("Choose a broadcast template first.");
  const data = await api("/campaigns/preview", { method: "POST", body: JSON.stringify({ template_id: payload.template_id, subject: payload.subject, sample: payload.global_personalization }) });
  $("#previewSubject").textContent = data.subject;
  $("#campaignPreviewFrame").srcdoc = data.html;
}

async function calculateAudience() {
  const data = await api("/campaigns/audience-preview", { method: "POST", body: JSON.stringify(audiencePayload()) });
  state.audiencePreview = data;
  const fields = [["Requested", data.requested], ["Active", data.active], ["Suppressed", data.suppressed], ["Invalid", data.invalid], ["Final", data.final_recipients]];
  $("#audiencePreview").className = "audience-preview";
  $("#audiencePreview").innerHTML = fields.map(([label, value]) => `<div class="audience-count"><strong>${number(value)}</strong><span>${label}</span></div>`).join("");
  return data;
}

function validateWizardStep(step) {
  if (step === 1) {
    if (!$("#campaignName").value.trim() || !$("#campaignSubject").value.trim() || !$("#campaignTemplate").value) throw new Error("Complete campaign name, template and subject.");
    personalization();
  }
  if (step === 2 && !state.audiencePreview) throw new Error("Calculate and review the audience first.");
  if (step === 2 && !state.audiencePreview.final_recipients) throw new Error("The final audience is empty.");
  if (step === 3 && $("#deliveryMode").value !== "manual" && !state.testApproved) throw new Error("Send and verify the owner test before continuing.");
}

function setWizardStep(step) {
  state.wizardStep = Math.min(4, Math.max(1, step));
  $$('[data-step]').forEach((section) => { section.hidden = Number(section.dataset.step) !== state.wizardStep; });
  $$('[data-step-label]').forEach((item) => {
    const value = Number(item.dataset.stepLabel);
    item.classList.toggle("active", value === state.wizardStep);
    item.classList.toggle("done", value < state.wizardStep);
  });
  $("#previousStep").hidden = state.wizardStep === 1;
  $("#nextStep").hidden = state.wizardStep === 4;
  if (state.wizardStep === 4) renderCampaignReview();
}

function openWizard() {
  state.wizardStep = 1;
  state.draftCampaign = null;
  state.testApproved = false;
  state.audiencePreview = null;
  $("#campaignForm").reset();
  $("#campaignListPanel").hidden = true;
  $("#campaignWizard").hidden = false;
  $("#audiencePreview").className = "audience-preview empty-state";
  $("#audiencePreview").textContent = "Preview the audience before saving.";
  $("#testResult").innerHTML = "";
  setWizardStep(1);
}

function closeWizard() {
  $("#campaignWizard").hidden = true;
  $("#campaignListPanel").hidden = false;
  loadCampaigns(1).catch((error) => handleError(error, "campaign list"));
}

async function saveDraftAndTest() {
  validateWizardStep(1);
  validateWizardStep(2);
  const button = $("#saveAndTest");
  button.disabled = true;
  button.textContent = "Sending test…";
  try {
    if (!state.draftCampaign) {
      const created = await api("/campaigns", { method: "POST", body: JSON.stringify(draftPayload()) });
      state.draftCampaign = { id: created.campaign_id, total_recipients: created.total_recipients, status: created.status };
    }
    const result = await api(`/campaigns/${state.draftCampaign.id}/test-send`, { method: "POST", body: "{}" });
    state.testApproved = result.fingerprint_valid;
    $("#testResult").innerHTML = `<p>${statusChip("healthy", "Test sent")}</p><p class="footnote">Sent ${escapeHtml(dateTime(result.sent_at))} to ${escapeHtml(result.test_email)}.</p>`;
    showToast("Owner test email sent. Verify it before final delivery.");
  } finally {
    button.disabled = false;
    button.textContent = state.testApproved ? "Send test again" : "Save draft & send test";
  }
}

function renderCampaignReview() {
  const count = state.audiencePreview?.final_recipients || state.draftCampaign?.total_recipients || 0;
  const mode = $("#deliveryMode").value;
  const template = selectedBroadcastTemplate();
  $("#campaignReview").innerHTML = [["Campaign", $("#campaignName").value], ["Template", template?.name || "—"], ["Subject", $("#campaignSubject").value], ["Audience", `${count} final recipients`], ["Mode", mode], ["Owner test", state.testApproved ? "Approved" : "Not required for draft only"]].map(([label, value]) => `<div class="review-item"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`).join("");
  $("#confirmCount").value = mode === "manual" ? "" : String(count);
  $("#confirmPhrase").placeholder = `SEND ${count}`;
  $("#confirmationHelp").textContent = mode === "manual" ? "The campaign will remain a draft. No email will be sent." : `Enter ${count} and type SEND ${count} exactly. This is validated by the server.`;
  const final = $("#finalCampaignAction");
  final.textContent = mode === "manual" ? "Finish and keep draft" : mode === "schedule" ? "Schedule campaign" : "Send campaign";
  final.className = mode === "manual" ? "button primary" : "button danger";
  final.disabled = mode !== "manual" && !state.testApproved;
}

async function completeCampaignAction() {
  const mode = $("#deliveryMode").value;
  if (mode === "manual") {
    showToast("Draft saved. No email was sent.");
    closeWizard();
    return;
  }
  if (!state.draftCampaign) throw new Error("Save and test the draft first.");
  const body = {
    action: mode,
    confirm_recipient_count: Number($("#confirmCount").value),
    confirmation_phrase: $("#confirmPhrase").value,
  };
  if (mode === "schedule") {
    if (!$("#scheduleAt").value) throw new Error("Choose a schedule time.");
    body.schedule_at = new Date($("#scheduleAt").value).toISOString();
  }
  await api(`/campaigns/${state.draftCampaign.id}/actions`, { method: "POST", body: JSON.stringify(body) });
  showToast(mode === "schedule" ? "Campaign scheduled." : "Campaign send was queued.");
  closeWizard();
}

function findTemplate(kind, id) {
  return state.templates[kind]?.find((item) => item.id === Number(id));
}

async function previewTemplate(kind, id) {
  const item = findTemplate(kind, id);
  if (!item) return;
  let html = item.html_content;
  if (kind === "broadcast") {
    const data = await api(`/templates/${id}/preview`, { method: "POST", body: JSON.stringify({ sample: {} }) });
    html = data.html;
  }
  $("#templatePreviewTitle").textContent = item.name;
  $("#templatePreviewFrame").srcdoc = html || "<p>No preview content.</p>";
  $("#previewDialog").showModal();
}

function openTemplateForm(item = null, clone = false) {
  $("#templateForm").reset();
  $("#templateEditId").value = item && !clone ? item.id : "";
  $("#templateDialogTitle").textContent = item && !clone ? "Create new version" : clone ? "Clone broadcast template" : "New broadcast template";
  $("#templateName").value = item ? `${item.name}${clone ? " Copy" : ""}` : "";
  $("#templateDescription").value = item?.description || "";
  $("#templateHtml").value = item?.html_content || "";
  $("#templateCss").value = item?.css_content || "";
  $("#templateDialog").showModal();
}

function bindTemplateActions() {
  $$('[data-template-preview]').forEach((button) => button.addEventListener("click", () => {
    const [kind, id] = button.dataset.templatePreview.split(":");
    previewTemplate(kind, Number(id)).catch((error) => handleError(error, "template preview"));
  }));
  $$('[data-template-edit]').forEach((button) => button.addEventListener("click", () => openTemplateForm(findTemplate("broadcast", button.dataset.templateEdit))));
  $$('[data-template-clone]').forEach((button) => button.addEventListener("click", () => openTemplateForm(findTemplate("broadcast", button.dataset.templateClone), true)));
  $$('[data-template-retire]').forEach((button) => button.addEventListener("click", async () => {
    if (!window.confirm("Retire this template? Existing campaign history will remain.")) return;
    try {
      await api(`/templates/${button.dataset.templateRetire}`, { method: "DELETE" });
      showToast("Broadcast template retired.");
      await loadTemplates(true);
    } catch (error) {
      handleError(error, "template retirement");
    }
  }));
}

async function saveTemplate() {
  const editId = $("#templateEditId").value;
  const payload = { name: $("#templateName").value.trim(), description: $("#templateDescription").value.trim(), html_content: $("#templateHtml").value, css_content: $("#templateCss").value };
  if (!payload.name || !payload.html_content.trim()) throw new Error("Template name and HTML are required.");
  await api(editId ? `/templates/${editId}` : "/templates", { method: editId ? "PUT" : "POST", body: JSON.stringify(payload) });
  $("#templateDialog").close();
  showToast(editId ? "New immutable template version created." : "Broadcast template created.");
  await loadTemplates(true);
}

function hydrateFiltersFromUrl() {
  const params = new URLSearchParams(location.search);
  if (params.get("audience_status")) $("#audienceStatus").value = params.get("audience_status");
  if (["25", "50", "100"].includes(params.get("audience_size"))) $("#audiencePageSize").value = params.get("audience_size");
  if (params.get("audience_tag")) $("#audienceTag").innerHTML += `<option value="${escapeHtml(params.get("audience_tag"))}" selected>${escapeHtml(params.get("audience_tag"))}</option>`;
  if (params.get("audience_search")) $("#audienceSearch").value = params.get("audience_search");
}

function debounce(fn, wait = 350) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), wait); };
}

function bindEvents() {
  $$(".section-nav button").forEach((button) => button.addEventListener("click", () => setView(button.dataset.view)));
  $$('[data-view-link]').forEach((button) => button.addEventListener("click", () => setView(button.dataset.viewLink)));
  $$('[data-retry]').forEach((button) => button.addEventListener("click", () => setView(button.dataset.retry, false)));
  $("#refreshAll").addEventListener("click", () => setView(state.view, false));
  $("#rangeDays").addEventListener("change", () => setView(state.view, false));
  $("#menuToggle").addEventListener("click", () => {
    const open = $("#sidebar").classList.toggle("open");
    $("#menuToggle").setAttribute("aria-expanded", String(open));
  });

  $("#campaignStatus").addEventListener("change", () => loadCampaigns(1).catch((error) => handleError(error, "campaign list")));
  $("#campaignSearch").addEventListener("input", debounce(() => loadCampaigns(1).catch((error) => handleError(error, "campaign list"))));
  $("#audienceStatus").addEventListener("change", () => loadAudience(1).catch((error) => handleError(error, "audience")));
  $("#audienceTag").addEventListener("change", () => loadAudience(1).catch((error) => handleError(error, "audience")));
  $("#audiencePageSize").addEventListener("change", () => loadAudience(1).catch((error) => handleError(error, "audience")));
  $("#audienceSearch").addEventListener("input", debounce(() => loadAudience(1).catch((error) => handleError(error, "audience"))));
  $("#closeCustomerDrawer").addEventListener("click", closeCustomer);
  $("#drawerBackdrop").addEventListener("click", closeCustomer);

  $("#startCampaign").addEventListener("click", openWizard);
  $("#closeWizard").addEventListener("click", closeWizard);
  $("#campaignTemplate").addEventListener("change", () => { renderPersonalizationFields(); refreshCampaignPreview().catch(() => {}); });
  $("#previewCampaign").addEventListener("click", () => refreshCampaignPreview().catch((error) => handleError(error, "campaign preview")));
  $("#audienceSource").addEventListener("change", () => {
    $("#tagAudienceField").hidden = $("#audienceSource").value !== "tags";
    $("#customAudienceField").hidden = $("#audienceSource").value !== "custom";
    state.audiencePreview = null;
  });
  $("#previewAudience").addEventListener("click", () => calculateAudience().catch((error) => handleError(error, "audience preview")));
  $("#nextStep").addEventListener("click", () => { try { validateWizardStep(state.wizardStep); setWizardStep(state.wizardStep + 1); } catch (error) { showToast(error.message, true); } });
  $("#previousStep").addEventListener("click", () => setWizardStep(state.wizardStep - 1));
  $("#saveAndTest").addEventListener("click", () => saveDraftAndTest().catch((error) => handleError(error, "test send")));
  $("#deliveryMode").addEventListener("change", () => {
    const mode = $("#deliveryMode").value;
    $("#scheduleField").hidden = mode !== "schedule";
    if (mode === "send" && state.overview && !state.overview.scheduled_dispatcher.send_enabled) {
      $("#dispatcherExplanation").textContent = "Bulk sending is unavailable because the production broadcast safety lock is on.";
    } else if (mode === "schedule" && state.overview && !state.overview.scheduled_dispatcher.enabled) {
      $("#dispatcherExplanation").textContent = "Scheduling is unavailable because the production dispatcher safety lock is on.";
    } else $("#dispatcherExplanation").textContent = mode === "manual" ? "No email will be sent." : "A valid owner test and exact confirmation are required.";
  });
  $("#finalCampaignAction").addEventListener("click", () => completeCampaignAction().catch((error) => handleError(error, "campaign action")));

  $("#newTemplate").addEventListener("click", () => openTemplateForm());
  $("#importTemplate").addEventListener("click", () => $("#templateFile").click());
  $("#templateFile").addEventListener("change", async () => {
    const file = $("#templateFile").files?.[0];
    if (!file) return;
    const html = await file.text();
    openTemplateForm();
    $("#templateDialogTitle").textContent = "Import broadcast template";
    $("#templateName").value = file.name.replace(/\.html?$/i, "");
    $("#templateHtml").value = html;
    $("#templateFile").value = "";
  });
  $("#saveTemplate").addEventListener("click", () => saveTemplate().catch((error) => handleError(error, "template save")));
  $("#closePreview").addEventListener("click", () => $("#previewDialog").close());
  $("#closeCampaignDetail").addEventListener("click", () => $("#campaignDetailDialog").close());
  document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeCustomer(); });
}

async function init() {
  hydrateFiltersFromUrl();
  bindEvents();
  const initial = location.hash.replace("#", "") || "overview";
  await setView(initial, false);
}

init().catch((error) => handleError(error, "dashboard initialization"));
