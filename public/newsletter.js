const state = {
  overview: null,
  templates: [],
  currentView: "overview",
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

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
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString();
}

function statusPill(status) {
  const safe = String(status || "unknown").toLowerCase();
  const tone = ["sent", "active", "completed"].includes(safe)
    ? "good"
    : ["failed", "bounced", "unsubscribed"].includes(safe)
      ? "bad"
      : ["scheduled", "queued", "sending", "paused"].includes(safe)
        ? "warn"
        : "neutral";
  return `<span class="status-pill ${tone}">${escapeHtml(safe)}</span>`;
}

function showToast(message, error = false) {
  const toast = $("#newsletterToast");
  toast.textContent = message;
  toast.classList.toggle("error", error);
  toast.hidden = false;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => { toast.hidden = true; }, 4500);
}

function showNotice(message, error = false) {
  const notice = $("#newsletterNotice");
  notice.textContent = message;
  notice.classList.toggle("error", error);
  notice.hidden = !message;
}

async function api(path, options = {}) {
  const response = await fetch(`/api/newsletter${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
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

function kpi(label, value, sub = "") {
  return `<article class="kpi-card"><p class="kpi-label">${escapeHtml(label)}</p><p class="kpi-value">${escapeHtml(value)}</p><p class="kpi-sub">${escapeHtml(sub)}</p></article>`;
}

function setView(view, updateHash = true) {
  if (!$("#view-" + view)) view = "overview";
  state.currentView = view;
  $$(".newsletter-view").forEach((node) => { node.hidden = node.id !== `view-${view}`; });
  $$(".newsletter-tab").forEach((node) => node.classList.toggle("active", node.dataset.view === view));
  if (updateHash) history.replaceState(null, "", `#${view}`);
  loadView(view).catch(handleError);
}

async function loadView(view) {
  showNotice("");
  if (view === "overview") await loadOverview();
  if (view === "campaigns") await Promise.all([loadCampaigns(), loadTemplates()]);
  if (view === "subscribers") await loadSubscribers();
  if (view === "templates") await loadTemplates(true);
  if (view === "analytics") await loadAnalytics();
}

function renderJourney(element, title, summary) {
  const enabled = Boolean(summary?.enabled);
  element.innerHTML = `
    <div class="card-title-row"><div><p class="eyebrow">Automated journey</p><h2>${escapeHtml(title)}</h2></div>${statusPill(enabled ? "active" : "disabled")}</div>
    <div class="mini-stat-grid">
      <div><strong>${number(summary?.sent)}</strong><span>Sent</span></div>
      <div><strong>${percent(summary?.open_rate)}</strong><span>Open rate</span></div>
      <div><strong>${percent(summary?.ctr)}</strong><span>CTR</span></div>
      <div><strong>${number(summary?.scheduled_deliveries)}</strong><span>Scheduled</span></div>
    </div>
    <p class="muted">Rollout: ${escapeHtml(summary?.rollout_mode || "—")}${summary?.cooldown_days ? ` · ${number(summary.cooldown_days)}-day cooldown` : ""}</p>`;
}

async function loadOverview() {
  const data = await api("/overview");
  state.overview = data;
  $("#integrationStatus").className = "status-pill good";
  $("#integrationStatus").textContent = "Connected securely";
  $("#newsletterKpis").innerHTML = [
    kpi("Active subscribers", number(data.subscribers.active), `${number(data.subscribers.total)} total`),
    kpi("Campaigns", number(data.campaigns.total), `${number(data.campaigns.sent)} completed`),
    kpi("Active templates", number(data.templates.active), "Available to broadcasts"),
    kpi("Total delivered", number(Number(data.delivery.broadcast_sent) + Number(data.delivery.journey_sent)), `${number(data.delivery.journey_sent)} journey emails`),
  ].join("");
  renderJourney($("#journeyN1"), "N1 · Onboarding", data.journeys.n1);
  renderJourney($("#journeyN2"), "N2 · Low credits", data.journeys.n2);

  const dispatcher = data.scheduled_dispatcher;
  $("#dispatcherCard").innerHTML = `
    <div class="card-title-row"><div><p class="eyebrow">Broadcast engine</p><h2>Scheduled Dispatcher</h2></div>${statusPill(dispatcher.enabled ? "active" : "disabled")}</div>
    <div class="mini-stat-grid">
      <div><strong>${number(dispatcher.scheduled)}</strong><span>Scheduled</span></div>
      <div><strong>${number(dispatcher.due)}</strong><span>Due</span></div>
      <div><strong>${number(dispatcher.queued)}</strong><span>Queued</span></div>
      <div><strong>${number(dispatcher.failed)}</strong><span>Failed</span></div>
    </div>
    <p class="muted">Next run: ${escapeHtml(dateTime(dispatcher.next_scheduled_at))}</p>
    ${dispatcher.enabled ? "" : '<p class="safety-note">Safety lock is on. Scheduled campaigns will not dispatch until production approval.</p>'}`;

  const rows = data.recent_campaigns.map((campaign) => `
    <tr class="clickable-row" data-campaign-id="${campaign.id}">
      <td><strong>${escapeHtml(campaign.name)}</strong><small>${escapeHtml(campaign.subject)}</small></td>
      <td>${statusPill(campaign.status)}</td><td>${escapeHtml(campaign.recipient_source || "—")}</td>
      <td>${number(campaign.sent_count)}</td><td>${escapeHtml(dateTime(campaign.scheduled_at))}</td>
    </tr>`).join("");
  $("#recentCampaignsBody").innerHTML = rows || '<tr><td colspan="5" class="empty-cell">No broadcast campaigns yet.</td></tr>';
  bindCampaignRows();
}

async function loadCampaigns() {
  const params = new URLSearchParams({
    status: $("#campaignStatusFilter").value,
    search: $("#campaignSearch").value.trim(),
    per_page: "100",
  });
  const data = await api(`/campaigns?${params}`);
  $("#campaignsBody").innerHTML = data.campaigns.map((campaign) => `
    <tr class="clickable-row" data-campaign-id="${campaign.id}">
      <td><strong>${escapeHtml(campaign.name)}</strong></td><td>${escapeHtml(campaign.subject)}</td>
      <td>${statusPill(campaign.status)}</td><td>${number(campaign.total_recipients)}</td>
      <td>${number(campaign.sent_count)}</td><td>${escapeHtml(dateTime(campaign.scheduled_at))}</td>
      <td><button class="text-btn" type="button" data-campaign-id="${campaign.id}">Open</button></td>
    </tr>`).join("");
  $("#campaignsEmpty").hidden = data.campaigns.length > 0;
  bindCampaignRows();
}

function bindCampaignRows() {
  $$('[data-campaign-id]').forEach((node) => {
    node.addEventListener("click", (event) => {
      event.stopPropagation();
      openCampaignDetail(Number(node.dataset.campaignId)).catch(handleError);
    });
  });
}

async function openCampaignDetail(id) {
  const { campaign } = await api(`/campaigns/${id}`);
  $("#campaignDetailTitle").textContent = campaign.name;
  const a = campaign.analytics || {};
  $("#campaignDetailContent").innerHTML = `
    <div class="detail-grid">
      <div><span>Subject</span><strong>${escapeHtml(campaign.subject)}</strong></div>
      <div><span>Status</span><strong>${statusPill(campaign.status)}</strong></div>
      <div><span>Template</span><strong>${escapeHtml(campaign.template_name || "—")}</strong></div>
      <div><span>Schedule</span><strong>${escapeHtml(dateTime(campaign.scheduled_at))}</strong></div>
    </div>
    <div class="mini-stat-grid detail-stats">
      <div><strong>${number(campaign.sent_count)}</strong><span>Sent</span></div>
      <div><strong>${percent(a.open_rate)}</strong><span>Open rate</span></div>
      <div><strong>${percent(a.click_rate)}</strong><span>CTR</span></div>
      <div><strong>${number(campaign.failed_count)}</strong><span>Failed</span></div>
    </div>
    <h3>Recipients (first ${number(campaign.recipients_returned)})</h3>
    <div class="table-wrap"><table><thead><tr><th>Email</th><th>Status</th><th>Opened</th><th>Clicked</th></tr></thead><tbody>
      ${campaign.recipients.map((r) => `<tr><td>${escapeHtml(r.email)}</td><td>${statusPill(r.status)}</td><td>${r.opened ? "Yes" : "—"}</td><td>${r.clicked ? "Yes" : "—"}</td></tr>`).join("") || '<tr><td colspan="4" class="empty-cell">No recipients</td></tr>'}
    </tbody></table></div>`;

  const actions = [];
  if (["draft", "failed"].includes(campaign.status)) actions.push(`<button class="primary-btn" data-campaign-action="send" data-id="${id}">Send now</button>`);
  if (["scheduled", "queued", "sending"].includes(campaign.status)) actions.push(`<button class="secondary-btn" data-campaign-action="pause" data-id="${id}">Pause</button>`);
  if (campaign.status === "paused") actions.push(`<button class="secondary-btn" data-campaign-action="resume" data-id="${id}">Resume</button>`);
  if (!["queued", "sending", "sent"].includes(campaign.status)) actions.push(`<button class="danger-btn" data-campaign-action="delete" data-id="${id}">Delete</button>`);
  $("#campaignDetailActions").innerHTML = `<span class="dialog-spacer"></span>${actions.join("")}`;
  $$('[data-campaign-action]').forEach((button) => button.addEventListener("click", () => campaignAction(button.dataset.campaignAction, id)));
  $("#campaignDetailDialog").showModal();
}

async function campaignAction(action, id) {
  const dangerous = action === "send";
  if (dangerous) {
    const answer = window.prompt("This starts real email delivery. Type SEND to continue.");
    if (answer !== "SEND") return;
  } else {
    const prompt = action === "delete" ? "Delete this campaign? This cannot be undone." : `${action[0].toUpperCase()}${action.slice(1)} this campaign?`;
    if (!window.confirm(prompt)) return;
  }
  if (action === "delete") await api(`/campaigns/${id}`, { method: "DELETE" });
  else await api(`/campaigns/${id}/actions`, { method: "POST", body: JSON.stringify({ action }) });
  $("#campaignDetailDialog").close();
  showToast(`Campaign ${action} complete.`);
  await loadCampaigns();
}

async function loadSubscribers() {
  const params = new URLSearchParams({
    status: $("#subscriberStatusFilter").value,
    search: $("#subscriberSearch").value.trim(),
    per_page: "200",
  });
  const data = await api(`/subscribers?${params}`);
  $("#subscriberKpis").innerHTML = [
    kpi("Total", number(data.counts.total)), kpi("Active", number(data.counts.active)),
    kpi("Unsubscribed", number(data.counts.unsubscribed)), kpi("Bounced", number(data.counts.bounced)),
  ].join("");
  $("#subscribersBody").innerHTML = data.subscribers.map((subscriber) => `
    <tr><td><strong>${escapeHtml(subscriber.email)}</strong></td><td>${escapeHtml(subscriber.name || "—")}</td>
    <td>${statusPill(subscriber.status)}</td><td>${(subscriber.tags || []).map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join(" ") || "—"}</td>
    <td>${escapeHtml(dateTime(subscriber.created_at))}</td></tr>`).join("");
  $("#subscribersEmpty").hidden = data.subscribers.length > 0;
}

async function loadTemplates(render = false) {
  const data = await api("/templates");
  state.templates = data.templates;
  $("#campaignTemplate").innerHTML = '<option value="">Select a template</option>' + data.templates.map((t) => `<option value="${t.id}">${escapeHtml(t.name)} · v${number(t.version)}</option>`).join("");
  if (!render) return;
  $("#templatesBody").innerHTML = data.templates.map((template) => `
    <tr><td><strong>${escapeHtml(template.name)}</strong><small>${escapeHtml(template.description || "")}</small></td>
    <td>v${number(template.version)}</td><td>${(template.variables || []).map((v) => `<code>${escapeHtml(v)}</code>`).join(" ") || "—"}</td>
    <td>${escapeHtml(dateTime(template.updated_at))}</td><td class="table-actions">
      <button class="text-btn" data-template-preview="${template.id}">Preview</button>
      <button class="text-btn danger-text" data-template-delete="${template.id}">Retire</button>
    </td></tr>`).join("");
  $("#templatesEmpty").hidden = data.templates.length > 0;
  $$('[data-template-preview]').forEach((b) => b.addEventListener("click", () => previewTemplate(Number(b.dataset.templatePreview))));
  $$('[data-template-delete]').forEach((b) => b.addEventListener("click", () => deleteTemplate(Number(b.dataset.templateDelete))));
}

async function previewTemplate(id) {
  const template = state.templates.find((item) => item.id === id);
  const data = await api(`/templates/${id}/preview`, { method: "POST", body: JSON.stringify({ sample: {} }) });
  $("#templatePreviewTitle").textContent = template?.name || "Template preview";
  $("#templatePreviewFrame").srcdoc = data.html || data.html_content || "";
  $("#templatePreviewDialog").showModal();
}

async function deleteTemplate(id) {
  if (!window.confirm("Retire this template? Existing delivery history stays intact.")) return;
  await api(`/templates/${id}`, { method: "DELETE" });
  showToast("Template retired.");
  await loadTemplates(true);
}

async function loadAnalytics() {
  const data = await api(`/analytics?days=${encodeURIComponent($("#analyticsDays").value)}`);
  $("#analyticsKpis").innerHTML = [
    kpi("Delivered", number(data.total_sent), `Last ${number(data.days)} days`),
    kpi("Unique opens", number(data.total_opened), percent(data.open_rate)),
    kpi("Unique clicks", number(data.total_clicked), percent(data.click_rate)),
    kpi("Campaigns", number(data.broadcast.campaigns), "Broadcasts created"),
  ].join("");
  $("#broadcastAnalytics").innerHTML = `<p class="eyebrow">Broadcasts</p><h2>Campaign delivery</h2><div class="mini-stat-grid"><div><strong>${number(data.broadcast.sent)}</strong><span>Sent</span></div><div><strong>${number(data.broadcast.opened)}</strong><span>Opened</span></div><div><strong>${number(data.broadcast.clicked)}</strong><span>Clicked</span></div></div>`;
  $("#journeyAnalytics").innerHTML = `<p class="eyebrow">Automations</p><h2>Journey delivery</h2><div class="mini-stat-grid"><div><strong>${number(data.journeys.sent)}</strong><span>Sent</span></div><div><strong>${number(data.journeys.opened)}</strong><span>Opened</span></div><div><strong>${number(data.journeys.clicked)}</strong><span>Clicked</span></div></div>`;
  $("#topCampaignsBody").innerHTML = data.top_campaigns.map((campaign) => `<tr><td>${escapeHtml(campaign.campaign_name)}</td><td>${number(campaign.sent_count)}</td><td>${number(campaign.unique_opens)}</td><td>${percent(campaign.open_rate)}</td><td>${number(campaign.unique_clicks)}</td><td>${percent(campaign.click_rate)}</td></tr>`).join("") || '<tr><td colspan="6" class="empty-cell">No sent campaigns in this range.</td></tr>';
}

function campaignPayload() {
  const mode = $("#campaignMode").value;
  const source = $("#campaignAudience").value;
  let personalization = {};
  const raw = $("#campaignPersonalization").value.trim();
  if (raw) personalization = JSON.parse(raw);
  const payload = {
    name: $("#campaignName").value.trim(),
    subject: $("#campaignSubject").value.trim(),
    template_id: Number($("#campaignTemplate").value),
    send_mode: mode,
    recipient_source: source,
    global_personalization: personalization,
  };
  if (source === "custom") payload.recipients = $("#campaignRecipients").value.split(/[\n,]/).map((email) => email.trim()).filter(Boolean);
  if (mode === "scheduled") {
    const scheduled = $("#campaignSchedule").value;
    if (!scheduled) throw new Error("Choose a schedule date and time.");
    payload.schedule_at = new Date(scheduled).toISOString();
  }
  return payload;
}

async function previewCampaign() {
  const payload = campaignPayload();
  const data = await api("/campaigns/preview", { method: "POST", body: JSON.stringify({ template_id: payload.template_id, subject: payload.subject, sample: payload.global_personalization }) });
  $("#campaignPreviewSubject").textContent = data.subject || payload.subject;
  $("#campaignPreviewFrame").srcdoc = data.html || "";
  $("#campaignPreview").hidden = false;
}

function parseCsv(text) {
  const rows = [];
  let row = [], field = "", quoted = false;
  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (char === '"' && quoted && text[i + 1] === '"') { field += '"'; i += 1; }
    else if (char === '"') quoted = !quoted;
    else if (char === "," && !quoted) { row.push(field); field = ""; }
    else if ((char === "\n" || char === "\r") && !quoted) {
      if (char === "\r" && text[i + 1] === "\n") i += 1;
      row.push(field); if (row.some((cell) => cell.trim())) rows.push(row); row = []; field = "";
    } else field += char;
  }
  row.push(field); if (row.some((cell) => cell.trim())) rows.push(row);
  if (rows.length < 2) return [];
  const headers = rows.shift().map((item) => item.trim().toLowerCase());
  return rows.map((values) => Object.fromEntries(headers.map((key, index) => [key, values[index]?.trim() || ""]))).map((record) => ({ ...record, tags: record.tags ? record.tags.split(/[;|]/).map((tag) => tag.trim()).filter(Boolean) : [] }));
}

function handleError(error) {
  console.error(error);
  const message = error?.message || "Something went wrong.";
  showNotice(message, true);
  showToast(message, true);
  if (state.currentView === "overview") {
    $("#integrationStatus").className = "status-pill bad";
    $("#integrationStatus").textContent = "Integration unavailable";
  }
}

function openCampaignDialog() {
  $("#campaignForm").reset();
  $("#campaignPreview").hidden = true;
  $("#campaignScheduleField").hidden = true;
  $("#customAudienceField").hidden = true;
  $("#browserTimezone").textContent = `Timezone: ${Intl.DateTimeFormat().resolvedOptions().timeZone}`;
  loadTemplates().then(() => $("#campaignDialog").showModal()).catch(handleError);
}

$$('.newsletter-tab').forEach((button) => button.addEventListener("click", () => setView(button.dataset.view)));
$$('[data-open-view]').forEach((button) => button.addEventListener("click", () => setView(button.dataset.openView)));
$$('[data-close-dialog]').forEach((button) => button.addEventListener("click", () => $("#" + button.dataset.closeDialog).close()));
$("#refreshNewsletter").addEventListener("click", () => loadView(state.currentView).then(() => showToast("Newsletter data refreshed.")).catch(handleError));
$("#applyCampaignFilters").addEventListener("click", () => loadCampaigns().catch(handleError));
$("#applySubscriberFilters").addEventListener("click", () => loadSubscribers().catch(handleError));
$("#analyticsDays").addEventListener("change", () => loadAnalytics().catch(handleError));
$("#openCampaignDialog").addEventListener("click", openCampaignDialog);
$("#openSubscriberDialog").addEventListener("click", () => { $("#subscriberForm").reset(); $("#subscriberDialog").showModal(); });
$("#openImportDialog").addEventListener("click", () => { $("#importForm").reset(); $("#importDialog").showModal(); });
$("#openTemplateDialog").addEventListener("click", () => { $("#templateForm").reset(); $("#templateDialog").showModal(); });
$("#campaignMode").addEventListener("change", () => { $("#campaignScheduleField").hidden = $("#campaignMode").value !== "scheduled"; });
$("#campaignAudience").addEventListener("change", () => { $("#customAudienceField").hidden = $("#campaignAudience").value !== "custom"; });
$("#previewCampaign").addEventListener("click", () => previewCampaign().catch(handleError));
$("#campaignForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const payload = campaignPayload();
    if (payload.send_mode === "immediate") {
      const answer = window.prompt("This starts real email delivery immediately. Type SEND to continue.");
      if (answer !== "SEND") return;
    }
    if (payload.send_mode === "scheduled" && state.overview && !state.overview.scheduled_dispatcher.enabled) {
      if (!window.confirm("The scheduled dispatcher safety lock is currently OFF. Save the schedule anyway? It will not send until the dispatcher is enabled.")) return;
    }
    const result = await api("/campaigns", { method: "POST", body: JSON.stringify(payload) });
    $("#campaignDialog").close();
    showToast(`Campaign saved with ${number(result.recipients_created)} recipients.`);
    await loadCampaigns();
  } catch (error) { handleError(error); }
});
$("#subscriberForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/subscribers", { method: "POST", body: JSON.stringify({ email: $("#subscriberEmail").value, name: $("#subscriberName").value, tags: $("#subscriberTags").value }) });
    $("#subscriberDialog").close(); showToast("Subscriber saved."); await loadSubscribers();
  } catch (error) { handleError(error); }
});
$("#importForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const file = $("#subscriberCsv").files[0];
    const subscribers = parseCsv(await file.text());
    if (!subscribers.length) throw new Error("No subscriber rows found in the CSV.");
    if (subscribers.length > 5000) throw new Error("Maximum 5,000 subscribers per import.");
    const result = await api("/subscribers/import", { method: "POST", body: JSON.stringify({ subscribers }) });
    $("#importDialog").close(); showToast(`Imported ${number(result.imported)}, updated ${number(result.updated)}, skipped ${number(result.skipped)}.`); await loadSubscribers();
  } catch (error) { handleError(error); }
});
$("#templateForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/templates", { method: "POST", body: JSON.stringify({ name: $("#templateName").value, description: $("#templateDescription").value, html_content: $("#templateHtml").value, css_content: $("#templateCss").value }) });
    $("#templateDialog").close(); showToast("Template created."); await loadTemplates(true);
  } catch (error) { handleError(error); }
});
[["#templateHtmlFile", "#templateHtml"], ["#templateCssFile", "#templateCss"]].forEach(([fileId, targetId]) => $(fileId).addEventListener("change", async () => { const file = $(fileId).files[0]; if (file) $(targetId).value = await file.text(); }));
window.addEventListener("hashchange", () => setView(location.hash.slice(1) || "overview", false));

setView(location.hash.slice(1) || "overview", false);
