import { $, escapeHtml, metricCard, mountSidebar, number, percent, setupMobileMenu, showError, updateStamp } from "./intelligence-shared.js";

mountSidebar("attribution");
setupMobileMenu();

let publicOrigin = "https://www.waysorted.com";

function formValue(id) { return $(id).value.trim(); }

function previewUrl() {
  try {
    const url = new URL(formValue("destinationPath") || "/payment", `${publicOrigin}/`);
    url.searchParams.set("utm_source", formValue("utmSource"));
    url.searchParams.set("utm_medium", formValue("utmMedium") || "referral");
    url.searchParams.set("utm_campaign", formValue("utmCampaign") || "checkout");
    $("linkPreview").textContent = url.toString();
  } catch { $("linkPreview").textContent = "Enter a valid destination path."; }
}

function showToast(message) {
  $("toast").textContent = message;
  $("toast").hidden = false;
  window.setTimeout(() => { $("toast").hidden = true; }, 2200);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}) },
    ...options,
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}

function moneyByCurrency(items) {
  if (!items?.length) return "—";
  return items.map(({ currency, amountSubunits }) => {
    try {
      return new Intl.NumberFormat("en-IN", {
        style: "currency",
        currency,
        maximumFractionDigits: 2,
      }).format(Number(amountSubunits) / 100);
    } catch {
      return `${currency} ${(Number(amountSubunits) / 100).toFixed(2)}`;
    }
  }).join(" · ");
}

function renderSummary(summary = {}) {
  $("attributionMetrics").classList.remove("skeleton-grid");
  $("attributionMetrics").innerHTML = [
    metricCard("Link opens", number(summary.opens || 0), `${number(summary.uniqueVisitors || 0)} unique browsers`),
    metricCard("Checkout attempts", number(summary.checkoutAttempts || 0), "All attributed purchase records"),
    metricCard("Successful purchases", number(summary.successfulPurchases || 0), `${number(summary.convertedVisitors || 0)} unique converted browsers`),
    metricCard("Visitor conversion", percent(summary.conversionRate || 0), "Unique converted browsers ÷ unique visitors"),
    metricCard("Net revenue", moneyByCurrency(summary.revenue), "Captured revenue less recorded refunds"),
  ].join("");
}

function renderCampaigns(items) {
  $("campaignsBody").innerHTML = items.length
    ? items.map((campaign) => {
      const metrics = campaign.metrics || {};
      return `<tr><td><strong>${escapeHtml(campaign.name)}</strong><small>${escapeHtml(campaign.utmSource)} / ${escapeHtml(campaign.utmCampaign)}</small></td><td>${number(metrics.opens || 0)}</td><td>${number(metrics.uniqueVisitors || 0)}</td><td>${number(metrics.checkoutAttempts || 0)}<small>${number(metrics.pendingAttempts || 0)} pending · ${number(metrics.failedAttempts || 0)} failed</small></td><td>${number(metrics.successfulPurchases || 0)}</td><td>${percent(metrics.conversionRate || 0)}</td><td>${escapeHtml(moneyByCurrency(metrics.revenue))}</td><td><button class="button secondary copy-campaign" type="button" data-url="${escapeHtml(campaign.checkoutUrl)}">Copy link</button></td></tr>`;
    }).join("")
    : '<tr><td colspan="8"><div class="empty-state">No campaigns yet. Create Madhura above to get started.</div></td></tr>';
  document.querySelectorAll(".copy-campaign").forEach((button) => {
    button.addEventListener("click", async () => {
      await navigator.clipboard.writeText(button.dataset.url);
      showToast("Campaign link copied");
    });
  });
}

async function loadCampaigns() {
  $("globalNotice").hidden = true;
  try {
    const data = await api(`/api/operations/attribution/campaigns?report=true&days=${encodeURIComponent($("rangeDays").value)}`);
    publicOrigin = data.publicOrigin || publicOrigin;
    renderSummary(data.summary);
    renderCampaigns(data.items || []);
    updateStamp(data.asOf || new Date());
    previewUrl();
  } catch (error) {
    showError(error);
    $("campaignsBody").innerHTML = '<tr><td colspan="8"><div class="empty-state error-text">Campaigns are unavailable.</div></td></tr>';
  }
}

$("campaignForm").addEventListener("input", previewUrl);
$("campaignForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("createCampaign");
  button.disabled = true;
  $("formStatus").textContent = "Creating…";
  try {
    const data = await api("/api/operations/attribution/campaigns", {
      method: "POST",
      body: JSON.stringify({ name: formValue("name"), utmSource: formValue("utmSource"), utmMedium: formValue("utmMedium"), utmCampaign: formValue("utmCampaign"), destinationPath: formValue("destinationPath") }),
    });
    await navigator.clipboard.writeText(data.campaign.checkoutUrl).catch(() => {});
    $("formStatus").textContent = "Created. Link copied.";
    showToast("Campaign created");
    await loadCampaigns();
  } catch (error) { $("formStatus").textContent = error.message; }
  finally { button.disabled = false; }
});

$("refreshCampaigns").addEventListener("click", loadCampaigns);
$("rangeDays").addEventListener("change", loadCampaigns);
loadCampaigns();
