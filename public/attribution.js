import { $, dateTime, escapeHtml, mountSidebar, setupMobileMenu, showError, updateStamp } from "./intelligence-shared.js";

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

function renderCampaigns(items) {
  $("campaignsBody").innerHTML = items.length
    ? items.map((campaign) => `<tr><td><strong>${escapeHtml(campaign.name)}</strong></td><td><span class="tag">${escapeHtml(campaign.utmSource)}</span></td><td>${escapeHtml(campaign.utmMedium)}</td><td>${escapeHtml(campaign.utmCampaign)}</td><td>${escapeHtml(dateTime(campaign.createdAt))}</td><td><button class="button secondary copy-campaign" type="button" data-url="${escapeHtml(campaign.checkoutUrl)}">Copy link</button></td></tr>`).join("")
    : '<tr><td colspan="6"><div class="empty-state">No campaigns yet. Create Madhura above to get started.</div></td></tr>';
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
    const data = await api("/api/operations/attribution/campaigns");
    publicOrigin = data.publicOrigin || publicOrigin;
    renderCampaigns(data.items || []);
    updateStamp(new Date());
    previewUrl();
  } catch (error) {
    showError(error);
    $("campaignsBody").innerHTML = '<tr><td colspan="6"><div class="empty-state error-text">Campaigns are unavailable.</div></td></tr>';
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
loadCampaigns();
