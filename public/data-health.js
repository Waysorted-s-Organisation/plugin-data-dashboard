import { $, escapeHtml, json, mountSidebar, number, percent, relative, setupMobileMenu, showError, updateStamp } from "./intelligence-shared.js";

mountSidebar("health");
setupMobileMenu();

async function load() {
  try {
    const data = await json("/api/operations/data-health");
    const components = [
      { label: "Waysorted database", status: data.components.backendDatabase.status, detail: `${number(data.components.backendDatabase.users)} users` },
      { label: "Newsletter integration", status: data.components.newsletter.status, detail: data.components.newsletter.status === "configured" ? "Connected server-side" : "Newsletter joins unavailable" },
      { label: "Plugin behavior telemetry", status: data.components.telemetry.status, detail: data.components.telemetry.status === "warming_up" ? `${number(data.components.telemetry.healthyDays)} of 7 coverage days` : data.components.telemetry.latestAt ? `Last event ${relative(data.components.telemetry.latestAt)}` : "No verified recent event" },
      { label: "Plugin identity", status: data.components.identity?.status || "unavailable", detail: data.components.identity?.devicesPerSession === undefined ? "Not assessed" : `${number(data.components.identity.devices)} device ids across ${number(data.components.identity.sessions)} sessions` },
      { label: "Operations API", status: data.api.status, detail: data.api.averageResponseMs === null ? "Collecting response-time data" : `${number(data.api.averageResponseMs)} ms average · ${number(data.api.failedRequests)} failed` },
    ];
    $("healthCards").innerHTML = components.map((row) => `<article class="health-card"><span class="status-chip ${row.status === "healthy" || row.status === "configured" || row.status === "stable" ? "good" : ["stale", "degraded", "warming_up", "per_launch"].includes(row.status) ? "warn" : "bad"}">${escapeHtml(row.status)}</span><h3>${escapeHtml(row.label)}</h3><p>${escapeHtml(row.detail)}</p></article>`).join("");
    $("freshness").innerHTML = Object.entries(data.freshness).map(([key, value]) => `<p><strong>${escapeHtml(key.replaceAll(/([A-Z])/g, " $1"))}</strong><span>${relative(value)}</span></p>`).join("");
    const wallet = data.coverage.wallets;
    const sessions = data.coverage.sessions;
    const attribution = data.coverage.toolAttribution;
    const identity = data.coverage.identityJoins;
    $("coverage").innerHTML = `<p><strong>Wallet coverage · ${percent(wallet.percent)}</strong><span>${number(wallet.initialized)} initialized · ${number(wallet.missing)} missing. Missing wallets show as unavailable.</span></p><p><strong>Successful sessions · ${number(sessions.completed)}</strong><span>${number(sessions.incompleteLinked)} user-linked sessions are incomplete and excluded from active-user metrics.</span></p><p><strong>Identity joins</strong><span>${number(identity.sessionsWithoutUser)} sessions and ${number(identity.reservationsWithoutUser)} reservations have no linked user.</span></p><p><strong>Tool jobs · ${number(data.coverage.toolJobs?.backgroundCompletionsExcluded)} background completions excluded</strong><span>${escapeHtml(data.coverage.toolJobs?.message || "")}${data.coverage.toolJobs?.ungatedTools?.length ? ` Ungated: ${escapeHtml(data.coverage.toolJobs.ungatedTools.join(", "))}.` : ""}</span></p><p><strong>Plugin identity stability</strong><span>${escapeHtml(data.components.identity?.message || "Not assessed.")}</span></p><p><strong>Tool attribution · ${percent(attribution.percent)}</strong><span>${number(attribution.attributed)} attributed · ${number(attribution.unattributed)} unattributed terminal jobs.</span></p><p><strong>Revenue evidence</strong><span>${escapeHtml(data.coverage.revenue.message)}</span></p><p><strong>API reliability</strong><span>${number(data.api.failedRequests)} failed operations requests since ${relative(data.api.measuringSince)}.</span></p>`;
    const allHealthy = data.components.telemetry.status === "healthy" && data.api.status === "healthy";
    $("overallStatus").textContent = allHealthy ? "All sources healthy" : "Core data live · behavior partial";
    $("overallStatus").className = `status-chip ${allHealthy ? "good" : "warn"}`;
    updateStamp(data.asOf);
  } catch (error) { showError(error); }
}

$("refreshAll").addEventListener("click", load);
load();
