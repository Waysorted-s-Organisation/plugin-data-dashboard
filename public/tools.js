import { $, duration, escapeHtml, json, metricCard, mountSidebar, number, percent, setupMobileMenu, showError, updateStamp } from "./intelligence-shared.js";

mountSidebar("tools");
setupMobileMenu();

async function load() {
  try {
    const data = await json(`/api/operations/tools?days=${$("rangeDays").value}`);
    $("toolMetrics").innerHTML = metricCard("Credited jobs", number(data.summary.completedJobs), "Billable work only — credit-free tools are counted under activity")
      + metricCard("Credits consumed", number(data.summary.creditsConsumed), "Across completed jobs")
      + metricCard("Expired jobs", number(data.summary.expiredJobs), "Processing did not finish")
      + metricCard("Activity tracked", number(data.summary.telemetryOnlyTools), "Tools measured from plugin activity")
      + metricCard("Tracking gaps", number(data.summary.unavailableTools), "No credit or plugin activity recorded");
    // Three coverage states, not two. Tools without a credit system report
    // coverage "telemetry" and carry real activity counts; rendering them
    // through the unavailable template hid exactly the data they provide.
    $("toolGrid").innerHTML = data.items.map((row) => {
      if (row.coverage === "measured") {
        return `<button class="tool-score-card" data-tool="${row.key}"><div class="panel-head"><div><p class="overline">Measured</p><h3>${escapeHtml(row.label)}</h3></div><span class="status-chip ${row.completionRate >= 80 ? "good" : row.completionRate >= 50 ? "warn" : "bad"}">${percent(row.completionRate)} complete</span></div><div class="tool-stat-row"><span><strong>${number(row.uniqueUsers)}</strong> users</span><span><strong>${number(row.completedJobs)}</strong> completed</span><span><strong>${number(row.expiredJobs)}</strong> expired</span></div><p>${row.completedJobsChange > 0 ? "+" : ""}${row.completedJobsChange}% completed jobs vs previous period</p></button>`;
      }
      if (row.coverage === "telemetry") {
        const activity = row.telemetry || {};
        return `<article class="tool-score-card"><div class="panel-head"><div><p class="overline">Activity tracked</p><h3>${escapeHtml(row.label)}</h3></div><span class="status-chip good">No credits</span></div><div class="tool-stat-row"><span><strong>${number(row.uniqueUsers)}</strong> users</span><span><strong>${number(activity.opens)}</strong> opens</span><span><strong>${number(activity.actionsCompleted)}</strong> completed</span></div><p>${number(activity.actionsFailed)} failed · ${number(activity.errors)} errors shown · ${duration(activity.activeMs)} active</p><p class="coverage-note">${escapeHtml(row.message)}</p></article>`;
      }
      return `<article class="tool-score-card unavailable"><div class="panel-head"><div><p class="overline">${escapeHtml(row.catalogStatus || "Catalog")}</p><h3>${escapeHtml(row.label)}</h3></div><span class="status-chip warn">Unavailable</span></div><p>${escapeHtml(row.message)}</p><p><strong>${number(row.favorites)}</strong> users saved this tool as a favorite.</p></article>`;
    }).join("");
    document.querySelectorAll("[data-tool]").forEach((button) => button.addEventListener("click", () => openTool(button.dataset.tool)));
    updateStamp(data.asOf);
  } catch (error) { showError(error); }
}

async function openTool(key) {
  try {
    const row = await json(`/api/operations/tools/${encodeURIComponent(key)}?days=${$("rangeDays").value}`);
    $("toolTitle").textContent = row.label;
    $("toolBody").innerHTML = `<div class="detail-grid"><div><span>Unique users</span><strong>${number(row.uniqueUsers)}</strong></div><div><span>Completed jobs</span><strong>${number(row.completedJobs)}</strong></div><div><span>Completion rate</span><strong>${percent(row.completionRate)}</strong></div><div><span>Average completion</span><strong>${duration(row.averageCompletionMs)}</strong></div><div><span>Released</span><strong>${number(row.releasedJobs)}</strong></div><div><span>Expired</span><strong>${number(row.expiredJobs)}</strong></div><div><span>Compensated</span><strong>${number(row.compensatedJobs)}</strong></div><div><span>Still processing</span><strong>${number(row.processingJobs)}</strong></div><div><span>Repeat users</span><strong>${number(row.repeatUsers)}</strong></div><div><span>New users</span><strong>${number(row.newUsers)}</strong></div><div><span>Existing users</span><strong>${number(row.existingUsers)}</strong></div><div><span>Credits consumed</span><strong>${number(row.creditsConsumed)}</strong></div></div><h3>Features and formats</h3><div class="rank-list">${row.features.map((item) => `<p><strong>${escapeHtml(item.feature)}</strong><span>${number(item.count)} jobs</span></p>`).join("")}</div><div class="notice coverage-notice"><strong>How calculated:</strong> completion rate excludes work still processing and keeps completed, released, expired, and compensated work separate.</div>`;
    $("toolDialog").showModal();
  } catch (error) { showError(error); }
}

$("closeTool").addEventListener("click", () => $("toolDialog").close());
$("rangeDays").addEventListener("change", load);
$("refreshAll").addEventListener("click", load);
load();
