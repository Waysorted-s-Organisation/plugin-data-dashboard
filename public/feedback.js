import { $, dateTime, escapeHtml, json, metricCard, mountSidebar, number, setupMobileMenu, showError, updateStamp } from "./intelligence-shared.js";

mountSidebar("feedback");
setupMobileMenu();

const rank = (object) => Object.entries(object || {}).map(([key, value]) => `<p><strong>${escapeHtml(key)}</strong><span>${number(value)}</span></p>`).join("") || '<div class="empty-state">No data in this period.</div>';
const userLink = (userId) => userId ? ` · <a href="/users.html?user=${encodeURIComponent(userId)}">Open user</a>` : "";

async function load() {
  try {
    const data = await json(`/api/operations/feedback?days=${$("rangeDays").value}`);
    $("feedbackMetrics").innerHTML = metricCard("Feedback responses", number(data.summary.feedback), `Rated sample: ${number(data.summary.ratedResponses)}`)
      + metricCard("Average score", data.summary.averageScore === null ? "Unavailable" : `${data.summary.averageScore} / 5`, "10-point legacy ratings normalized to 5")
      + metricCard("Feature requests", number(data.summary.featureRequests), "Not deleted, created in range")
      + metricCard("Votes", number(data.summary.votes), "Demand signal across requests");
    $("feedbackTools").innerHTML = rank(data.feedbackByTool);
    $("requestStatus").innerHTML = rank(data.requestStatus);
    $("recentFeedback").innerHTML = data.recentFeedback.map((row) => `<div class="activity-item"><span class="activity-symbol">${row.rawScore === null ? "·" : escapeHtml(`${row.rawScore}/${row.scale}`)}</span><div><p><strong>${escapeHtml(row.tool || row.type || "General feedback")}</strong> · ${escapeHtml(row.source)}</p><small>${escapeHtml(row.comment || "No written comment")}${userLink(row.authorUserId)}</small></div><time>${dateTime(row.createdAt)}</time></div>`).join("") || '<div class="empty-state">No feedback in this period.</div>';
    $("topRequests").innerHTML = data.topRequests.map((row) => `<div class="activity-item"><span class="activity-symbol">${number(row.votes)}</span><div><p><strong>${escapeHtml(row.title)}</strong></p><small>${escapeHtml(row.board || "General")} · ${escapeHtml(row.status || "Unspecified")}${userLink(row.authorUserId)}</small></div><time>${dateTime(row.createdAt)}</time></div>`).join("") || '<div class="empty-state">No feature requests in this period.</div>';
    updateStamp(data.asOf);
  } catch (error) { showError(error); }
}

$("rangeDays").addEventListener("change", load);
$("refreshAll").addEventListener("click", load);
load();
