const summaryGrid = document.getElementById("summaryGrid");
const creditSourceStrip = document.getElementById("creditSourceStrip");
const usersBody = document.getElementById("usersBody");
const statusLine = document.getElementById("statusLine");
const refreshBtn = document.getElementById("refreshBtn");
const exportBtn = document.getElementById("exportBtn");
const sendNewsletterBtn = document.getElementById("sendNewsletterBtn");
const searchInput = document.getElementById("searchInput");
const toolBreakdownCanvas = document.getElementById("toolBreakdownChart");
const toolBreakdownEmpty = document.getElementById("toolBreakdownEmpty");
const runoutList = document.getElementById("runoutList");
const powerUsersList = document.getElementById("powerUsersList");
const newsletterMeta = document.getElementById("newsletterMeta");
const newsletterPreview = document.getElementById("newsletterPreview");
const newsletterState = document.getElementById("newsletterState");

const CHART_COLORS = [
  "#5d9bff",
  "#78e6d8",
  "#ffb454",
  "#ff6b6b",
  "#6ee7b7",
  "#f472b6",
  "#facc15",
  "#60a5fa",
];

const RISK_ORDER = {
  critical: 0,
  warning: 1,
  healthy: 2,
};

const TOOL_LABELS = {
  dashboard: "Dashboard",
  "collapsed-dashboard": "Collapsed Dashboard",
  palettable: "Palette Tool",
  "frame-gallery": "Frames to PDF",
  "import-tool": "Import Tool",
  "unit-converter": "Unit Converter",
  "html-to-design": "HTML to Design",
  "comment-summarizer": "Comment Summarizer",
  profile: "Profile",
  "wayfall-game": "Wayfall Game",
  "liquid-glass": "Liquid Glass",
  ai: "AI Import",
  eps: "EPS Import",
  pdf: "PDF Import",
  psd: "PSD Import",
  unknown: "Unknown",
};

let creditData = null;
let toolBreakdownChart = null;
let searchQuery = "";
let sortColumn = "risk";
let sortDirection = "asc";

if (window.Chart) {
  window.Chart.defaults.color = "#a9b7e5";
  window.Chart.defaults.borderColor = "rgba(159, 183, 255, 0.18)";
}

function resolveApiPath(path) {
  return new URL(path, `${window.location.protocol}//${window.location.host}`).toString();
}

function escapeHtml(value) {
  const text = String(value === null || value === undefined ? "" : value);
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function numberLabel(value, digits = 0) {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(Number(value || 0));
}

function durationMinutes(ms) {
  return numberLabel(Number(ms || 0) / 60000, 1);
}

function dateLabel(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleString();
}

function riskPillClass(level) {
  if (level === "critical") return "pill-signal-low";
  if (level === "warning") return "pill-signal-medium";
  return "pill-signal-high";
}

function riskLabel(level) {
  if (level === "critical") return "Critical";
  if (level === "warning") return "Watch";
  return "Healthy";
}

function toolLabel(toolId) {
  return TOOL_LABELS[String(toolId || "").trim().toLowerCase()] || String(toolId || "Unknown");
}

function setStatus(message, isError = false) {
  statusLine.textContent = message;
  statusLine.classList.toggle("bad", Boolean(isError));
}

function getIdentity(user) {
  return user.name || user.email || user._id || "Unknown user";
}

function renderSourceStrip() {
  creditSourceStrip.innerHTML = `
    <article class="source-chip">
      <span class="pill pill-signal-high">Balances</span>
      <span>Real-time from backend <span class="mono">users</span> and <span class="mono">userbillings</span>.</span>
    </article>
    <article class="source-chip">
      <span class="pill pill-signal-medium">Tool Spend</span>
      <span>Derived from backend <span class="mono">creditledgers</span> reservation activity.</span>
    </article>
    <article class="source-chip">
      <span class="pill pill-muted">Tool Time</span>
      <span>Derived from plugin analytics <span class="mono">tool_time_spent</span> events.</span>
    </article>
  `;
}

function getFilteredUsers() {
  const users = Array.isArray(creditData && creditData.users) ? creditData.users : [];
  let filtered = users;

  if (searchQuery) {
    const query = searchQuery.toLowerCase();
    filtered = filtered.filter((user) => {
      const haystack = [
        user.name || "",
        user.email || "",
        user._id || "",
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(query);
    });
  }

  return [...filtered].sort((a, b) => {
    let aValue;
    let bValue;

    switch (sortColumn) {
      case "identity":
        aValue = getIdentity(a);
        bValue = getIdentity(b);
        break;
      case "risk":
        aValue = RISK_ORDER[a.riskLevel] ?? 99;
        bValue = RISK_ORDER[b.riskLevel] ?? 99;
        break;
      case "creditsRemaining":
        aValue = Number(a.creditsRemaining ?? -1);
        bValue = Number(b.creditsRemaining ?? -1);
        break;
      case "estimatedDailyBurn":
        aValue = Number(a.estimatedDailyBurn || 0);
        bValue = Number(b.estimatedDailyBurn || 0);
        break;
      case "depletionDays":
        aValue = Number.isFinite(Number(a.depletionDays)) ? Number(a.depletionDays) : Number.MAX_SAFE_INTEGER;
        bValue = Number.isFinite(Number(b.depletionDays)) ? Number(b.depletionDays) : Number.MAX_SAFE_INTEGER;
        break;
      case "totalConsumed":
        aValue = Number(a.totalConsumed || 0);
        bValue = Number(b.totalConsumed || 0);
        break;
      case "lastSeen":
        aValue = a.lastSeen ? new Date(a.lastSeen).getTime() : 0;
        bValue = b.lastSeen ? new Date(b.lastSeen).getTime() : 0;
        break;
      default:
        aValue = 0;
        bValue = 0;
    }

    if (aValue < bValue) return sortDirection === "asc" ? -1 : 1;
    if (aValue > bValue) return sortDirection === "asc" ? 1 : -1;
    return 0;
  });
}

function renderSummary(summary) {
  const cards = [
    {
      label: "Tracked Users",
      value: numberLabel(summary.trackedUsers),
      sub: `${numberLabel(summary.authenticatedUsers)} authenticated`,
    },
    {
      label: "Runout Soon",
      value: numberLabel(summary.runoutSoonUsers),
      sub: `<= ${numberLabel(summary.lowCreditUsers)} already low`,
      tone: summary.runoutSoonUsers > 0 ? "warn" : "good",
    },
    {
      label: "Credits Consumed",
      value: numberLabel(summary.creditsConsumedTotal, 0),
      sub: "Lifetime measured spend",
    },
    {
      label: "Credits Left",
      value: numberLabel(summary.creditsRemainingTotal, 0),
      sub: "Latest known balances",
    },
    {
      label: "Avg Burn / Day",
      value: numberLabel(summary.averageDailyBurn, 2),
      sub: "Estimated across tracked users",
    },
  ];

  summaryGrid.innerHTML = cards
    .map(
      (card) => `
        <article class="kpi-card ${card.tone ? `kpi-${card.tone}` : ""}">
          <p class="kpi-label">${escapeHtml(card.label)}</p>
          <p class="kpi-value">${escapeHtml(card.value)}</p>
          <p class="kpi-sub">${escapeHtml(card.sub)}</p>
        </article>
      `
    )
    .join("");
}

function renderToolBreakdown(tools) {
  if (!Array.isArray(tools) || !tools.length) {
    toolBreakdownEmpty.hidden = false;
    toolBreakdownCanvas.parentElement.hidden = true;
    if (toolBreakdownChart) {
      toolBreakdownChart.destroy();
      toolBreakdownChart = null;
    }
    return;
  }

  toolBreakdownEmpty.hidden = true;
  toolBreakdownCanvas.parentElement.hidden = false;
  if (toolBreakdownChart) {
    toolBreakdownChart.destroy();
  }

  toolBreakdownChart = new Chart(toolBreakdownCanvas, {
    type: "doughnut",
    data: {
      labels: tools.map((tool) => toolLabel(tool.tool)),
      datasets: [
        {
          data: tools.map((tool) => tool.totalAmount),
          backgroundColor: tools.map((_, index) => CHART_COLORS[index % CHART_COLORS.length]),
          borderColor: "rgba(6, 11, 23, 0.82)",
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "62%",
      plugins: {
        legend: {
          position: "right",
          labels: {
            boxWidth: 10,
            boxHeight: 10,
            padding: 14,
            font: {
              family: "Space Grotesk",
              size: 12,
            },
          },
        },
        tooltip: {
          callbacks: {
            label(context) {
              const row = tools[context.dataIndex];
              return `${toolLabel(row.tool)}: ${numberLabel(row.totalAmount, 2)} credits across ${row.count} ledger entries`;
            },
          },
        },
      },
    },
  });
}

function renderStackList(target, items, emptyText, formatter) {
  if (!items.length) {
    target.innerHTML = `<p class="muted">${escapeHtml(emptyText)}</p>`;
    return;
  }

  target.innerHTML = items
    .map((item) => formatter(item))
    .join("");
}

function renderRunoutCandidates(users) {
  renderStackList(
    runoutList,
    users.slice(0, 8),
    "No runout-risk users yet.",
    (user) => {
      const topCreditTool = user.topCreditTools && user.topCreditTools[0] ? user.topCreditTools[0] : null;
      const topTimeTool = user.topTools && user.topTools[0] ? user.topTools[0] : null;
      return `
        <article class="stack-item">
          <div class="stack-item-main">
            <div class="event-title">${escapeHtml(getIdentity(user))}</div>
            <div class="event-detail">${escapeHtml(user.email || user._id || "")}</div>
          </div>
          <div class="stack-item-meta">
            <span class="pill ${riskPillClass(user.riskLevel)}">${escapeHtml(riskLabel(user.riskLevel))}</span>
            <span class="mono">${escapeHtml(numberLabel(user.creditsRemaining || 0, 0))} credits</span>
            <span class="muted">${escapeHtml(user.depletionDays ? `${user.depletionDays} days left` : "No runway yet")}</span>
            <span class="muted">Top spend: ${escapeHtml(topCreditTool ? `${toolLabel(topCreditTool.tool)} (${numberLabel(topCreditTool.totalAmount, 0)} cr)` : "Not attributed yet")}</span>
            <span class="muted">Top time: ${escapeHtml(topTimeTool ? toolLabel(topTimeTool.tool) : "No timed workflow yet")}</span>
          </div>
        </article>
      `;
    }
  );
}

function renderPowerUsers(users) {
  renderStackList(
    powerUsersList,
    users.slice(0, 8),
    "No tool-time data yet.",
    (user) => {
      const topTool = user.topTools && user.topTools[0] ? toolLabel(user.topTools[0].tool) : "Unknown";
      return `
        <article class="stack-item">
          <div class="stack-item-main">
            <div class="event-title">${escapeHtml(getIdentity(user))}</div>
            <div class="event-detail">${escapeHtml(user.email || user._id || "")}</div>
          </div>
          <div class="stack-item-meta">
            <span class="pill pill-muted">${escapeHtml(durationMinutes(user.totalTimeSpentMs))} min</span>
            <span class="muted">Top tool: ${escapeHtml(topTool)}</span>
          </div>
        </article>
      `;
    }
  );
}

function renderNewsletter(newsletter) {
  newsletterState.textContent = newsletter.canSend ? "Ready to send" : "Preview only";
  newsletterState.className = `pill ${newsletter.canSend ? "pill-signal-high" : "pill-muted"}`;
  newsletterMeta.innerHTML = `
    <article class="bucket-card">
      <p class="bucket-label">Recipients</p>
      <p class="bucket-value">${numberLabel((newsletter.recipients || []).length)}</p>
      <p class="bucket-meta">${escapeHtml((newsletter.recipients || []).join(", ") || "Not configured")}</p>
    </article>
    <article class="bucket-card">
      <p class="bucket-label">Runout Users</p>
      <p class="bucket-value">${numberLabel((newsletter.runoutCandidates || []).length)}</p>
      <p class="bucket-meta">Included in the digest</p>
    </article>
    <article class="bucket-card">
      <p class="bucket-label">Power Users</p>
      <p class="bucket-value">${numberLabel((newsletter.powerUsers || []).length)}</p>
      <p class="bucket-meta">Sorted by tool time</p>
    </article>
  `;
  newsletterPreview.textContent = `${newsletter.subject}\n\n${newsletter.text}`;
  sendNewsletterBtn.disabled = !newsletter.canSend;
}

function renderUsersTable(users) {
  if (!users.length) {
    usersBody.innerHTML = `<tr><td colspan="8" class="muted">No users found.</td></tr>`;
    return;
  }

  usersBody.innerHTML = users
    .map((user) => {
      const timePills = Array.isArray(user.topTools) && user.topTools.length
        ? user.topTools
            .map((tool) => `<span class="pill pill-muted">${escapeHtml(toolLabel(tool.tool))} ${escapeHtml(durationMinutes(tool.timeSpentMs))}m</span>`)
            .join(" ")
        : "";
      const creditPills = Array.isArray(user.topCreditTools) && user.topCreditTools.length
        ? user.topCreditTools
            .map((tool) => `<span class="pill">${escapeHtml(toolLabel(tool.tool))} ${escapeHtml(numberLabel(tool.totalAmount, 0))}cr</span>`)
            .join(" ")
        : "";
      const toolPills = [timePills, creditPills].filter(Boolean).join(" ") || `<span class="muted">No workflow signal yet</span>`;

      return `
        <tr>
          <td>
            <div class="event-title">${escapeHtml(getIdentity(user))}</div>
            <div class="event-detail">${escapeHtml(user.email || user._id || "")}</div>
          </td>
          <td><span class="pill ${riskPillClass(user.riskLevel)}">${escapeHtml(riskLabel(user.riskLevel))}</span></td>
          <td>${user.creditsRemaining === null ? `<span class="muted">Unknown</span>` : escapeHtml(numberLabel(user.creditsRemaining, 0))}</td>
          <td>${escapeHtml(numberLabel(user.estimatedDailyBurn, 2))}</td>
          <td>${user.depletionDays ? escapeHtml(`${user.depletionDays} days`) : `<span class="muted">--</span>`}</td>
          <td>${escapeHtml(numberLabel(user.totalConsumed, 2))}</td>
          <td>${toolPills}</td>
          <td class="muted">${escapeHtml(dateLabel(user.lastSeen))}</td>
        </tr>
      `;
    })
    .join("");
}

function render() {
  if (!creditData) return;
  renderSourceStrip();
  renderSummary(creditData.summary || {});
  renderToolBreakdown(creditData.toolBreakdown || []);
  renderRunoutCandidates(creditData.newsletter && creditData.newsletter.runoutCandidates ? creditData.newsletter.runoutCandidates : []);
  renderPowerUsers(creditData.topPowerUsers || []);
  renderNewsletter(creditData.newsletter || { recipients: [], canSend: false, subject: "No preview", text: "" });
  renderUsersTable(getFilteredUsers());
}

async function loadCreditIntelligence() {
  setStatus("Loading credit intelligence…");

  try {
    const response = await fetch(resolveApiPath("/api/plugin-analytics/credit-intelligence"));
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    creditData = await response.json();
    render();
    setStatus(`Loaded ${numberLabel((creditData.users || []).length)} tracked users.`);
  } catch (error) {
    console.error(error);
    setStatus(`Failed to load credit intelligence: ${error.message}`, true);
  }
}

function exportCsv() {
  const users = getFilteredUsers();
  const rows = [
    [
      "User",
      "Email",
      "Risk",
      "Credits Left",
      "Burn Per Day",
      "Runway Days",
      "Lifetime Spend",
      "Top Tools",
      "Last Seen",
    ],
    ...users.map((user) => [
      getIdentity(user),
      user.email || user._id || "",
      riskLabel(user.riskLevel),
      user.creditsRemaining ?? "",
      user.estimatedDailyBurn ?? 0,
      user.depletionDays ?? "",
      user.totalConsumed ?? 0,
      [
        ...(user.topTools || []).map((tool) => `${toolLabel(tool.tool)} (${durationMinutes(tool.timeSpentMs)}m)`),
        ...(user.topCreditTools || []).map((tool) => `${toolLabel(tool.tool)} (${numberLabel(tool.totalAmount, 0)}cr)`),
      ].join("; "),
      user.lastSeen || "",
    ]),
  ];

  const csv = rows
    .map((row) => row.map((value) => `"${String(value).replace(/"/g, '""')}"`).join(","))
    .join("\n");

  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `credit-intelligence-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

async function sendNewsletter() {
  setStatus("Sending credit digest…");

  try {
    const response = await fetch(resolveApiPath("/api/plugin-analytics/newsletter/runout-send"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    });
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.error || `HTTP ${response.status}`);
    }

    if (!payload.sent) {
      setStatus(`Digest not sent: ${payload.reason}`, true);
      return;
    }

    setStatus(`Digest sent to ${(payload.recipients || []).join(", ")}.`);
  } catch (error) {
    console.error(error);
    setStatus(`Failed to send digest: ${error.message}`, true);
  }
}

function toggleSort(column) {
  if (sortColumn === column) {
    sortDirection = sortDirection === "asc" ? "desc" : "asc";
  } else {
    sortColumn = column;
    sortDirection = column === "risk" ? "asc" : "desc";
  }
  renderUsersTable(getFilteredUsers());
}

document.querySelectorAll("th.sortable").forEach((header) => {
  header.addEventListener("click", () => toggleSort(header.dataset.sort));
});

searchInput.addEventListener("input", (event) => {
  searchQuery = event.target.value.trim();
  renderUsersTable(getFilteredUsers());
});

refreshBtn.addEventListener("click", loadCreditIntelligence);
exportBtn.addEventListener("click", exportCsv);
sendNewsletterBtn.addEventListener("click", sendNewsletter);

loadCreditIntelligence();
