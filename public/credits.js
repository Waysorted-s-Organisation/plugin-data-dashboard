const usersBody = document.getElementById("usersBody");
const kpiGrid = document.getElementById("kpiGrid");
const statusLine = document.getElementById("statusLine");
const refreshBtn = document.getElementById("refreshBtn");
const exportBtn = document.getElementById("exportBtn");
const searchInput = document.getElementById("searchInput");
const toolBreakdownCanvas = document.getElementById("toolBreakdownChart");
const toolBreakdownEmpty = document.getElementById("toolBreakdownEmpty");

const CHART_COLORS = ["#5d9bff", "#78e6d8", "#ffb454", "#ff6b6b", "#2fd6a1", "#c084fc"];

let allUsers = [];
let toolsMap = new Map();
let sortColumn = null;
let sortDirection = "asc";
let searchQuery = "";
let toolBreakdownChart = null;

async function loadCredits() {
  statusLine.textContent = "Loading credits data...";
  statusLine.className = "status";

  try {
    const res = await fetch("/api/plugin-analytics/credit-consumption");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    const toolsRes = await fetch("/api/plugin-analytics/user-top-tools");
    const toolsData = toolsRes.ok ? await toolsRes.json() : { users: [] };

    toolsMap = new Map();
    for (const u of toolsData.users) {
      toolsMap.set(u._id, u.topTools || []);
    }

    allUsers = data.users;

    renderKPIs(allUsers);
    renderFilteredTable();
    fetchToolBreakdown();

    statusLine.textContent = `Loaded ${allUsers.length} users.`;
  } catch (error) {
    statusLine.textContent = `Error: ${error.message}`;
    statusLine.className = "status bad";
  }
}

function calculateBurnRate(users) {
  if (!users.length) return 0;

  let totalRate = 0;
  let usersWithData = 0;

  for (const u of users) {
    const consumed = u.totalConsumed || 0;
    if (consumed <= 0 || !u.lastSeen) continue;

    const lastSeen = new Date(u.lastSeen);
    const now = new Date();
    const daysSinceActive = Math.max(1, (now - lastSeen) / 86400000);
    // Estimate days of usage -- use a rough heuristic: totalConsumed spread
    // over the period from first activity to now. Since we don't have firstSeen,
    // we approximate using the ratio of consumed credits. Accounts with more
    // consumption likely started earlier. Use daysSinceActive as a lower bound
    // and scale by consumed amount relative to average.
    const daysEstimate = Math.max(daysSinceActive, 1);
    totalRate += consumed / daysEstimate;
    usersWithData++;
  }

  return usersWithData > 0 ? totalRate / usersWithData : 0;
}

function getDepletionDays(user) {
  const consumed = user.totalConsumed || 0;
  const remaining = user.creditsRemaining;
  if (remaining === null || remaining === undefined || remaining <= 0) return null;
  if (consumed <= 0) return null;

  const lastSeen = new Date(user.lastSeen);
  const now = new Date();
  const daysSinceActive = Math.max(1, (now - lastSeen) / 86400000);
  const dailyRate = consumed / Math.max(daysSinceActive, 1);

  if (dailyRate <= 0) return null;
  return Math.round(remaining / dailyRate);
}

function renderKPIs(users) {
  let totalCreditsAvailable = 0;
  let totalCreditsConsumed = 0;
  let usersWithLowCredits = 0;

  for (const u of users) {
    totalCreditsAvailable += (u.creditsRemaining || 0);
    totalCreditsConsumed += (u.totalConsumed || 0);
    if (u.creditsRemaining !== null && u.creditsRemaining <= 5) {
      usersWithLowCredits++;
    }
  }

  const avgBurnRate = calculateBurnRate(users);
  const burnRateDisplay = avgBurnRate > 0 ? avgBurnRate.toFixed(1) : "--";

  kpiGrid.innerHTML = `
    <div class="kpi-card">
      <p class="kpi-label">Users Tracked</p>
      <p class="kpi-value">${users.length}</p>
      <p class="kpi-sub">Total users with credits data</p>
    </div>
    <div class="kpi-card">
      <p class="kpi-label">Credits Consumed</p>
      <p class="kpi-value">${totalCreditsConsumed}</p>
      <p class="kpi-sub">Total credits spent across tools</p>
    </div>
    <div class="kpi-card">
      <p class="kpi-label">Available Credits</p>
      <p class="kpi-value">${totalCreditsAvailable}</p>
      <p class="kpi-sub">Total active credits in ecosystem</p>
    </div>
    <div class="kpi-card ${usersWithLowCredits > 0 ? 'kpi-warn' : 'kpi-good'}">
      <p class="kpi-label">Low Credit Users</p>
      <p class="kpi-value">${usersWithLowCredits}</p>
      <p class="kpi-sub">Users with &le; 5 credits</p>
    </div>
    <div class="kpi-card">
      <p class="kpi-label">Avg Burn Rate</p>
      <p class="kpi-value">${burnRateDisplay}</p>
      <p class="kpi-sub">Credits consumed per user per day</p>
    </div>
  `;
}

async function fetchToolBreakdown() {
  try {
    const res = await fetch("/api/plugin-analytics/credit-consumption/by-tool");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    if (!data.tools || data.tools.length === 0) {
      showToolBreakdownEmpty();
      return;
    }

    renderToolBreakdownChart(data.tools);
  } catch {
    showToolBreakdownEmpty();
  }
}

function showToolBreakdownEmpty() {
  toolBreakdownEmpty.style.display = "block";
}

function renderToolBreakdownChart(tools) {
  toolBreakdownEmpty.style.display = "none";

  const labels = tools.map(t => t.tool);
  const values = tools.map(t => t.totalAmount);
  const colors = labels.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);

  if (toolBreakdownChart) {
    toolBreakdownChart.destroy();
  }

  toolBreakdownChart = new Chart(toolBreakdownCanvas, {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: colors,
        borderColor: "rgba(6, 11, 23, 0.8)",
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "right",
          labels: {
            color: "#9eadd6",
            font: { family: "Space Grotesk", size: 12 },
            padding: 14,
          },
        },
        tooltip: {
          callbacks: {
            label(ctx) {
              const tool = tools[ctx.dataIndex];
              return `${ctx.label}: ${tool.totalAmount} credits (${tool.count} uses)`;
            },
          },
        },
      },
      cutout: "55%",
    },
  });
}

function getFilteredAndSortedUsers() {
  let filtered = allUsers;

  if (searchQuery) {
    const q = searchQuery.toLowerCase();
    filtered = filtered.filter(u => {
      const name = (u.name || "").toLowerCase();
      const email = (u.email || u._id || "").toLowerCase();
      return name.includes(q) || email.includes(q);
    });
  }

  if (sortColumn) {
    filtered = [...filtered].sort((a, b) => {
      let valA, valB;

      if (sortColumn === "creditsRemaining") {
        valA = a.creditsRemaining ?? -1;
        valB = b.creditsRemaining ?? -1;
      } else if (sortColumn === "totalConsumed") {
        valA = a.totalConsumed || 0;
        valB = b.totalConsumed || 0;
      } else if (sortColumn === "lastSeen") {
        valA = a.lastSeen ? new Date(a.lastSeen).getTime() : 0;
        valB = b.lastSeen ? new Date(b.lastSeen).getTime() : 0;
      } else if (sortColumn === "depletion") {
        valA = getDepletionDays(a) ?? Infinity;
        valB = getDepletionDays(b) ?? Infinity;
      } else {
        return 0;
      }

      if (valA < valB) return sortDirection === "asc" ? -1 : 1;
      if (valA > valB) return sortDirection === "asc" ? 1 : -1;
      return 0;
    });
  }

  return filtered;
}

function renderFilteredTable() {
  const users = getFilteredAndSortedUsers();
  renderTable(users, toolsMap);
  updateSortIndicators();
}

function renderTable(users, tMap) {
  usersBody.innerHTML = "";

  if (users.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6" class="muted" style="text-align:center; padding:24px;">No users found.</td>`;
    usersBody.appendChild(tr);
    return;
  }

  for (const u of users) {
    const topTools = tMap.get(u._id) || [];

    let toolsHtml = '<span class="muted">None</span>';
    if (topTools.length > 0) {
      toolsHtml = topTools.map(t => {
        const mins = (t.timeSpentMs / 60000).toFixed(1);
        return `<span class="pill">${t.tool} (${mins}m)</span>`;
      }).join(" ");
    }

    let creditsClass = "pill-signal-high";
    if (u.creditsRemaining <= 5) creditsClass = "pill-signal-low";
    if (u.creditsRemaining <= 20 && u.creditsRemaining > 5) creditsClass = "pill-signal-medium";

    const depletionDays = getDepletionDays(u);
    let depletionHtml = '<span class="muted">--</span>';
    if (depletionDays !== null) {
      let depClass = "depletion-safe";
      if (depletionDays <= 7) depClass = "depletion-danger";
      else if (depletionDays <= 30) depClass = "";
      depletionHtml = `<span class="depletion-estimate ${depClass}">~${depletionDays} days</span>`;
    }

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        <div class="event-title">${u.name || "Anonymous User"}</div>
        <div class="event-detail">${u.email || u._id}</div>
      </td>
      <td>
        ${u.creditsRemaining !== null ? `<span class="pill ${creditsClass}">${u.creditsRemaining} credits</span>` : '<span class="muted">Unknown</span>'}
      </td>
      <td>${u.totalConsumed || 0}</td>
      <td>${toolsHtml}</td>
      <td class="muted">${u.lastSeen ? new Date(u.lastSeen).toLocaleString() : "--"}</td>
      <td>${depletionHtml}</td>
    `;
    usersBody.appendChild(tr);
  }
}

function updateSortIndicators() {
  const headers = document.querySelectorAll("th.sortable");
  for (const th of headers) {
    const indicator = th.querySelector(".sort-indicator");
    const col = th.dataset.sort;
    if (col === sortColumn) {
      indicator.textContent = sortDirection === "asc" ? " ▲" : " ▼";
    } else {
      indicator.textContent = "";
    }
  }
}

function sortTable(column) {
  if (sortColumn === column) {
    sortDirection = sortDirection === "asc" ? "desc" : "asc";
  } else {
    sortColumn = column;
    sortDirection = "asc";
  }
  renderFilteredTable();
}

function filterTable(query) {
  searchQuery = query;
  renderFilteredTable();
}

function exportCSV() {
  const users = getFilteredAndSortedUsers();
  if (users.length === 0) return;

  const headers = ["Name", "Email", "Available Credits", "Credits Consumed", "Top Tools", "Last Active", "Projected Depletion"];
  const rows = users.map(u => {
    const topTools = toolsMap.get(u._id) || [];
    const toolStr = topTools.map(t => `${t.tool} (${(t.timeSpentMs / 60000).toFixed(1)}m)`).join("; ");
    const depletion = getDepletionDays(u);
    return [
      u.name || "Anonymous User",
      u.email || u._id,
      u.creditsRemaining !== null ? u.creditsRemaining : "",
      u.totalConsumed || 0,
      toolStr || "None",
      u.lastSeen ? new Date(u.lastSeen).toISOString() : "",
      depletion !== null ? `~${depletion} days` : "--",
    ];
  });

  let csv = headers.join(",") + "\n";
  for (const row of rows) {
    csv += row.map(v => `"${String(v).replace(/"/g, '""')}"`).join(",") + "\n";
  }

  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `credit-consumption-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// -- Event listeners --

document.querySelectorAll("th.sortable").forEach(th => {
  th.addEventListener("click", () => sortTable(th.dataset.sort));
});

searchInput.addEventListener("input", (e) => filterTable(e.target.value));
exportBtn.addEventListener("click", exportCSV);
refreshBtn.addEventListener("click", loadCredits);
loadCredits();
