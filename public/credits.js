const usersBody = document.getElementById("usersBody");
const kpiGrid = document.getElementById("kpiGrid");
const statusLine = document.getElementById("statusLine");
const refreshBtn = document.getElementById("refreshBtn");

async function loadCredits() {
  statusLine.textContent = "Loading credits data...";
  statusLine.className = "status";
  
  try {
    const res = await fetch("/api/plugin-analytics/credit-consumption");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    
    // Get top tools separately and merge
    const toolsRes = await fetch("/api/plugin-analytics/user-top-tools");
    const toolsData = toolsRes.ok ? await toolsRes.json() : { users: [] };
    
    const toolsMap = new Map();
    for (const u of toolsData.users) {
      toolsMap.set(u._id, u.topTools || []);
    }
    
    renderKPIs(data.users);
    renderTable(data.users, toolsMap);
    
    statusLine.textContent = `Loaded ${data.users.length} users.`;
  } catch (error) {
    statusLine.textContent = `Error: ${error.message}`;
    statusLine.className = "status bad";
  }
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
  `;
}

function renderTable(users, toolsMap) {
  usersBody.innerHTML = "";
  
  for (const u of users) {
    const topTools = toolsMap.get(u._id) || [];
    
    let toolsHtml = '<span class="muted">None</span>';
    if (topTools.length > 0) {
      toolsHtml = topTools.map(t => {
        const mins = (t.timeSpentMs / 60000).toFixed(1);
        return \`<span class="pill">\${t.tool} (\${mins}m)</span>\`;
      }).join(" ");
    }
    
    let creditsClass = "pill-signal-high";
    if (u.creditsRemaining <= 5) creditsClass = "pill-signal-low";
    if (u.creditsRemaining <= 20 && u.creditsRemaining > 5) creditsClass = "pill-signal-medium";
    
    const tr = document.createElement("tr");
    tr.innerHTML = \`
      <td>
        <div class="event-title">\${u.name || "Anonymous User"}</div>
        <div class="event-detail">\${u.email || u._id}</div>
      </td>
      <td>
        \${u.creditsRemaining !== null ? \`<span class="pill \${creditsClass}">\${u.creditsRemaining} credits</span>\` : '<span class="muted">Unknown</span>'}
      </td>
      <td>\${u.totalConsumed || 0}</td>
      <td>\${toolsHtml}</td>
      <td class="muted">\${new Date(u.lastSeen).toLocaleString()}</td>
    \`;
    usersBody.appendChild(tr);
  }
}

refreshBtn.addEventListener("click", loadCredits);
loadCredits();
