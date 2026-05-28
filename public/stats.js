const statsGrid = document.getElementById("statsGrid");
const statusLine = document.getElementById("statusLine");
const actionBtns = document.querySelectorAll(".action-btn");

async function loadStats() {
  try {
    const res = await fetch("/api/plugin-analytics/stats");
    if (!res.ok) throw new Error("Failed to load");
    const data = await res.json();
    
    statsGrid.innerHTML = \`
      <div class="stat-card">
        <div class="value">\${data.mau || 0}</div>
        <div class="label">Monthly Active Users</div>
      </div>
      <div class="stat-card">
        <div class="value">\${data.installs || 0}</div>
        <div class="label">Total Installs</div>
      </div>
      <div class="stat-card">
        <div class="value">\${data.likes || 0}</div>
        <div class="label">Likes</div>
      </div>
      <div class="stat-card">
        <div class="value">\${data.saves || 0}</div>
        <div class="label">Saves</div>
      </div>
      <div class="stat-card">
        <div class="value">\${data.follows || 0}</div>
        <div class="label">Followers</div>
      </div>
      <div class="stat-card">
        <div class="value">\${data.reused || 0}</div>
        <div class="label">Reuses</div>
      </div>
    \`;
    statusLine.textContent = "Live updating...";
    statusLine.style.display = "none";
  } catch (err) {
    statusLine.textContent = "Error loading stats.";
    statusLine.className = "status bad";
  }
}

async function performAction(action) {
  try {
    const res = await fetch(\`/api/plugin-analytics/stats/\${action}\`, {
      method: "POST"
    });
    if (res.ok) {
      loadStats(); // Reload to show updated numbers
    }
  } catch (err) {
    console.error("Action failed", err);
  }
}

actionBtns.forEach(btn => {
  btn.addEventListener("click", () => {
    const action = btn.getAttribute("data-action");
    performAction(action);
  });
});

loadStats();
