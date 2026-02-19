const statusLine = document.getElementById("statusLine");
const kpiGrid = document.getElementById("kpiGrid");
const actionsBody = document.getElementById("actionsBody");
const sessionsBody = document.getElementById("sessionsBody");
const eventsBody = document.getElementById("eventsBody");
const heatmapCanvas = document.getElementById("heatmapCanvas");
const heatmapCount = document.getElementById("heatmapCount");

const rangePresetEl = document.getElementById("rangePreset");
const fromDateEl = document.getElementById("fromDate");
const toDateEl = document.getElementById("toDate");
const toolFilterEl = document.getElementById("toolFilter");
const authFilterEl = document.getElementById("authFilter");
const refreshBtn = document.getElementById("refreshBtn");

const state = {
  from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
  to: new Date(),
  tool: "all",
  auth: "all",
};

let toolChart = null;
let dailyChart = null;
let loadDebounceTimer = null;
let activeLoadController = null;
let currentLoadToken = 0;

function setStatus(text, isError = false) {
  statusLine.textContent = text;
  statusLine.classList.toggle("bad", Boolean(isError));
}

function toLocalInputValue(date) {
  const pad = (n) => String(n).padStart(2, "0");
  const d = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}`;
}

function parseLocalInputValue(value, fallback) {
  if (!value) return fallback;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? fallback : d;
}

function durationLabel(ms) {
  const seconds = Math.floor(Number(ms || 0) / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remSeconds = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remSeconds}s`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `${hours}h ${remMinutes}m`;
}

function numberLabel(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function getQueryParams() {
  const params = new URLSearchParams({
    from: state.from.toISOString(),
    to: state.to.toISOString(),
  });

  if (state.tool && state.tool !== "all") {
    params.set("tool", state.tool);
  }

  if (state.auth && state.auth !== "all") {
    params.set("auth", state.auth);
  }

  return params;
}

async function fetchJson(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText} - ${text}`);
  }
  return response.json();
}

function renderKpis(kpis) {
  const cards = [
    { label: "Total Events", value: numberLabel(kpis.totalEvents) },
    { label: "Sessions", value: numberLabel(kpis.totalSessions) },
    { label: "Auth Users", value: numberLabel(kpis.authenticatedUsers) },
    { label: "Anonymous Events", value: numberLabel(kpis.anonymousEvents) },
    { label: "Avg Session", value: durationLabel(kpis.avgSessionDurationMs) },
    { label: "Max Session", value: durationLabel(kpis.maxSessionDurationMs) },
  ];

  kpiGrid.innerHTML = cards
    .map(
      (card) => `
      <article class="kpi-card">
        <p class="kpi-label">${card.label}</p>
        <p class="kpi-value">${card.value}</p>
      </article>
    `
    )
    .join("");
}

function renderTopActions(actions) {
  if (!actions || !actions.length) {
    actionsBody.innerHTML = `<tr><td colspan="2">No action events in this range.</td></tr>`;
    return;
  }

  actionsBody.innerHTML = actions
    .map(
      (item) => `
      <tr>
        <td>${item.action || "(unknown)"}</td>
        <td>${numberLabel(item.count)}</td>
      </tr>
    `
    )
    .join("");
}

function renderSessions(sessions) {
  if (!sessions || !sessions.length) {
    sessionsBody.innerHTML = `<tr><td colspan="6">No sessions in this range.</td></tr>`;
    return;
  }

  sessionsBody.innerHTML = sessions
    .map((session) => {
      const user = session.user && session.user.isAuthenticated
        ? `${session.user.name || "User"}${session.user.email ? ` (${session.user.email})` : ""}`
        : "Anonymous";
      const tools = Array.isArray(session.tools) ? session.tools.filter(Boolean).join(", ") : "-";
      return `
        <tr>
          <td title="${session.sessionId}">${session.sessionId.slice(0, 16)}…</td>
          <td>${user}</td>
          <td>${durationLabel(session.durationMs)}</td>
          <td>${numberLabel(session.eventCount)}</td>
          <td>${tools || "-"}</td>
          <td>${new Date(session.endedAt).toLocaleString()}</td>
        </tr>
      `;
    })
    .join("");
}

function renderRecentEvents(events) {
  if (!events || !events.length) {
    eventsBody.innerHTML = `<tr><td colspan="6">No events in this range.</td></tr>`;
    return;
  }

  eventsBody.innerHTML = events
    .map((event) => {
      const user = event.user && event.user.isAuthenticated
        ? event.user.name || event.user.email || "Auth User"
        : "Anonymous";
      return `
        <tr>
          <td>${new Date(event.eventAt).toLocaleString()}</td>
          <td>${event.eventType}</td>
          <td>${event.tool || "-"}</td>
          <td>${event.source || "-"}</td>
          <td>${user}</td>
          <td title="${event.sessionId}">${event.sessionId.slice(0, 14)}…</td>
        </tr>
      `;
    })
    .join("");
}

function renderToolChart(tools) {
  const ctx = document.getElementById("toolChart");
  const labels = (tools || []).slice(0, 10).map((t) => t.tool || "unknown");
  const eventCounts = (tools || []).slice(0, 10).map((t) => t.eventCount || t.events || 0);

  if (!toolChart) {
    toolChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "Events",
            data: eventCounts,
            backgroundColor: "rgba(24, 83, 209, 0.75)",
            borderRadius: 6,
          },
        ],
      },
      options: {
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: { display: false },
        },
        scales: {
          x: { ticks: { maxRotation: 20, minRotation: 20 } },
        },
      },
    });
    return;
  }

  toolChart.data.labels = labels;
  toolChart.data.datasets[0].data = eventCounts;
  toolChart.update("none");
}

function renderDailyChart(points) {
  const ctx = document.getElementById("dailyChart");
  const labels = (points || []).map((p) => p.day);
  const events = (points || []).map((p) => p.events);
  const sessions = (points || []).map((p) => p.sessions);

  if (!dailyChart) {
    dailyChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Events",
            data: events,
            borderColor: "#1853d1",
            backgroundColor: "rgba(24, 83, 209, 0.16)",
            tension: 0.25,
            fill: true,
          },
          {
            label: "Sessions",
            data: sessions,
            borderColor: "#0ea476",
            backgroundColor: "rgba(14, 164, 118, 0.12)",
            tension: 0.25,
            fill: false,
          },
        ],
      },
      options: {
        maintainAspectRatio: false,
        animation: false,
      },
    });
    return;
  }

  dailyChart.data.labels = labels;
  dailyChart.data.datasets[0].data = events;
  dailyChart.data.datasets[1].data = sessions;
  dailyChart.update("none");
}

function drawHeatmap(heatmap) {
  const ctx = heatmapCanvas.getContext("2d");
  const width = heatmapCanvas.width;
  const height = heatmapCanvas.height;

  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#f7f9ff";
  ctx.fillRect(0, 0, width, height);

  const isCompact = Boolean(heatmap && heatmap.compact);

  if (isCompact) {
    const bins = Array.isArray(heatmap.bins) ? heatmap.bins : [];
    const grid = heatmap.grid || { x: 96, y: 24 };
    const maxCount = Number(heatmap.maxCount || 0);
    const totalPoints = Number(heatmap.totalPoints || 0);

    if (!bins.length || maxCount <= 0) {
      ctx.fillStyle = "#6d7ea3";
      ctx.font = "14px IBM Plex Sans";
      ctx.fillText("No click data for this range.", 20, 28);
      heatmapCount.textContent = "0 points";
      return;
    }

    const cellWidth = width / Math.max(1, Number(grid.x || 96));
    const cellHeight = height / Math.max(1, Number(grid.y || 24));

    for (const bin of bins) {
      const intensity = Math.min(1, Number(bin.count || 0) / maxCount);
      if (intensity <= 0) continue;

      ctx.fillStyle = `rgba(255, 77, 0, ${0.08 + intensity * 0.48})`;
      ctx.fillRect(
        Math.floor(Number(bin.x || 0) * cellWidth),
        Math.floor(Number(bin.y || 0) * cellHeight),
        Math.ceil(cellWidth),
        Math.ceil(cellHeight)
      );
    }

    ctx.strokeStyle = "rgba(24, 83, 209, 0.12)";
    for (let x = 0; x <= width; x += 103) {
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, height);
      ctx.stroke();
    }
    for (let y = 0; y <= height; y += 40) {
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
      ctx.stroke();
    }

    heatmapCount.textContent = `${numberLabel(totalPoints)} points`;
    return;
  }

  const points = Array.isArray(heatmap && heatmap.points) ? heatmap.points : [];
  if (!points.length) {
    ctx.fillStyle = "#6d7ea3";
    ctx.font = "14px IBM Plex Sans";
    ctx.fillText("No click data for this range.", 20, 28);
    heatmapCount.textContent = "0 points";
    return;
  }

  const maxPoints = 1200;
  const sampled = points.slice(0, maxPoints);

  sampled.forEach((point) => {
    const px = Number.isFinite(Number(point.normalizedX))
      ? Number(point.normalizedX) * width
      : Number(point.x || 0);
    const py = Number.isFinite(Number(point.normalizedY))
      ? Number(point.normalizedY) * height
      : Number(point.y || 0);

    if (!Number.isFinite(px) || !Number.isFinite(py)) return;

    const gradient = ctx.createRadialGradient(px, py, 2, px, py, 24);
    gradient.addColorStop(0, "rgba(255, 77, 0, 0.22)");
    gradient.addColorStop(1, "rgba(255, 77, 0, 0)");

    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.arc(px, py, 24, 0, Math.PI * 2);
    ctx.fill();
  });

  ctx.strokeStyle = "rgba(24, 83, 209, 0.15)";
  for (let x = 0; x <= width; x += 103) {
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, height);
    ctx.stroke();
  }

  for (let y = 0; y <= height; y += 40) {
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  heatmapCount.textContent = `${numberLabel(points.length)} points`;
}

function updateToolFilterOptions(tools) {
  const currentValue = toolFilterEl.value;
  const options = ["all"].concat((tools || []).map((t) => t.tool).filter(Boolean));
  const unique = Array.from(new Set(options));

  toolFilterEl.innerHTML = unique
    .map((tool) => {
      const label = tool === "all" ? "All tools" : tool;
      return `<option value="${tool}">${label}</option>`;
    })
    .join("");

  if (unique.includes(currentValue)) {
    toolFilterEl.value = currentValue;
  } else {
    toolFilterEl.value = "all";
    state.tool = "all";
  }
}

async function loadDashboard() {
  if (activeLoadController) {
    activeLoadController.abort();
  }
  activeLoadController = new AbortController();
  const loadToken = ++currentLoadToken;

  const params = getQueryParams();
  params.set("heatmapCompact", "1");
  params.set("heatmapLimit", "12000");
  params.set("heatmapGridX", "96");
  params.set("heatmapGridY", "24");
  params.set("sessionsLimit", "60");
  params.set("eventsLimit", "100");
  const query = params.toString();
  setStatus("Loading dashboard data…");
  const startedAt = performance.now();

  try {
    const dashboard = await fetchJson(`/api/plugin-analytics/dashboard?${query}`, {
      signal: activeLoadController.signal,
    });

    if (loadToken !== currentLoadToken) {
      return;
    }

    const summary = dashboard.summary || {};
    const toolUsage = dashboard.toolUsage || {};
    const sessions = dashboard.sessions || {};
    const recentEvents = dashboard.recentEvents || {};

    renderKpis(summary.kpis || {});
    renderTopActions(summary.topActions || []);
    renderToolChart(toolUsage.tools || []);
    renderDailyChart(summary.eventsByDay || []);
    drawHeatmap(dashboard.heatmap || {});
    renderSessions(sessions.sessions || []);
    renderRecentEvents(recentEvents.events || []);
    updateToolFilterOptions(toolUsage.tools || []);

    const elapsedMs = Math.round(performance.now() - startedAt);

    setStatus(
      `Updated ${new Date().toLocaleTimeString()} • ${numberLabel(
        summary.kpis && summary.kpis.totalEvents
      )} events in range • ${elapsedMs}ms`
    );
  } catch (error) {
    if (error && error.name === "AbortError") {
      return;
    }
    console.error(error);
    setStatus(`Failed to load dashboard: ${error.message}`, true);
  }
}

function applyPreset(preset) {
  const now = new Date();
  if (preset === "24h") {
    state.from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    state.to = now;
  } else if (preset === "7d") {
    state.from = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    state.to = now;
  } else if (preset === "30d") {
    state.from = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    state.to = now;
  }

  fromDateEl.value = toLocalInputValue(state.from);
  toDateEl.value = toLocalInputValue(state.to);
}

function scheduleLoad(delayMs = 180) {
  if (loadDebounceTimer) {
    clearTimeout(loadDebounceTimer);
  }
  loadDebounceTimer = setTimeout(() => {
    loadDebounceTimer = null;
    loadDashboard();
  }, delayMs);
}

function bindControls() {
  rangePresetEl.addEventListener("change", () => {
    const preset = rangePresetEl.value;
    if (preset !== "custom") {
      applyPreset(preset);
      scheduleLoad(80);
    }
  });

  fromDateEl.addEventListener("change", () => {
    state.from = parseLocalInputValue(fromDateEl.value, state.from);
    rangePresetEl.value = "custom";
    scheduleLoad(80);
  });

  toDateEl.addEventListener("change", () => {
    state.to = parseLocalInputValue(toDateEl.value, state.to);
    rangePresetEl.value = "custom";
    scheduleLoad(80);
  });

  toolFilterEl.addEventListener("change", () => {
    state.tool = toolFilterEl.value;
    scheduleLoad(80);
  });

  authFilterEl.addEventListener("change", () => {
    state.auth = authFilterEl.value;
    scheduleLoad(80);
  });

  refreshBtn.addEventListener("click", () => loadDashboard());
}

function init() {
  applyPreset("7d");
  authFilterEl.value = "all";
  bindControls();
  loadDashboard();
}

init();
