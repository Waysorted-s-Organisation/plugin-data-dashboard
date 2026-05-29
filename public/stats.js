const statsHighlights = document.getElementById("statsHighlights");
const statsMeta = document.getElementById("statsMeta");
const inactiveMetrics = document.getElementById("inactiveMetrics");
const metricTabs = document.getElementById("metricTabs");
const rangeGroup = document.getElementById("rangeGroup");
const modeGroup = document.getElementById("modeGroup");
const chartCanvas = document.getElementById("statsChart");
const historyBody = document.getElementById("historyBody");
const statusLine = document.getElementById("statusLine");
const refreshBtn = document.getElementById("refreshBtn");

const ALL_METRICS = [
  { key: "mau", label: "Monthly Active Users", short: "MAU" },
  { key: "likes", label: "Likes", short: "Likes" },
  { key: "saves", label: "Saves", short: "Saves" },
  { key: "follows", label: "Follows", short: "Follows" },
  { key: "authenticatedUsers", label: "Auths", short: "Auths" },
  { key: "reused", label: "Reused", short: "Reused" },
];

let currentRange = "7d";
let currentMode = "linear";
let currentMetric = "mau";
let currentStats = null;
let currentHistory = [];
let chart = null;

if (window.Chart) {
  window.Chart.defaults.color = "#a9b7e5";
  window.Chart.defaults.borderColor = "rgba(159, 183, 255, 0.18)";
}

function resolveApiPath(path) {
  return new URL(path, `${window.location.protocol}//${window.location.host}`).toString();
}

function numberLabel(value, digits = 0) {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(Number(value || 0));
}

function dateLabel(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function shortDateLabel(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function deltaMarkup(value) {
  if (!Number.isFinite(Number(value)) || Number(value) === 0) {
    return `<span class="pill pill-muted">0</span>`;
  }

  const positive = Number(value) > 0;
  const label = `${positive ? "+" : ""}${numberLabel(value)}`;
  return `<span class="pill ${positive ? "pill-signal-high" : "pill-signal-low"}">${label}</span>`;
}

function getActiveMetrics() {
  return ALL_METRICS.filter((metric) => {
    if (metric.key === "mau" || metric.key === "authenticatedUsers" || metric.key === "reused") {
      return true;
    }
    if (metricValue(currentStats, metric.key) > 0) {
      return true;
    }
    return currentHistory.some((row) => metricValue(row, metric.key) > 0);
  });
}

function getInactiveMetrics() {
  const activeKeys = new Set(getActiveMetrics().map((metric) => metric.key));
  return ALL_METRICS.filter((metric) => !activeKeys.has(metric.key));
}

function metricValue(snapshot, metricKey) {
  if (!snapshot) return 0;
  if (metricKey === "reused") {
    return Number(snapshot.reused ?? snapshot.reuses ?? 0);
  }
  return Number(snapshot[metricKey] ?? 0);
}

function renderHighlights() {
  if (!currentStats) return;
  const delta = currentStats.deltas || {};
  const coverageStart = currentHistory[0] ? dateLabel(currentHistory[0].date || currentHistory[0]._id) : "--";

  const cards = [
    {
      label: "Current MAU",
      value: numberLabel(currentStats.mau),
      sub: `${numberLabel(currentStats.authenticatedUsers)} authenticated users`,
    },
    {
      label: "Reuse",
      value: numberLabel(currentStats.reused),
      sub: `Daily change ${numberLabel(delta.reused || 0)}`,
    },
    {
      label: "Coverage",
      value: `${numberLabel(currentHistory.length)} days`,
      sub: `History starts ${coverageStart}`,
    },
    {
      label: "Signals With Data",
      value: numberLabel(getActiveMetrics().length),
      sub: `${numberLabel(getInactiveMetrics().length)} inactive right now`,
    },
  ];

  statsHighlights.innerHTML = cards
    .map(
      (card) => `
        <article class="kpi-card">
          <p class="kpi-label">${card.label}</p>
          <p class="kpi-value">${card.value}</p>
          <p class="kpi-sub">${card.sub}</p>
        </article>
      `
    )
    .join("");
}

function renderMeta() {
  const firstRow = currentHistory[0];
  const lastRow = currentHistory[currentHistory.length - 1];
  const start = firstRow ? dateLabel(firstRow.date || firstRow._id) : "--";
  const end = lastRow ? dateLabel(lastRow.date || lastRow._id) : "--";
  statsMeta.innerHTML = `
    <article class="source-chip">
      <span class="pill pill-signal-high">Coverage</span>
      <span>${numberLabel(currentHistory.length)} daily points from ${start} to ${end}.</span>
    </article>
    <article class="source-chip">
      <span class="pill pill-signal-medium">MAU</span>
      <span>Calculated as a rolling 30-day active-user count from analytics events.</span>
    </article>
    <article class="source-chip">
      <span class="pill pill-muted">Engagement</span>
      <span>Likes, saves, and follows are only shown when production data exists.</span>
    </article>
  `;

  const inactive = getInactiveMetrics();
  if (!inactive.length) {
    inactiveMetrics.hidden = true;
    inactiveMetrics.textContent = "";
    return;
  }

  inactiveMetrics.hidden = false;
  inactiveMetrics.textContent = `No production records yet for: ${inactive.map((metric) => metric.short).join(", ")}.`;
}

function renderMetricTabs() {
  const delta = currentStats && currentStats.deltas ? currentStats.deltas : {};
  const metrics = getActiveMetrics();
  if (!metrics.some((metric) => metric.key === currentMetric)) {
    currentMetric = metrics[0] ? metrics[0].key : "mau";
  }
  metricTabs.innerHTML = metrics.map((metric) => {
    const active = metric.key === currentMetric;
    return `
      <button class="metric-tab ${active ? "active" : ""}" data-metric="${metric.key}" type="button" role="tab" aria-selected="${active}">
        <span class="kpi-label">${metric.short}</span>
        <span class="metric-tab-value">${numberLabel(metricValue(currentStats, metric.key))}</span>
        <span class="metric-tab-delta">${deltaMarkup(delta[metric.key] || 0)}</span>
      </button>
    `;
  }).join("");
}

function renderHistoryTable() {
  if (!currentHistory.length) {
    historyBody.innerHTML = `<tr><td colspan="7" class="muted">No snapshots available.</td></tr>`;
    return;
  }

  historyBody.innerHTML = currentHistory
    .slice()
    .reverse()
    .map((row) => `
      <tr>
        <td>${dateLabel(row.date || row._id)}</td>
        <td>${numberLabel(row.mau)}</td>
        <td>${numberLabel(row.authenticatedUsers)}</td>
        <td>${numberLabel(row.likes)}</td>
        <td>${numberLabel(row.saves)}</td>
        <td>${numberLabel(row.follows)}</td>
        <td>${numberLabel(row.reuses ?? row.reused ?? 0)}</td>
      </tr>
    `)
    .join("");
}

function renderChart() {
  if (chart) {
    chart.destroy();
    chart = null;
  }

  const metrics = getActiveMetrics();
  const metric = metrics.find((item) => item.key === currentMetric) || metrics[0] || ALL_METRICS[0];
  const labels = currentHistory.map((row) => shortDateLabel(row.date || row._id));
  let values = currentHistory.map((row) => metricValue(row, currentMetric));

  if (currentMode === "cumulative") {
    let runningTotal = 0;
    values = values.map((value) => {
      runningTotal += Number(value || 0);
      return runningTotal;
    });
  }

  const context = chartCanvas.getContext("2d");
  const gradient = context.createLinearGradient(0, 0, 0, chartCanvas.parentElement.clientHeight);
  gradient.addColorStop(0, "rgba(93, 155, 255, 0.28)");
  gradient.addColorStop(1, "rgba(93, 155, 255, 0)");

  chart = new Chart(context, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: metric.label,
          data: values,
          borderColor: "#5d9bff",
          backgroundColor: gradient,
          fill: true,
          tension: 0.28,
          pointRadius: values.length <= 1 ? 5 : 0,
          pointHoverRadius: 5,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
      },
      interaction: {
        intersect: false,
        mode: "index",
      },
      scales: {
        x: {
          grid: { color: "rgba(156, 182, 255, 0.08)", drawBorder: false },
          ticks: {
            maxTicksLimit: 12,
          },
          border: { display: false },
        },
        y: {
          beginAtZero: true,
          grid: { color: "rgba(156, 182, 255, 0.08)", drawBorder: false },
          border: { display: false },
        },
      },
    },
  });
}

async function fetchJson(path) {
  const response = await fetch(resolveApiPath(path));
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

async function loadStats() {
  statusLine.textContent = "Loading stats…";
  statusLine.classList.remove("bad");

  try {
    const [stats, history] = await Promise.all([
      fetchJson("/api/plugin-analytics/stats"),
      fetchJson(`/api/plugin-analytics/stats/history?range=${currentRange}`),
    ]);

    currentStats = stats;
    currentHistory = Array.isArray(history)
      ? history.sort((a, b) => new Date(a.date || a._id) - new Date(b.date || b._id))
      : [];

    renderHighlights();
    renderMeta();
    renderMetricTabs();
    renderChart();
    renderHistoryTable();
    statusLine.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    console.error(error);
    statusLine.textContent = `Failed to load stats: ${error.message}`;
    statusLine.classList.add("bad");
  }
}

rangeGroup.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-range]");
  if (!button) return;
  rangeGroup.querySelectorAll("button").forEach((item) => item.classList.remove("active"));
  button.classList.add("active");
  currentRange = button.dataset.range;
  loadStats();
});

modeGroup.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-mode]");
  if (!button) return;
  modeGroup.querySelectorAll("button").forEach((item) => item.classList.remove("active"));
  button.classList.add("active");
  currentMode = button.dataset.mode;
  renderChart();
});

metricTabs.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-metric]");
  if (!button) return;
  currentMetric = button.dataset.metric;
  renderMetricTabs();
  renderChart();
});

refreshBtn.addEventListener("click", loadStats);

loadStats();
