/* ──────────────────────────────────────────────────────
   stats.js  --  Waysorted Plugin Suite  --  Public Stats
   ────────────────────────────────────────────────────── */

// ── DOM References ──────────────────────────────────────
const kpiGrid      = document.getElementById('kpiGrid');
const metricTabs   = document.getElementById('metricTabs');
const rangeGroup   = document.getElementById('rangeGroup');
const modeGroup    = document.getElementById('modeGroup');
const historyBody  = document.getElementById('historyBody');
const statusLine   = document.getElementById('statusLine');
const chartCanvas  = document.getElementById('statsChart');
const actionBtns   = document.querySelectorAll('.action-btn');

// ── State ───────────────────────────────────────────────
let currentRange   = '90d';
let currentMetric  = 'mau';
let currentMode    = 'linear';
let statsData      = null;
let historyData    = [];
let chartInstance  = null;
let refreshTimer   = null;

// ── Metric Config ───────────────────────────────────────
const METRICS = [
  { key: 'mau',      label: 'Monthly Active Users', short: 'Users' },
  { key: 'likes',    label: 'Total Likes',          short: 'Likes' },
  { key: 'saves',    label: 'Total Saves',          short: 'Saves' },
  { key: 'follows',  label: 'Followers',            short: 'Follows' },
  { key: 'installs', label: 'Auths',                short: 'Auths' },
  { key: 'reused',   label: 'Reuses',               short: 'Reused' },
];

// ── Utility ─────────────────────────────────────────────
function fmt(n) {
  if (n == null) return '--';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000)     return (n / 1_000).toFixed(1) + 'K';
  return String(n);
}

function deltaHTML(value) {
  if (value == null || value === 0) {
    return '<span class="delta-badge delta-neutral">0</span>';
  }
  const cls  = value > 0 ? 'delta-up' : 'delta-down';
  const sign = value > 0 ? '+' : '';
  return `<span class="delta-badge ${cls}">${sign}${value}</span>`;
}

function fmtDate(d) {
  const dt = new Date(d);
  return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function fmtDateShort(d) {
  const dt = new Date(d);
  return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

// ── Load Stats (KPIs + tab values) ─────────────────────
async function loadStats() {
  try {
    const res = await fetch('/api/plugin-analytics/stats');
    if (!res.ok) throw new Error('Failed to load stats');
    statsData = await res.json();

    renderKPIs(statsData);
    updateTabValues(statsData);
    statusLine.style.display = 'none';
  } catch (err) {
    console.error('loadStats error:', err);
    statusLine.textContent = 'Error loading stats.';
    statusLine.className = 'status bad';
  }
}

function renderKPIs(data) {
  const deltas = data.deltas?.daily || {};
  const cards = METRICS.map(m => {
    const val   = data[m.key] ?? 0;
    const delta = deltas[m.key] ?? null;
    return `
      <div class="kpi-card">
        <p class="kpi-label">${m.label}</p>
        <p class="kpi-value">${fmt(val)}</p>
        <p class="kpi-sub">${deltaHTML(delta)}</p>
      </div>`;
  }).join('');
  kpiGrid.innerHTML = cards;
}

function updateTabValues(data) {
  METRICS.forEach(m => {
    const el = document.getElementById(`tab-val-${m.key}`);
    if (el) el.textContent = fmt(data[m.key] ?? 0);
  });
}

// ── Load History ────────────────────────────────────────
async function loadHistory(range) {
  try {
    const res = await fetch(`/api/plugin-analytics/stats/history?range=${range}`);
    if (!res.ok) throw new Error('Failed to load history');
    historyData = await res.json();

    // Sort oldest first for charting
    historyData.sort((a, b) => new Date(a.date || a._id) - new Date(b.date || b._id));

    renderChart(currentMetric, currentMode);
    renderTable(historyData);
  } catch (err) {
    console.error('loadHistory error:', err);
    historyData = [];
    renderChart(currentMetric, currentMode);
    renderTable([]);
  }
}

// ── Render Chart ────────────────────────────────────────
function renderChart(metric, mode) {
  if (!chartCanvas) return;
  const ctx = chartCanvas.getContext('2d');

  // Destroy previous instance
  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }

  if (!historyData || historyData.length === 0) {
    // Show empty state
    ctx.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
    const parent = chartCanvas.parentElement;
    let noData = parent.querySelector('.no-data');
    if (!noData) {
      noData = document.createElement('div');
      noData.className = 'no-data';
      noData.textContent = 'No data yet';
      parent.appendChild(noData);
    }
    return;
  }

  // Remove no-data overlay if present
  const noData = chartCanvas.parentElement.querySelector('.no-data');
  if (noData) noData.remove();

  const labels = historyData.map(d => fmtDateShort(d.date || d._id));
  let values   = historyData.map(d => d[metric] ?? 0);

  // Cumulative mode: running total
  if (mode === 'cumulative') {
    const cumulative = [];
    let total = 0;
    for (const v of values) {
      total += v;
      cumulative.push(total);
    }
    values = cumulative;
  }

  // Gradient fill
  const gradient = ctx.createLinearGradient(0, 0, 0, chartCanvas.parentElement.clientHeight);
  gradient.addColorStop(0, 'rgba(93, 155, 255, 0.28)');
  gradient.addColorStop(0.7, 'rgba(93, 155, 255, 0.06)');
  gradient.addColorStop(1, 'rgba(93, 155, 255, 0)');

  chartInstance = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: METRICS.find(m => m.key === metric)?.short || metric,
        data: values,
        borderColor: '#5d9bff',
        borderWidth: 2,
        backgroundColor: gradient,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 5,
        pointHoverBackgroundColor: '#5d9bff',
        pointHoverBorderColor: '#fff',
        pointHoverBorderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        intersect: false,
        mode: 'index',
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(10, 18, 36, 0.94)',
          titleColor: '#ecf2ff',
          bodyColor: '#ecf2ff',
          borderColor: 'rgba(156, 182, 255, 0.22)',
          borderWidth: 1,
          padding: 10,
          cornerRadius: 8,
          titleFont: { family: 'Space Grotesk', weight: '600' },
          bodyFont: { family: 'Space Grotesk' },
          displayColors: false,
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(156, 182, 255, 0.08)', drawBorder: false },
          ticks: {
            color: '#9eadd6',
            font: { family: 'Space Grotesk', size: 11 },
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 12,
          },
          border: { display: false },
        },
        y: {
          grid: { color: 'rgba(156, 182, 255, 0.08)', drawBorder: false },
          ticks: {
            color: '#9eadd6',
            font: { family: 'Space Grotesk', size: 11 },
            padding: 8,
          },
          border: { display: false },
          beginAtZero: true,
        },
      },
    },
  });
}

// ── Render Historical Table ─────────────────────────────
function renderTable(data) {
  if (!historyBody) return;

  if (!data || data.length === 0) {
    historyBody.innerHTML = `
      <tr>
        <td colspan="7" style="text-align:center; padding:32px; color:var(--muted);">
          No data yet
        </td>
      </tr>`;
    return;
  }

  // Show newest first in table
  const sorted = [...data].reverse();
  historyBody.innerHTML = sorted.map(row => `
    <tr>
      <td>${fmtDate(row.date || row._id)}</td>
      <td>${row.mau ?? 0}</td>
      <td>${row.likes ?? 0}</td>
      <td>${row.saves ?? 0}</td>
      <td>${row.follows ?? 0}</td>
      <td>${row.installs ?? 0}</td>
      <td>${row.reused ?? 0}</td>
    </tr>`).join('');
}

// ── Action Buttons (Like / Save / Follow) ───────────────
async function performAction(action) {
  try {
    const res = await fetch(`/api/plugin-analytics/stats/${action}`, {
      method: 'POST',
    });
    if (res.ok) {
      loadStats();
    }
  } catch (err) {
    console.error('Action failed:', err);
  }
}

// ── Event Listeners ─────────────────────────────────────

// Range selector
rangeGroup.addEventListener('click', (e) => {
  const btn = e.target.closest('.range-btn');
  if (!btn) return;
  rangeGroup.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentRange = btn.dataset.range;
  loadHistory(currentRange);
});

// Mode toggle
modeGroup.addEventListener('click', (e) => {
  const btn = e.target.closest('.mode-btn');
  if (!btn) return;
  modeGroup.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentMode = btn.dataset.mode;
  renderChart(currentMetric, currentMode);
});

// Metric tabs
metricTabs.addEventListener('click', (e) => {
  const tab = e.target.closest('.metric-tab');
  if (!tab) return;
  metricTabs.querySelectorAll('.metric-tab').forEach(t => t.classList.remove('active'));
  tab.classList.add('active');
  currentMetric = tab.dataset.metric;
  renderChart(currentMetric, currentMode);
});

// Action buttons
actionBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    const action = btn.getAttribute('data-action');
    performAction(action);
  });
});

// ── Init ────────────────────────────────────────────────
async function init() {
  await Promise.all([
    loadStats(),
    loadHistory(currentRange),
  ]);
}

init();

// ── Auto-refresh every 60s ──────────────────────────────
refreshTimer = setInterval(() => {
  loadStats();
  loadHistory(currentRange);
}, 60_000);
