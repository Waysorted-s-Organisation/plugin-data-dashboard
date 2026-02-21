const statusLine = document.getElementById("statusLine")
const heatmapMetaEl = document.getElementById("heatmapMeta")
const heatmapCanvas = document.getElementById("heatmapCanvas")
const targetsBody = document.getElementById("targetsBody")
const refreshBtn = document.getElementById("refreshBtn")

const rangePresetEl = document.getElementById("rangePreset")
const fromDateEl = document.getElementById("fromDate")
const toDateEl = document.getElementById("toDate")
const toolFilterEl = document.getElementById("toolFilter")
const authFilterEl = document.getElementById("authFilter")
const actionFilterEl = document.getElementById("actionFilter")

const TOOL_LABELS = {
  dashboard: "Plugin Dashboard",
  "collapsed-dashboard": "Collapsed Dashboard",
  palettable: "Palette Tool",
  "frame-gallery": "Frame Gallery",
  "import-tool": "Import Tool",
  "unit-converter": "Unit Converter",
  profile: "User Profile",
  "wayfall-game": "Wayfall Mini Game",
  "liquid-glass": "Liquid Glass",
}

const state = {
  from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
  to: new Date(),
  tool: "all",
  auth: "all",
  action: "all",
}

let loadDebounceTimer = null
let realtimeTimer = null
let latestHeatmap = null
let resizeObserver = null

function escapeHtml(value) {
  const text = String(value === null || value === undefined ? "" : value)
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;")
}

function setStatus(text, isError = false) {
  statusLine.textContent = text
  statusLine.classList.toggle("bad", Boolean(isError))
}

function toLocalInputValue(date) {
  const pad = (n) => String(n).padStart(2, "0")
  const d = new Date(date.getTime() - date.getTimezoneOffset() * 60000)
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}`
}

function parseLocalInputValue(value, fallback) {
  if (!value) return fallback
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? fallback : d
}

function numberLabel(value) {
  return new Intl.NumberFormat().format(Number(value || 0))
}

function humanizeIdentifier(value) {
  const raw = String(value || "").trim()
  if (!raw) return "Unknown"
  return raw
    .replace(/[._-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
}

function labelTool(tool) {
  const key = String(tool || "").trim().toLowerCase()
  if (!key) return "Unknown"
  return TOOL_LABELS[key] || humanizeIdentifier(key)
}

function getQueryParams() {
  const params = new URLSearchParams({
    from: state.from.toISOString(),
    to: state.to.toISOString(),
  })
  if (state.tool !== "all") params.set("tool", state.tool)
  if (state.auth !== "all") params.set("auth", state.auth)
  if (state.action !== "all") params.set("action", state.action)
  return params
}

async function fetchJson(path) {
  const response = await fetch(path)
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`${response.status} ${response.statusText} - ${text}`)
  }
  return response.json()
}

function applyPreset(preset) {
  const now = new Date()
  if (preset === "24h") {
    state.from = new Date(now.getTime() - 24 * 60 * 60 * 1000)
    state.to = now
  } else if (preset === "7d") {
    state.from = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
    state.to = now
  } else if (preset === "30d") {
    state.from = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)
    state.to = now
  }
  fromDateEl.value = toLocalInputValue(state.from)
  toDateEl.value = toLocalInputValue(state.to)
}

function ensureCanvasSize(minHeight = 420) {
  const parent = heatmapCanvas && heatmapCanvas.parentElement
  const rect = parent ? parent.getBoundingClientRect() : null
  const cssWidth = Math.max(260, Math.floor((rect && rect.width) || heatmapCanvas.clientWidth || 260))
  const cssHeight = Math.max(
    minHeight,
    Math.floor((rect && rect.height) || heatmapCanvas.clientHeight || minHeight)
  )
  const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1))
  const width = Math.floor(cssWidth * dpr)
  const height = Math.floor(cssHeight * dpr)

  if (heatmapCanvas.width !== width || heatmapCanvas.height !== height) {
    heatmapCanvas.width = width
    heatmapCanvas.height = height
  }

  heatmapCanvas.style.width = `${cssWidth}px`
  heatmapCanvas.style.height = `${cssHeight}px`

  const ctx = heatmapCanvas.getContext("2d")
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
  return { ctx, width: cssWidth, height: cssHeight }
}

function drawHeatmap(heatmap) {
  latestHeatmap = heatmap || {}
  const { ctx, width, height } = ensureCanvasSize(420)

  ctx.clearRect(0, 0, width, height)
  ctx.fillStyle = "#0b1324"
  ctx.fillRect(0, 0, width, height)

  const bins = Array.isArray(heatmap && heatmap.bins) ? heatmap.bins : []
  const grid = heatmap && heatmap.grid ? heatmap.grid : { x: 128, y: 40 }
  const maxCount = Number(heatmap && heatmap.maxCount ? heatmap.maxCount : 0)
  const totalPoints = Number(heatmap && heatmap.totalPoints ? heatmap.totalPoints : 0)

  if (!bins.length || maxCount <= 0) {
    ctx.fillStyle = "#9caada"
    ctx.font = "500 14px 'Space Grotesk'"
    ctx.fillText("No click data for this range.", 20, 28)
    heatmapMetaEl.textContent = "0 points"
    return
  }

  const cellWidth = width / Math.max(1, Number(grid.x || 128))
  const cellHeight = height / Math.max(1, Number(grid.y || 40))

  for (const bin of bins) {
    const intensity = Math.min(1, Number(bin.count || 0) / maxCount)
    if (intensity <= 0) continue

    const hue = 38 - intensity * 30
    const alpha = 0.08 + intensity * 0.72
    ctx.fillStyle = `hsla(${hue}, 100%, 56%, ${alpha})`
    ctx.fillRect(
      Math.floor(Number(bin.x || 0) * cellWidth),
      Math.floor(Number(bin.y || 0) * cellHeight),
      Math.ceil(cellWidth),
      Math.ceil(cellHeight)
    )
  }

  ctx.strokeStyle = "rgba(159, 183, 255, 0.14)"
  ctx.lineWidth = 1
  const columns = Math.max(10, Math.min(24, Math.floor(width / 76)))
  const rows = Math.max(8, Math.min(18, Math.floor(height / 40)))
  const stepX = width / columns
  const stepY = height / rows

  for (let i = 1; i < columns; i += 1) {
    const x = Math.floor(i * stepX) + 0.5
    ctx.beginPath()
    ctx.moveTo(x, 0)
    ctx.lineTo(x, height)
    ctx.stroke()
  }

  for (let i = 1; i < rows; i += 1) {
    const y = Math.floor(i * stepY) + 0.5
    ctx.beginPath()
    ctx.moveTo(0, y)
    ctx.lineTo(width, y)
    ctx.stroke()
  }

  heatmapMetaEl.textContent = `${numberLabel(totalPoints)} points • ${numberLabel(bins.length)} active bins`
}

function renderTopTargets(heatmapPoints) {
  const points = Array.isArray(heatmapPoints) ? heatmapPoints : []

  if (!points.length) {
    targetsBody.innerHTML = `<tr><td colspan="3">No click targets found for this range.</td></tr>`
    return
  }

  const counter = new Map()

  for (const point of points) {
    const targetRaw =
      point.actionLabel ||
      point.elementId ||
      point.action ||
      point.elementTag ||
      "unknown-target"

    const toolRaw = point.elementToolId || point.tool || "unknown"
    const key = `${String(targetRaw)}|${String(toolRaw)}`
    const existing = counter.get(key)

    if (existing) {
      existing.count += 1
    } else {
      counter.set(key, {
        target: String(targetRaw),
        tool: String(toolRaw),
        count: 1,
      })
    }
  }

  const rows = Array.from(counter.values()).sort((a, b) => b.count - a.count).slice(0, 40)

  targetsBody.innerHTML = rows
    .map(
      (row) => `<tr>
      <td>${escapeHtml(humanizeIdentifier(row.target))}</td>
      <td>${escapeHtml(labelTool(row.tool))}</td>
      <td>${numberLabel(row.count)}</td>
    </tr>`
    )
    .join("")
}

function updateToolFilterOptions(tools) {
  const currentValue = toolFilterEl.value
  const values = ["all"].concat((tools || []).map((row) => row.tool).filter(Boolean))
  const unique = Array.from(new Set(values))

  toolFilterEl.innerHTML = unique
    .map(
      (tool) =>
        `<option value="${escapeHtml(tool)}">${escapeHtml(tool === "all" ? "All tools" : labelTool(tool))}</option>`
    )
    .join("")

  if (unique.includes(currentValue)) {
    toolFilterEl.value = currentValue
  } else {
    toolFilterEl.value = "all"
    state.tool = "all"
  }
}

function updateActionFilterOptions(actions) {
  const currentValue = actionFilterEl.value
  const unique = [
    "all",
    ...Array.from(new Set((actions || []).map((row) => row.action).filter(Boolean))),
  ]

  actionFilterEl.innerHTML = unique
    .map(
      (action) =>
        `<option value="${escapeHtml(action)}">${escapeHtml(
          action === "all" ? "All actions" : humanizeIdentifier(action)
        )}</option>`
    )
    .join("")

  if (unique.includes(currentValue)) {
    actionFilterEl.value = currentValue
  } else {
    actionFilterEl.value = "all"
    state.action = "all"
  }
}

async function loadHeatmap(options = {}) {
  const silent = Boolean(options.silent)
  const query = getQueryParams().toString()
  if (!silent) {
    setStatus("Loading heatmap intelligence…")
  }

  const startedAt = performance.now()

  try {
    const [dashboard, compactHeatmap, fullHeatmap] = await Promise.all([
      fetchJson(`/api/plugin-analytics/dashboard?${query}&eventsLimit=1&sessionsLimit=1&heatmapLimit=1`),
      fetchJson(`/api/plugin-analytics/heatmap?${query}&compact=1&limit=20000&gridX=128&gridY=40`),
      fetchJson(`/api/plugin-analytics/heatmap?${query}&compact=0&limit=6000`),
    ])

    drawHeatmap(compactHeatmap)
    renderTopTargets(fullHeatmap.points || [])
    updateToolFilterOptions((dashboard.toolUsage && dashboard.toolUsage.tools) || [])
    updateActionFilterOptions((dashboard.actionCatalog && dashboard.actionCatalog.actions) || [])

    const elapsedMs = Math.round(performance.now() - startedAt)
    setStatus(
      `Updated ${new Date().toLocaleTimeString()} • ${numberLabel(
        compactHeatmap.totalPoints || 0
      )} points • ${elapsedMs}ms`
    )
  } catch (error) {
    console.error(error)
    setStatus(`Failed to load heatmap analytics: ${error.message}`, true)
  }
}

function scheduleLoad(delayMs = 140) {
  if (loadDebounceTimer) clearTimeout(loadDebounceTimer)
  loadDebounceTimer = setTimeout(() => {
    loadDebounceTimer = null
    loadHeatmap()
  }, delayMs)
}

function startRealtimeRefresh() {
  if (realtimeTimer) clearInterval(realtimeTimer)
  realtimeTimer = setInterval(() => {
    if (document.visibilityState !== "visible") return
    loadHeatmap({ silent: true })
  }, 10000)
}

function bindResize() {
  if (resizeObserver) {
    resizeObserver.disconnect()
    resizeObserver = null
  }

  const redraw = () => {
    if (latestHeatmap) {
      drawHeatmap(latestHeatmap)
    }
  }

  if (typeof ResizeObserver !== "undefined") {
    resizeObserver = new ResizeObserver(() => redraw())
    if (heatmapCanvas.parentElement) {
      resizeObserver.observe(heatmapCanvas.parentElement)
    }
  }

  window.addEventListener("resize", redraw)
}

function bindControls() {
  rangePresetEl.addEventListener("change", () => {
    const preset = rangePresetEl.value
    if (preset !== "custom") {
      applyPreset(preset)
      scheduleLoad(60)
    }
  })
  fromDateEl.addEventListener("change", () => {
    state.from = parseLocalInputValue(fromDateEl.value, state.from)
    rangePresetEl.value = "custom"
    scheduleLoad(60)
  })
  toDateEl.addEventListener("change", () => {
    state.to = parseLocalInputValue(toDateEl.value, state.to)
    rangePresetEl.value = "custom"
    scheduleLoad(60)
  })
  toolFilterEl.addEventListener("change", () => {
    state.tool = toolFilterEl.value
    scheduleLoad(60)
  })
  authFilterEl.addEventListener("change", () => {
    state.auth = authFilterEl.value
    scheduleLoad(60)
  })
  actionFilterEl.addEventListener("change", () => {
    state.action = actionFilterEl.value
    scheduleLoad(60)
  })
  refreshBtn.addEventListener("click", () => loadHeatmap())
}

function init() {
  applyPreset("7d")
  toolFilterEl.value = "all"
  authFilterEl.value = "all"
  actionFilterEl.value = "all"
  bindControls()
  bindResize()
  loadHeatmap()
  startRealtimeRefresh()
}

init()
