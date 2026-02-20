const statusLine = document.getElementById("statusLine")
const kpiGrid = document.getElementById("kpiGrid")
const insightsGrid = document.getElementById("insightsGrid")
const actionsBody = document.getElementById("actionsBody")
const sessionsBody = document.getElementById("sessionsBody")
const eventsBody = document.getElementById("eventsBody")
const heatmapCanvas = document.getElementById("heatmapCanvas")
const heatmapCount = document.getElementById("heatmapCount")

const rangePresetEl = document.getElementById("rangePreset")
const fromDateEl = document.getElementById("fromDate")
const toDateEl = document.getElementById("toDate")
const toolFilterEl = document.getElementById("toolFilter")
const authFilterEl = document.getElementById("authFilter")
const showSystemEventsEl = document.getElementById("showSystemEvents")
const refreshBtn = document.getElementById("refreshBtn")

const PASSIVE_EVENT_TYPES = new Set(["session_heartbeat", "plugin_message"])
const TOOL_LABELS = {
  dashboard: "Plugin Dashboard",
  palettable: "Palette Tool",
  frame_gallery: "Frame Gallery",
  import_tool: "Import Tool",
  unit_converter: "Unit Converter",
  liquid_glass: "Liquid Glass",
  profile: "User Profile",
  game: "Mini Game",
  unknown: "Unmapped Tool",
}

const EVENT_METADATA = {
  plugin_session_started: {
    label: "Session Started",
    description: "Plugin launch recorded for a new session.",
    category: "Lifecycle",
    signal: "medium",
  },
  plugin_session_ended: {
    label: "Session Ended",
    description: "Plugin close/end lifecycle event.",
    category: "Lifecycle",
    signal: "medium",
  },
  session_heartbeat: {
    label: "Session Heartbeat",
    description: "Background keep-alive ping while plugin stays open.",
    category: "System",
    signal: "low",
  },
  plugin_message: {
    label: "Plugin Message",
    description: "Internal UI to plugin bridge communication.",
    category: "System",
    signal: "low",
  },
  ui_click: {
    label: "UI Click",
    description: "User clicked a control in plugin UI.",
    category: "Interaction",
    signal: "high",
  },
  tool_opened: {
    label: "Tool Opened",
    description: "A tool panel was opened by user.",
    category: "Navigation",
    signal: "high",
  },
  tool_closed: {
    label: "Tool Closed",
    description: "A tool panel was closed by user.",
    category: "Navigation",
    signal: "medium",
  },
  tool_context_changed: {
    label: "Tool Context Changed",
    description: "User navigated between plugin tool contexts.",
    category: "Navigation",
    signal: "medium",
  },
  tool_time_spent: {
    label: "Tool Time Spent",
    description: "Measured active time spent inside a tool.",
    category: "Engagement",
    signal: "high",
  },
  analytics_transport_updated: {
    label: "Analytics Transport Updated",
    description: "Analytics endpoint or token setting was updated.",
    category: "System",
    signal: "low",
  },
  unknown_event: {
    label: "Unknown Event",
    description: "Event type not yet mapped in dashboard catalog.",
    category: "Unmapped",
    signal: "low",
  },
}

const state = {
  from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
  to: new Date(),
  tool: "all",
  auth: "all",
  includeSystemEvents: false,
}

let toolChart = null
let dailyChart = null
let loadDebounceTimer = null
let activeLoadController = null
let currentLoadToken = 0
let realtimeRefreshTimer = null
let lastRenderFingerprint = ""
const REALTIME_REFRESH_MS = 8000

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

function durationLabel(ms) {
  const seconds = Math.floor(Number(ms || 0) / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  const remSeconds = seconds % 60
  if (minutes < 60) return `${minutes}m ${remSeconds}s`
  const hours = Math.floor(minutes / 60)
  const remMinutes = minutes % 60
  return `${hours}h ${remMinutes}m`
}

function numberLabel(value) {
  return new Intl.NumberFormat().format(Number(value || 0))
}

function percentLabel(value, digits = 1) {
  const n = Number(value || 0)
  return `${(n * 100).toFixed(digits)}%`
}

function humanizeIdentifier(value) {
  const raw = String(value || "").trim()
  if (!raw) return "Unknown"
  return raw
    .replace(/[._-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
}

function labelTool(toolId) {
  const key = String(toolId || "unknown").trim().toLowerCase()
  if (!key) return "Unknown Tool"
  if (TOOL_LABELS[key]) return TOOL_LABELS[key]
  return humanizeIdentifier(key)
}

function getEventMeta(eventKey) {
  const key = String(eventKey || "unknown_event").trim() || "unknown_event"
  const mapped = EVENT_METADATA[key]
  if (mapped) {
    return {
      key,
      ...mapped,
      passive: PASSIVE_EVENT_TYPES.has(key),
    }
  }

  return {
    key,
    label: humanizeIdentifier(key),
    description: "Custom telemetry event captured from plugin runtime.",
    category: "Custom",
    signal: "medium",
    passive: PASSIVE_EVENT_TYPES.has(key),
  }
}

function signalPillClass(signal) {
  if (signal === "high") return "pill-signal-high"
  if (signal === "medium") return "pill-signal-medium"
  return "pill-signal-low"
}

function eventDetailText(event, meta) {
  const payload = event && event.payload && typeof event.payload === "object" ? event.payload : {}
  const action = String(payload.action || "").trim()

  if (meta.key === "tool_time_spent") {
    return `Time tracked: ${durationLabel(payload.durationMs)} in ${labelTool(event.tool)}`
  }

  if (meta.key === "ui_click") {
    const element = payload.element && typeof payload.element === "object" ? payload.element : {}
    const target = element.id || element.toolId || element.tag || "UI control"
    return `Clicked: ${humanizeIdentifier(target)}`
  }

  if (meta.key === "tool_opened" || meta.key === "tool_closed" || meta.key === "tool_context_changed") {
    const toolFromPayload = payload.tool || payload.uiTool || event.tool
    return `${meta.label} for ${labelTool(toolFromPayload)}`
  }

  if (meta.key === "session_heartbeat") {
    return "Background health ping, not a direct user action"
  }

  if (meta.key === "plugin_message") {
    const messageType = payload.type || payload.messageType || payload.action
    if (messageType) return `Internal message: ${humanizeIdentifier(messageType)}`
    return "Internal bridge message between UI and plugin runtime"
  }

  if (action && action !== meta.key) {
    return `Action: ${humanizeIdentifier(action)}`
  }

  return meta.description
}

function buildDerivedAnalysis(dashboard) {
  const summary = dashboard.summary || {}
  const kpis = summary.kpis || {}
  const totalEvents = Number(kpis.totalEvents || 0)
  const totalSessions = Math.max(1, Number(kpis.totalSessions || 0))
  const tools = Array.isArray(dashboard.toolUsage && dashboard.toolUsage.tools)
    ? dashboard.toolUsage.tools
    : []

  const eventBreakdown = Array.isArray(dashboard.eventTypeBreakdown)
    ? dashboard.eventTypeBreakdown
    : []

  const eventBreakdownRows = eventBreakdown.map((row) => {
    const eventType = String(row.eventType || "unknown_event")
    const count = Number(row.count || 0)
    return {
      eventType,
      count,
      meta: getEventMeta(eventType),
    }
  })

  const passiveFromBreakdown = eventBreakdownRows
    .filter((row) => row.meta.passive)
    .reduce((sum, row) => sum + row.count, 0)

  const sortedTools = tools
    .map((toolRow) => ({
      tool: String(toolRow.tool || "unknown"),
      eventCount: Number(toolRow.eventCount || 0),
      activeEventCount: Number(toolRow.activeEventCount || toolRow.eventCount || 0),
      passiveEventCount: Number(toolRow.passiveEventCount || 0),
      clickCount: Number(toolRow.clickCount || 0),
      timeSpentMs: Number(toolRow.timeSpentMs || 0),
    }))
    .sort((a, b) => b.activeEventCount - a.activeEventCount)

  const passiveFromTools = sortedTools.reduce(
    (sum, toolRow) => sum + Number(toolRow.passiveEventCount || 0),
    0
  )

  const passiveEvents = passiveFromTools > 0 ? passiveFromTools : passiveFromBreakdown
  const meaningfulEvents = Math.max(totalEvents - passiveEvents, 0)
  const noiseShare = totalEvents > 0 ? passiveEvents / totalEvents : 0
  const activeShare = totalEvents > 0 ? meaningfulEvents / totalEvents : 0
  const avgMeaningfulPerSession = meaningfulEvents / totalSessions

  const clickRow = eventBreakdownRows.find((row) => row.eventType === "ui_click")
  const clickCount = clickRow ? clickRow.count : 0

  const topMeaningfulEvent = eventBreakdownRows
    .filter((row) => !row.meta.passive)
    .sort((a, b) => b.count - a.count)[0] || null

  const topTool = sortedTools[0] || null
  const topToolShare = topTool && meaningfulEvents > 0
    ? topTool.activeEventCount / meaningfulEvents
    : 0

  const notes = []

  if (noiseShare > 0.7) {
    notes.push({
      tone: "warn",
      title: "High system-noise detected",
      text: `${percentLabel(noiseShare)} of events are system telemetry. Use focus mode to prioritize user intent signals.`,
    })
  } else {
    notes.push({
      tone: "good",
      title: "Healthy event quality",
      text: `${percentLabel(activeShare)} of events are user-driven actions.`,
    })
  }

  if (Number(kpis.authenticatedUsers || 0) === 0) {
    notes.push({
      tone: "neutral",
      title: "No authenticated users in this range",
      text: "Data is currently from anonymous users only. Auth segmentation is available when login events arrive.",
    })
  }

  if (topMeaningfulEvent) {
    notes.push({
      tone: "neutral",
      title: `Top intent event: ${topMeaningfulEvent.meta.label}`,
      text: `${numberLabel(topMeaningfulEvent.count)} occurrences in selected range.`,
    })
  }

  if (topTool) {
    notes.push({
      tone: "neutral",
      title: `Most active tool: ${labelTool(topTool.tool)}`,
      text: `${percentLabel(topToolShare)} of meaningful actions come from this tool.`,
    })
  }

  return {
    totalEvents,
    meaningfulEvents,
    passiveEvents,
    activeShare,
    noiseShare,
    avgMeaningfulPerSession,
    clickCount,
    clickPerSession: clickCount / totalSessions,
    topMeaningfulEvent,
    topTool,
    topToolShare,
    notes,
  }
}

function getQueryParams() {
  const params = new URLSearchParams({
    from: state.from.toISOString(),
    to: state.to.toISOString(),
  })

  if (state.tool && state.tool !== "all") {
    params.set("tool", state.tool)
  }

  if (state.auth && state.auth !== "all") {
    params.set("auth", state.auth)
  }

  return params
}

async function fetchJson(path, options = {}) {
  const response = await fetch(path, options)
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`${response.status} ${response.statusText} - ${text}`)
  }
  return response.json()
}

function renderKpis(kpis, analysis) {
  const cards = [
    {
      label: "Total Telemetry",
      value: numberLabel(kpis.totalEvents),
      sub: "All captured events",
    },
    {
      label: "Meaningful Actions",
      value: numberLabel(analysis.meaningfulEvents),
      sub: `${percentLabel(analysis.activeShare)} user-driven`,
      tone: "good",
    },
    {
      label: "System Noise",
      value: numberLabel(analysis.passiveEvents),
      sub: `${percentLabel(analysis.noiseShare)} background`,
      tone: analysis.noiseShare > 0.7 ? "warn" : "neutral",
    },
    {
      label: "Sessions",
      value: numberLabel(kpis.totalSessions),
      sub: `${numberLabel(kpis.authenticatedUsers)} authenticated users`,
    },
    {
      label: "Active Actions / Session",
      value: analysis.avgMeaningfulPerSession.toFixed(1),
      sub: "Intent depth",
    },
    {
      label: "Avg Session Time",
      value: durationLabel(kpis.avgSessionDurationMs),
      sub: `Max ${durationLabel(kpis.maxSessionDurationMs)}`,
    },
  ]

  kpiGrid.innerHTML = cards
    .map(
      (card) => `
      <article class="kpi-card ${card.tone ? `kpi-${card.tone}` : ""}">
        <p class="kpi-label">${escapeHtml(card.label)}</p>
        <p class="kpi-value">${escapeHtml(card.value)}</p>
        <p class="kpi-sub">${escapeHtml(card.sub)}</p>
      </article>
    `
    )
    .join("")
}

function renderInsights(analysis) {
  const cards = []

  if (analysis.topTool) {
    cards.push({
      tone: "good",
      title: "Dominant Workflow",
      value: labelTool(analysis.topTool.tool),
      text: `${percentLabel(analysis.topToolShare)} of meaningful actions are concentrated here.`,
    })
  }

  if (analysis.topMeaningfulEvent) {
    cards.push({
      tone: "neutral",
      title: "Top Intent Signal",
      value: analysis.topMeaningfulEvent.meta.label,
      text: `${numberLabel(analysis.topMeaningfulEvent.count)} events in selected range.`,
    })
  }

  cards.push({
    tone: "neutral",
    title: "Click Intensity",
    value: `${analysis.clickPerSession.toFixed(2)} / session`,
    text: `${numberLabel(analysis.clickCount)} click interactions captured.`,
  })

  if (analysis.noiseShare > 0.7) {
    cards.push({
      tone: "warn",
      title: "Telemetry Quality Alert",
      value: "High background noise",
      text: "Enable focus mode (system events off) for clearer behavior analysis.",
    })
  } else {
    cards.push({
      tone: "good",
      title: "Telemetry Quality",
      value: "Signal is clear",
      text: `${percentLabel(analysis.activeShare)} of traffic reflects user intent.`,
    })
  }

  insightsGrid.innerHTML = cards
    .map(
      (card) => `
      <article class="insight-card insight-${card.tone}">
        <p class="insight-title">${escapeHtml(card.title)}</p>
        <p class="insight-value">${escapeHtml(card.value)}</p>
        <p class="insight-text">${escapeHtml(card.text)}</p>
      </article>
    `
    )
    .join("")
}

function renderTopActions(actions, totalEvents) {
  const rows = (actions || [])
    .map((item) => {
      const actionKey = String(item.action || "unknown_event")
      const meta = getEventMeta(actionKey)
      return {
        actionKey,
        count: Number(item.count || 0),
        meta,
      }
    })
    .filter((row) => state.includeSystemEvents || !row.meta.passive)
    .sort((a, b) => b.count - a.count)

  if (!rows.length) {
    actionsBody.innerHTML = `<tr><td colspan="5">No action events in this range.</td></tr>`
    return
  }

  actionsBody.innerHTML = rows
    .slice(0, 12)
    .map((row) => {
      const share = totalEvents > 0 ? row.count / totalEvents : 0
      return `
      <tr>
        <td>
          <div class="event-title">${escapeHtml(row.meta.label)}</div>
          <div class="event-detail mono">${escapeHtml(row.actionKey)}</div>
        </td>
        <td>${escapeHtml(row.meta.description)}</td>
        <td><span class="pill ${row.meta.passive ? "pill-muted" : ""}">${escapeHtml(row.meta.category)}</span></td>
        <td>${numberLabel(row.count)}</td>
        <td>${percentLabel(share)}</td>
      </tr>
    `
    })
    .join("")
}

function renderSessions(sessions) {
  const rows = (sessions || []).filter((session) => {
    const activeCount = Number(session.activeEventCount || 0)
    return state.includeSystemEvents || activeCount > 0
  })

  if (!rows.length) {
    sessionsBody.innerHTML = `<tr><td colspan="6">No sessions with meaningful actions in this range.</td></tr>`
    return
  }

  sessionsBody.innerHTML = rows
    .map((session) => {
      const user = session.user && session.user.isAuthenticated
        ? `${session.user.name || "User"}${session.user.email ? ` (${session.user.email})` : ""}`
        : "Anonymous"

      const tools = Array.isArray(session.tools)
        ? session.tools
            .filter((tool) => Boolean(tool))
            .map((tool) => labelTool(tool))
            .join(", ")
        : "-"

      const sessionId = String(session.sessionId || "unknown-session")
      const activeActions = state.includeSystemEvents
        ? Number(session.eventCount || 0)
        : Number(session.activeEventCount || 0)

      return `
        <tr>
          <td title="${escapeHtml(sessionId)}">${escapeHtml(sessionId.slice(0, 16))}…</td>
          <td>${escapeHtml(user)}</td>
          <td>${escapeHtml(durationLabel(session.durationMs))}</td>
          <td>${numberLabel(activeActions)}</td>
          <td>${escapeHtml(tools || "-")}</td>
          <td>${escapeHtml(new Date(session.endedAt).toLocaleString())}</td>
        </tr>
      `
    })
    .join("")
}

function renderRecentEvents(events) {
  const rows = (events || []).filter((event) => {
    const meta = getEventMeta(event.eventType)
    return state.includeSystemEvents || !meta.passive
  })

  if (!rows.length) {
    eventsBody.innerHTML = `<tr><td colspan="6">No user-intent events in this range.</td></tr>`
    return
  }

  eventsBody.innerHTML = rows
    .slice(0, 120)
    .map((event) => {
      const user = event.user && event.user.isAuthenticated
        ? event.user.name || event.user.email || "Authenticated"
        : "Anonymous"

      const sessionId = String(event.sessionId || "unknown-session")
      const meta = getEventMeta(event.eventType)
      const detail = eventDetailText(event, meta)

      return `
        <tr>
          <td>${escapeHtml(new Date(event.eventAt).toLocaleString())}</td>
          <td>
            <div class="event-title">${escapeHtml(meta.label)}</div>
            <div class="event-detail">${escapeHtml(detail)}</div>
          </td>
          <td>${escapeHtml(labelTool(event.tool))}</td>
          <td><span class="pill ${signalPillClass(meta.signal)}">${escapeHtml(meta.signal.toUpperCase())}</span></td>
          <td>${escapeHtml(user)}</td>
          <td title="${escapeHtml(sessionId)}">${escapeHtml(sessionId.slice(0, 14))}…</td>
        </tr>
      `
    })
    .join("")
}

function renderToolChart(tools) {
  const ctx = document.getElementById("toolChart")

  const rows = (tools || [])
    .map((toolRow) => {
      const eventCount = Number(toolRow.eventCount || 0)
      const activeEventCount = Number(toolRow.activeEventCount || eventCount)
      const value = state.includeSystemEvents ? eventCount : activeEventCount

      return {
        label: labelTool(toolRow.tool),
        value,
      }
    })
    .filter((row) => row.value > 0)
    .sort((a, b) => b.value - a.value)
    .slice(0, 10)

  const labels = rows.map((row) => row.label)
  const values = rows.map((row) => row.value)

  if (!toolChart) {
    toolChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: state.includeSystemEvents ? "Telemetry Events" : "Meaningful Actions",
            data: values,
            backgroundColor: "rgba(25, 104, 255, 0.78)",
            borderColor: "rgba(25, 104, 255, 1)",
            borderRadius: 8,
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
          x: { ticks: { maxRotation: 18, minRotation: 18 } },
          y: { beginAtZero: true },
        },
      },
    })
    return
  }

  toolChart.data.labels = labels
  toolChart.data.datasets[0].data = values
  toolChart.data.datasets[0].label = state.includeSystemEvents ? "Telemetry Events" : "Meaningful Actions"
  toolChart.update("none")
}

function renderDailyChart(points) {
  const ctx = document.getElementById("dailyChart")
  const labels = (points || []).map((p) => p.day)
  const events = (points || []).map((p) => p.events)
  const sessions = (points || []).map((p) => p.sessions)

  if (!dailyChart) {
    dailyChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Telemetry Events",
            data: events,
            borderColor: "#1968ff",
            backgroundColor: "rgba(25, 104, 255, 0.16)",
            tension: 0.25,
            fill: true,
          },
          {
            label: "Sessions",
            data: sessions,
            borderColor: "#00a870",
            backgroundColor: "rgba(0, 168, 112, 0.12)",
            tension: 0.25,
            fill: false,
          },
        ],
      },
      options: {
        maintainAspectRatio: false,
        animation: false,
        scales: {
          y: { beginAtZero: true },
        },
      },
    })
    return
  }

  dailyChart.data.labels = labels
  dailyChart.data.datasets[0].data = events
  dailyChart.data.datasets[1].data = sessions
  dailyChart.update("none")
}

function drawHeatmap(heatmap) {
  const ctx = heatmapCanvas.getContext("2d")
  const width = heatmapCanvas.width
  const height = heatmapCanvas.height

  ctx.clearRect(0, 0, width, height)
  ctx.fillStyle = "#f8fbff"
  ctx.fillRect(0, 0, width, height)

  const isCompact = Boolean(heatmap && heatmap.compact)

  if (isCompact) {
    const bins = Array.isArray(heatmap.bins) ? heatmap.bins : []
    const grid = heatmap.grid || { x: 96, y: 24 }
    const maxCount = Number(heatmap.maxCount || 0)
    const totalPoints = Number(heatmap.totalPoints || 0)

    if (!bins.length || maxCount <= 0) {
      ctx.fillStyle = "#5c6f98"
      ctx.font = "14px Manrope"
      ctx.fillText("No click data for this range.", 20, 28)
      heatmapCount.textContent = "0 points"
      return
    }

    const cellWidth = width / Math.max(1, Number(grid.x || 96))
    const cellHeight = height / Math.max(1, Number(grid.y || 24))

    for (const bin of bins) {
      const intensity = Math.min(1, Number(bin.count || 0) / maxCount)
      if (intensity <= 0) continue

      ctx.fillStyle = `rgba(255, 87, 34, ${0.08 + intensity * 0.52})`
      ctx.fillRect(
        Math.floor(Number(bin.x || 0) * cellWidth),
        Math.floor(Number(bin.y || 0) * cellHeight),
        Math.ceil(cellWidth),
        Math.ceil(cellHeight)
      )
    }

    ctx.strokeStyle = "rgba(25, 104, 255, 0.11)"
    for (let x = 0; x <= width; x += 103) {
      ctx.beginPath()
      ctx.moveTo(x, 0)
      ctx.lineTo(x, height)
      ctx.stroke()
    }
    for (let y = 0; y <= height; y += 40) {
      ctx.beginPath()
      ctx.moveTo(0, y)
      ctx.lineTo(width, y)
      ctx.stroke()
    }

    heatmapCount.textContent = `${numberLabel(totalPoints)} points`
    return
  }

  const points = Array.isArray(heatmap && heatmap.points) ? heatmap.points : []
  if (!points.length) {
    ctx.fillStyle = "#5c6f98"
    ctx.font = "14px Manrope"
    ctx.fillText("No click data for this range.", 20, 28)
    heatmapCount.textContent = "0 points"
    return
  }

  const sampled = points.slice(0, 1200)

  sampled.forEach((point) => {
    const px = Number.isFinite(Number(point.normalizedX))
      ? Number(point.normalizedX) * width
      : Number(point.x || 0)
    const py = Number.isFinite(Number(point.normalizedY))
      ? Number(point.normalizedY) * height
      : Number(point.y || 0)

    if (!Number.isFinite(px) || !Number.isFinite(py)) return

    const gradient = ctx.createRadialGradient(px, py, 2, px, py, 24)
    gradient.addColorStop(0, "rgba(255, 87, 34, 0.24)")
    gradient.addColorStop(1, "rgba(255, 87, 34, 0)")

    ctx.fillStyle = gradient
    ctx.beginPath()
    ctx.arc(px, py, 24, 0, Math.PI * 2)
    ctx.fill()
  })

  heatmapCount.textContent = `${numberLabel(points.length)} points`
}

function updateToolFilterOptions(tools) {
  const currentValue = toolFilterEl.value
  const options = ["all"].concat((tools || []).map((t) => t.tool).filter(Boolean))
  const unique = Array.from(new Set(options))

  toolFilterEl.innerHTML = unique
    .map((tool) => {
      const label = tool === "all" ? "All tools" : labelTool(tool)
      return `<option value="${escapeHtml(tool)}">${escapeHtml(label)}</option>`
    })
    .join("")

  if (unique.includes(currentValue)) {
    toolFilterEl.value = currentValue
  } else {
    toolFilterEl.value = "all"
    state.tool = "all"
  }
}

async function loadDashboard(options = {}) {
  const silent = Boolean(options.silent)

  if (activeLoadController) {
    activeLoadController.abort()
  }
  activeLoadController = new AbortController()
  const loadToken = ++currentLoadToken

  const params = getQueryParams()
  params.set("heatmapCompact", "1")
  params.set("heatmapLimit", "12000")
  params.set("heatmapGridX", "96")
  params.set("heatmapGridY", "24")
  params.set("sessionsLimit", "80")
  params.set("eventsLimit", "220")
  const query = params.toString()

  if (!silent) {
    setStatus("Loading dashboard data…")
  }

  const startedAt = performance.now()

  try {
    const dashboard = await fetchJson(`/api/plugin-analytics/dashboard?${query}`, {
      signal: activeLoadController.signal,
    })

    if (loadToken !== currentLoadToken) {
      return
    }

    const summary = dashboard.summary || {}
    const toolUsage = dashboard.toolUsage || {}
    const sessions = dashboard.sessions || {}
    const recentEvents = dashboard.recentEvents || {}
    const analysis = buildDerivedAnalysis(dashboard)

    const nextRenderFingerprint = JSON.stringify({
      includeSystemEvents: state.includeSystemEvents,
      kpis: summary.kpis || {},
      topActions: summary.topActions || [],
      eventTypeBreakdown: dashboard.eventTypeBreakdown || [],
      eventsByDay: summary.eventsByDay || [],
      toolUsage: toolUsage.tools || [],
      heatmap: dashboard.heatmap || {},
      sessions: sessions.sessions || [],
      events: recentEvents.events || [],
    })

    if (silent && nextRenderFingerprint === lastRenderFingerprint) {
      const elapsedMs = Math.round(performance.now() - startedAt)
      setStatus(`Updated ${new Date().toLocaleTimeString()} • no changes • ${elapsedMs}ms`)
      return
    }

    lastRenderFingerprint = nextRenderFingerprint

    renderKpis(summary.kpis || {}, analysis)
    renderInsights(analysis)
    renderTopActions(summary.topActions || [], Number(summary.kpis && summary.kpis.totalEvents))
    renderToolChart(toolUsage.tools || [])
    renderDailyChart(summary.eventsByDay || [])
    drawHeatmap(dashboard.heatmap || {})
    renderSessions(sessions.sessions || [])
    renderRecentEvents(recentEvents.events || [])
    updateToolFilterOptions(toolUsage.tools || [])

    const elapsedMs = Math.round(performance.now() - startedAt)
    setStatus(
      `Updated ${new Date().toLocaleTimeString()} • ${numberLabel(analysis.meaningfulEvents)} meaningful actions • ${elapsedMs}ms`
    )
  } catch (error) {
    if (error && error.name === "AbortError") {
      return
    }
    console.error(error)
    setStatus(`Failed to load dashboard: ${error.message}`, true)
  }
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

function scheduleLoad(delayMs = 180) {
  if (loadDebounceTimer) {
    clearTimeout(loadDebounceTimer)
  }
  loadDebounceTimer = setTimeout(() => {
    loadDebounceTimer = null
    loadDashboard()
  }, delayMs)
}

function bindControls() {
  rangePresetEl.addEventListener("change", () => {
    const preset = rangePresetEl.value
    if (preset !== "custom") {
      applyPreset(preset)
      scheduleLoad(80)
    }
  })

  fromDateEl.addEventListener("change", () => {
    state.from = parseLocalInputValue(fromDateEl.value, state.from)
    rangePresetEl.value = "custom"
    scheduleLoad(80)
  })

  toDateEl.addEventListener("change", () => {
    state.to = parseLocalInputValue(toDateEl.value, state.to)
    rangePresetEl.value = "custom"
    scheduleLoad(80)
  })

  toolFilterEl.addEventListener("change", () => {
    state.tool = toolFilterEl.value
    scheduleLoad(80)
  })

  authFilterEl.addEventListener("change", () => {
    state.auth = authFilterEl.value
    scheduleLoad(80)
  })

  showSystemEventsEl.addEventListener("change", () => {
    state.includeSystemEvents = Boolean(showSystemEventsEl.checked)
    scheduleLoad(20)
  })

  refreshBtn.addEventListener("click", () => loadDashboard())
}

function startRealtimeRefresh() {
  if (realtimeRefreshTimer) {
    clearInterval(realtimeRefreshTimer)
  }

  realtimeRefreshTimer = setInterval(() => {
    if (document.visibilityState !== "visible") return
    loadDashboard({ silent: true })
  }, REALTIME_REFRESH_MS)
}

function init() {
  applyPreset("7d")
  authFilterEl.value = "all"
  showSystemEventsEl.checked = false
  state.includeSystemEvents = false
  bindControls()
  loadDashboard()
  startRealtimeRefresh()
}

init()
