const statusLine = document.getElementById("statusLine")
const refreshBtn = document.getElementById("refreshBtn")

const rangePresetEl = document.getElementById("rangePreset")
const fromDateEl = document.getElementById("fromDate")
const toDateEl = document.getElementById("toDate")
const toolFilterEl = document.getElementById("toolFilter")
const authFilterEl = document.getElementById("authFilter")
const actionFilterEl = document.getElementById("actionFilter")

const featureKpisEl = document.getElementById("featureKpis")
const paletteBody = document.getElementById("paletteBody")
const favoriteBody = document.getElementById("favoriteBody")
const importBucketsEl = document.getElementById("importBuckets")
const exportBucketsEl = document.getElementById("exportBuckets")
const modeTimeCardsEl = document.getElementById("modeTimeCards")
const dpiBreakdownEl = document.getElementById("dpiBreakdown")
const compressionBreakdownEl = document.getElementById("compressionBreakdown")
const colorModeBreakdownEl = document.getElementById("colorModeBreakdown")
const mergeSummaryEl = document.getElementById("mergeSummary")
const mergeDistBody = document.getElementById("mergeDistBody")

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
  eps: "EPS Importer",
  psd: "PSD Importer",
  ai: "AI Importer",
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

function percentLabel(part, total) {
  const base = Number(total || 0)
  if (!base) return "0.0%"
  return `${((Number(part || 0) / base) * 100).toFixed(1)}%`
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

function prettyPaletteLabel(value) {
  const raw = String(value || "")
  if (!raw) return "Unknown"

  if (raw.startsWith("selected:")) {
    return `Selected Option: ${humanizeIdentifier(raw.slice(9))}`
  }
  if (raw.startsWith("scheme:")) {
    return `Scheme: ${humanizeIdentifier(raw.slice(7))}`
  }
  if (raw.startsWith("variation:")) {
    return `Variation: ${humanizeIdentifier(raw.slice(10))}`
  }

  const parts = raw.split(":")
  if (parts.length === 2) {
    return `${humanizeIdentifier(parts[0])}: ${humanizeIdentifier(parts[1])}`
  }

  return humanizeIdentifier(raw)
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

function bucketCardHtml(label, value, meta = "") {
  return `<article class="bucket-card">
    <p class="bucket-label">${escapeHtml(label)}</p>
    <p class="bucket-value">${escapeHtml(numberLabel(value))}</p>
    ${meta ? `<p class="bucket-meta">${escapeHtml(meta)}</p>` : ""}
  </article>`
}

function sumCounts(rows) {
  return (rows || []).reduce((sum, row) => sum + Number(row && row.count ? row.count : 0), 0)
}

function findCount(rows, keyName, keyValue) {
  const row = (rows || []).find((item) => String(item[keyName]) === String(keyValue))
  return Number(row && row.count ? row.count : 0)
}

function renderKpis(features) {
  const kpis = features.kpis || {}
  const colorModeRows = features.colorModeBreakdown || []
  const compressionRows = features.compressionBreakdown || []
  const importRows = features.importSizeBuckets || []
  const exportRows = features.exportSizeBuckets || []

  const passwordTotal = Number(kpis.passwordEnabledRuns || 0) + Number(kpis.passwordDisabledRuns || 0)
  const collapsedMs = Number(kpis.collapsedModeMs || 0)
  const expandedMs = Number(kpis.expandedModeMs || 0)
  const modeTotal = collapsedMs + expandedMs

  const cmykCount = findCount(colorModeRows, "colorMode", "CMYK")
  const rgbCount = findCount(colorModeRows, "colorMode", "RGB")
  const highCompression = findCount(compressionRows, "compression", "high")
  const importLarge =
    findCount(importRows, "bucket", "10-20MB") + findCount(importRows, "bucket", ">20MB")
  const exportLarge =
    findCount(exportRows, "bucket", "10-20MB") + findCount(exportRows, "bucket", ">20MB")

  const cards = [
    {
      label: "Palette Exports",
      value: kpis.paletteExportEvents || 0,
      sub: kpis.topPaletteExport
        ? `Top: ${prettyPaletteLabel(kpis.topPaletteExport.palette)}`
        : "Top: -",
    },
    {
      label: "Favorite Adds",
      value: kpis.favoriteAdds || 0,
      sub: kpis.topFavoritedTool ? `Top: ${labelTool(kpis.topFavoritedTool.tool)}` : "Top: -",
      tone: "good",
    },
    {
      label: "Favorite Removes",
      value: kpis.favoriteRemoves || 0,
      sub: "Removed from favorites",
    },
    {
      label: "Shrink Mode Time",
      value: durationLabel(collapsedMs),
      sub: modeTotal ? percentLabel(collapsedMs, modeTotal) : "0.0%",
    },
    {
      label: "Full Mode Time",
      value: durationLabel(expandedMs),
      sub: modeTotal ? percentLabel(expandedMs, modeTotal) : "0.0%",
    },
    {
      label: "Export Runs",
      value: kpis.exportRuns || 0,
      sub: "PDF export sessions",
      tone: "good",
    },
    {
      label: "Password Protected",
      value: kpis.passwordEnabledRuns || 0,
      sub: passwordTotal ? percentLabel(kpis.passwordEnabledRuns, passwordTotal) : "0.0%",
    },
    {
      label: "CMYK Export Preference",
      value: cmykCount,
      sub: `${percentLabel(cmykCount, cmykCount + rgbCount)} of color-mode exports`,
    },
    {
      label: "High Compression",
      value: highCompression,
      sub: `${percentLabel(highCompression, sumCounts(compressionRows))} of export runs`,
    },
    {
      label: "Merged PDF Groups",
      value: kpis.mergedPdfGroups || 0,
      sub: `${numberLabel(kpis.mergedPagesTotal || 0)} total merged pages`,
      tone: "good",
    },
    {
      label: "Avg Pages / Merge",
      value: Number(kpis.avgPagesPerMerge || 0).toFixed(2),
      sub: `Max ${numberLabel(kpis.maxPagesPerMerge || 0)} pages`,
    },
    {
      label: "Large Files (Import/Export)",
      value: importLarge + exportLarge,
      sub: `${numberLabel(importLarge)} import • ${numberLabel(exportLarge)} export`,
    },
  ]

  featureKpisEl.innerHTML = cards
    .map(
      (card) => `<article class="kpi-card ${card.tone ? `kpi-${card.tone}` : ""}">
        <p class="kpi-label">${escapeHtml(card.label)}</p>
        <p class="kpi-value">${escapeHtml(card.value)}</p>
        <p class="kpi-sub">${escapeHtml(card.sub)}</p>
      </article>`
    )
    .join("")
}

function renderPaletteRows(rows) {
  const data = Array.isArray(rows) ? rows.slice(0, 30) : []
  if (!data.length) {
    paletteBody.innerHTML = `<tr><td colspan="2">No palette export events in selected range.</td></tr>`
    return
  }
  paletteBody.innerHTML = data
    .map((row) => `<tr>
      <td>${escapeHtml(prettyPaletteLabel(row.palette))}</td>
      <td>${numberLabel(row.count)}</td>
    </tr>`)
    .join("")
}

function renderFavoriteRows(rows) {
  const data = Array.isArray(rows) ? rows.slice(0, 30) : []
  if (!data.length) {
    favoriteBody.innerHTML = `<tr><td colspan="2">No favorite add events in selected range.</td></tr>`
    return
  }
  favoriteBody.innerHTML = data
    .map((row) => `<tr>
      <td>${escapeHtml(labelTool(row.tool))}</td>
      <td>${numberLabel(row.count)}</td>
    </tr>`)
    .join("")
}

function renderBucketGrid(targetEl, rows, labelKey, labelFormatter = humanizeIdentifier) {
  const data = Array.isArray(rows) ? rows : []
  if (!data.length) {
    targetEl.innerHTML = bucketCardHtml("No data", 0)
    return
  }

  const total = sumCounts(data)
  targetEl.innerHTML = data
    .map((row) => {
      const label = labelFormatter(row[labelKey])
      const count = Number(row.count || 0)
      const share = percentLabel(count, total)
      return bucketCardHtml(label, count, share)
    })
    .join("")
}

function renderModeTime(modeTime) {
  const collapsedMs = Number(modeTime && modeTime.collapsedMs) || 0
  const expandedMs = Number(modeTime && modeTime.expandedMs) || 0
  const total = collapsedMs + expandedMs

  modeTimeCardsEl.innerHTML =
    bucketCardHtml(
      "Collapsed",
      durationLabel(collapsedMs),
      total ? percentLabel(collapsedMs, total) : "0.0%"
    ) +
    bucketCardHtml(
      "Expanded",
      durationLabel(expandedMs),
      total ? percentLabel(expandedMs, total) : "0.0%"
    )
}

function renderMergeSummary(features) {
  const kpis = features.kpis || {}
  const groups = Number(kpis.mergedPdfGroups || 0)
  const pages = Number(kpis.mergedPagesTotal || 0)

  mergeSummaryEl.innerHTML = `
    <article class="bucket-card">
      <p class="bucket-label">Merged Groups</p>
      <p class="bucket-value">${escapeHtml(numberLabel(groups))}</p>
    </article>
    <article class="bucket-card">
      <p class="bucket-label">Total Merged Pages</p>
      <p class="bucket-value">${escapeHtml(numberLabel(pages))}</p>
    </article>
    <article class="bucket-card">
      <p class="bucket-label">Avg Pages Per Merge</p>
      <p class="bucket-value">${escapeHtml(Number(kpis.avgPagesPerMerge || 0).toFixed(2))}</p>
    </article>
    <article class="bucket-card">
      <p class="bucket-label">Max Pages In One Merge</p>
      <p class="bucket-value">${escapeHtml(numberLabel(kpis.maxPagesPerMerge || 0))}</p>
    </article>
  `

  const dist = Array.isArray(features.mergeDistribution) ? features.mergeDistribution : []
  if (!dist.length) {
    mergeDistBody.innerHTML = `<tr><td colspan="2">No merged PDF distributions available yet.</td></tr>`
    return
  }
  mergeDistBody.innerHTML = dist
    .map((row) => `<tr>
      <td>${escapeHtml(`${row.pages} pages`)}</td>
      <td>${numberLabel(row.count)}</td>
    </tr>`)
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

async function loadFeatures(options = {}) {
  const query = getQueryParams().toString()
  const silent = Boolean(options.silent)
  if (!silent) {
    setStatus("Loading feature intelligence…")
  }
  const startedAt = performance.now()

  try {
    const [features, dashboard] = await Promise.all([
      fetchJson(`/api/plugin-analytics/features?${query}`),
      fetchJson(`/api/plugin-analytics/dashboard?${query}&eventsLimit=1&sessionsLimit=1&heatmapLimit=1`),
    ])

    renderKpis(features)
    renderPaletteRows(features.paletteExports || [])
    renderFavoriteRows(features.favoritedTools || [])
    renderBucketGrid(importBucketsEl, features.importSizeBuckets || [], "bucket", (value) => value)
    renderBucketGrid(exportBucketsEl, features.exportSizeBuckets || [], "bucket", (value) => value)
    renderModeTime(features.modeTime || {})
    renderBucketGrid(dpiBreakdownEl, features.dpiBreakdown || [], "dpi", (value) => `${value} DPI`)
    renderBucketGrid(compressionBreakdownEl, features.compressionBreakdown || [], "compression")
    renderBucketGrid(colorModeBreakdownEl, features.colorModeBreakdown || [], "colorMode")
    renderMergeSummary(features)
    updateToolFilterOptions((dashboard.toolUsage && dashboard.toolUsage.tools) || [])
    updateActionFilterOptions((dashboard.actionCatalog && dashboard.actionCatalog.actions) || [])

    const elapsedMs = Math.round(performance.now() - startedAt)
    setStatus(
      `Updated ${new Date().toLocaleTimeString()} • ${numberLabel(
        (features.kpis && features.kpis.exportRuns) || 0
      )} export runs • ${elapsedMs}ms`
    )
  } catch (error) {
    console.error(error)
    setStatus(`Failed to load feature analytics: ${error.message}`, true)
  }
}

function scheduleLoad(delayMs = 140) {
  if (loadDebounceTimer) clearTimeout(loadDebounceTimer)
  loadDebounceTimer = setTimeout(() => {
    loadDebounceTimer = null
    loadFeatures()
  }, delayMs)
}

function startRealtimeRefresh() {
  if (realtimeTimer) clearInterval(realtimeTimer)
  realtimeTimer = setInterval(() => {
    if (document.visibilityState !== "visible") return
    loadFeatures({ silent: true })
  }, 10000)
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
  refreshBtn.addEventListener("click", () => loadFeatures())
}

function init() {
  applyPreset("7d")
  toolFilterEl.value = "all"
  authFilterEl.value = "all"
  actionFilterEl.value = "all"
  bindControls()
  loadFeatures()
  startRealtimeRefresh()
}

init()
