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
const actionFilterEl = document.getElementById("actionFilter")
const showSystemEventsEl = document.getElementById("showSystemEvents")
const refreshBtn = document.getElementById("refreshBtn")

const PASSIVE_EVENT_TYPES = new Set([
  "session_heartbeat",
  "ui_heartbeat",
  "ui_state_snapshot",
  "ui_visibility_change",
  "ui_resize",
  "analytics_transport_updated",
])

const TOOL_LABELS = {
  dashboard: "Plugin Dashboard",
  "collapsed-dashboard": "Collapsed Dashboard",
  collapsed_dashboard: "Collapsed Dashboard",
  palettable: "Palette Tool",
  palette: "Palette Tool",
  "frame-gallery": "Frame Gallery",
  frame_gallery: "Frame Gallery",
  "import-tool": "Import Tool",
  import_tool: "Import Tool",
  "unit-converter": "Unit Converter",
  unit_converter: "Unit Converter",
  "wayfall-game": "Wayfall Mini Game",
  game: "Wayfall Mini Game",
  "liquid-glass": "Liquid Glass",
  liquid_glass: "Liquid Glass",
  profile: "User Profile",
  unknown: "Unattributed / Legacy",
  unattributed: "Unattributed / Legacy",
  system: "System",
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
    description: "Internal UI-to-main bridge communication.",
    category: "System",
    signal: "low",
  },
  tool_opened: {
    label: "Tool Opened",
    description: "A tool panel was opened by the user.",
    category: "Navigation",
    signal: "high",
  },
  tool_closed: {
    label: "Tool Closed",
    description: "A tool panel was closed by the user.",
    category: "Navigation",
    signal: "medium",
  },
  tool_context_changed: {
    label: "Tool Context Changed",
    description: "User moved from one tool context to another.",
    category: "Navigation",
    signal: "medium",
  },
  tool_action: {
    label: "Tool Action",
    description: "Specific command/action taken inside a tool.",
    category: "Interaction",
    signal: "high",
  },
  tool_time_spent: {
    label: "Tool Time Spent",
    description: "Measured time spent in a specific tool.",
    category: "Engagement",
    signal: "high",
  },
  user_context_changed: {
    label: "User Context Updated",
    description: "User identity context changed in analytics runtime.",
    category: "Identity",
    signal: "medium",
  },
  ui_session_started: {
    label: "UI Session Started",
    description: "Dashboard UI runtime session initialized.",
    category: "Lifecycle",
    signal: "medium",
  },
  ui_click: {
    label: "UI Click",
    description: "User clicked a control in plugin UI.",
    category: "Interaction",
    signal: "high",
  },
  ui_input_changed: {
    label: "Input Changed",
    description: "User changed a field, dropdown, or toggle value.",
    category: "Interaction",
    signal: "high",
  },
  ui_tab_changed: {
    label: "Tab Changed",
    description: "User switched tab/view inside a tool.",
    category: "Navigation",
    signal: "high",
  },
  ui_keyboard_action: {
    label: "Keyboard Action",
    description: "Keyboard-triggered action on an interactive control.",
    category: "Interaction",
    signal: "medium",
  },
  ui_scroll: {
    label: "UI Scroll",
    description: "User scrolled in plugin UI.",
    category: "Interaction",
    signal: "medium",
  },
  ui_resize: {
    label: "UI Resize",
    description: "Plugin UI viewport size changed.",
    category: "System",
    signal: "low",
  },
  ui_visibility_change: {
    label: "UI Visibility Changed",
    description: "Browser/tab visibility status changed.",
    category: "System",
    signal: "low",
  },
  ui_state_snapshot: {
    label: "UI State Snapshot",
    description: "Current open/closed tool panel snapshot.",
    category: "System",
    signal: "low",
  },
  ui_heartbeat: {
    label: "UI Heartbeat",
    description: "Periodic UI health/heartbeat event.",
    category: "System",
    signal: "low",
  },
  ui_before_unload: {
    label: "UI Before Unload",
    description: "UI session is about to unload/close.",
    category: "Lifecycle",
    signal: "low",
  },
  ui_user_authenticated: {
    label: "UI User Authenticated",
    description: "User signed in and auth context became available.",
    category: "Identity",
    signal: "high",
  },
  ui_user_unauthenticated: {
    label: "UI User Unauthenticated",
    description: "User signed out or auth state cleared.",
    category: "Identity",
    signal: "medium",
  },
  analytics_transport_updated: {
    label: "Analytics Transport Updated",
    description: "Analytics endpoint or ingest token was changed.",
    category: "System",
    signal: "low",
  },
  palette_export_performed: {
    label: "Palette Export",
    description: "Palette variation or scheme export was executed.",
    category: "Palette",
    signal: "high",
  },
  tool_favorite_changed: {
    label: "Tool Favorite Changed",
    description: "User added or removed a tool from favorites.",
    category: "Engagement",
    signal: "high",
  },
  importer_favorite_changed: {
    label: "Importer Favorite Changed",
    description: "User updated importer favorites in import tool.",
    category: "Engagement",
    signal: "high",
  },
  import_file_selected: {
    label: "Import File Selected",
    description: "User selected a file for import workflow.",
    category: "Import",
    signal: "high",
  },
  import_conversion_completed: {
    label: "Import Conversion Completed",
    description: "File conversion completed before Figma import.",
    category: "Import",
    signal: "high",
  },
  pdf_export_requested: {
    label: "PDF Export Requested",
    description: "Export started with DPI/compression/password settings.",
    category: "Export",
    signal: "high",
  },
  pdf_merge_group_exported: {
    label: "Merged PDF Created",
    description: "A merged PDF group was generated.",
    category: "Export",
    signal: "high",
  },
  pdf_individual_exported: {
    label: "Individual PDF Created",
    description: "A single frame PDF was generated.",
    category: "Export",
    signal: "high",
  },
  pdf_export_completed: {
    label: "PDF Export Completed",
    description: "ZIP/PDF export pipeline completed successfully.",
    category: "Export",
    signal: "high",
  },
  unknown_event: {
    label: "Unknown Event",
    description: "Event not yet explicitly cataloged.",
    category: "Unmapped",
    signal: "low",
  },
}

const ACTION_METADATA = {
  "toggle-profile": {
    label: "Toggle Profile",
    description: "Open/close profile panel.",
    category: "Navigation",
    signal: "high",
  },
  "toggle-game": {
    label: "Toggle Game",
    description: "Open/close Wayfall game panel.",
    category: "Navigation",
    signal: "medium",
  },
  "toggle-palette": {
    label: "Toggle Palette Tool",
    description: "Open/close palette tool panel.",
    category: "Navigation",
    signal: "high",
  },
  "toggle-frame-gallery": {
    label: "Toggle Frame Gallery",
    description: "Open/close frame gallery panel.",
    category: "Navigation",
    signal: "high",
  },
  "toggle-import-tool": {
    label: "Toggle Import Tool",
    description: "Open/close import tool panel.",
    category: "Navigation",
    signal: "high",
  },
  "toggle-favorite": {
    label: "Toggle Favorite",
    description: "Mark or unmark importer/tool as favorite.",
    category: "Engagement",
    signal: "high",
  },
  "toggle-unit-converter": {
    label: "Toggle Unit Converter",
    description: "Open/close unit converter panel.",
    category: "Navigation",
    signal: "high",
  },
  "toggle-liquid-glass": {
    label: "Toggle Liquid Glass",
    description: "Enable/disable liquid glass preview.",
    category: "Navigation",
    signal: "medium",
  },
  "toggle-collapse": {
    label: "Toggle Dashboard Collapse",
    description: "Collapse/expand dashboard shell.",
    category: "Navigation",
    signal: "medium",
  },
  "resize-expanded": {
    label: "Resize Expanded UI",
    description: "Reset plugin UI into expanded state.",
    category: "Layout",
    signal: "low",
  },
  "tool-collapsed": {
    label: "Collapse Tool View",
    description: "Force tool panel into collapsed dashboard state.",
    category: "Layout",
    signal: "medium",
  },
  "palette-to-collapsed": {
    label: "Palette To Collapsed",
    description: "Close palette and switch to collapsed dashboard.",
    category: "Navigation",
    signal: "medium",
  },
  "resize-window": {
    label: "Resize Window",
    description: "Plugin window resized by UI command.",
    category: "Layout",
    signal: "low",
  },
  "get-ui-state": {
    label: "Request UI State",
    description: "UI requested latest state snapshot from main runtime.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "get-all-frames": {
    label: "Fetch Frames",
    description: "Load current document frames for frame gallery.",
    category: "Frame Workflow",
    signal: "high",
  },
  "export-frames-with-dpi": {
    label: "Export Frames (DPI)",
    description: "Export selected/all frames at requested DPI.",
    category: "Export",
    signal: "high",
  },
  "export-zip-with-password": {
    label: "Export ZIP",
    description: "Start protected ZIP export flow.",
    category: "Export",
    signal: "high",
  },
  "toggle-manual-selection": {
    label: "Toggle Manual Selection",
    description: "Enable/disable manual frame selection mode.",
    category: "Frame Workflow",
    signal: "high",
  },
  "clear-manual-selection": {
    label: "Clear Manual Selection",
    description: "Clear manually selected frame set.",
    category: "Frame Workflow",
    signal: "medium",
  },
  "get-manual-selection-state": {
    label: "Get Manual Selection State",
    description: "Query frame gallery manual selection state.",
    category: "Frame Workflow",
    signal: "low",
  },
  "convert-units": {
    label: "Convert Units",
    description: "Run unit conversion operation.",
    category: "Unit Conversion",
    signal: "high",
  },
  "create-frame": {
    label: "Create Frame",
    description: "Create frame from unit-converter settings.",
    category: "Unit Conversion",
    signal: "high",
  },
  "apply-preset": {
    label: "Apply Preset",
    description: "Apply saved preset to create frame.",
    category: "Unit Conversion",
    signal: "high",
  },
  "save-preset": {
    label: "Save Preset",
    description: "Persist a unit-converter preset.",
    category: "Unit Conversion",
    signal: "medium",
  },
  "delete-preset": {
    label: "Delete Preset",
    description: "Remove a saved unit-converter preset.",
    category: "Unit Conversion",
    signal: "medium",
  },
  "load-presets": {
    label: "Load Presets",
    description: "Retrieve available unit-converter presets.",
    category: "Unit Conversion",
    signal: "low",
  },
  "ui-loaded": {
    label: "UI Loaded",
    description: "Tool UI initialization completed.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "load-liked-presets": {
    label: "Load Liked Presets",
    description: "Fetch liked/favorite presets.",
    category: "Unit Conversion",
    signal: "low",
  },
  "save-liked-presets": {
    label: "Save Liked Presets",
    description: "Persist liked/favorite preset list.",
    category: "Unit Conversion",
    signal: "medium",
  },
  "export-frame": {
    label: "Export Frame",
    description: "Export current selected frame.",
    category: "Export",
    signal: "high",
  },
  "check-font-availability": {
    label: "Check Font Availability",
    description: "Validate required fonts before import.",
    category: "Import",
    signal: "high",
  },
  "import-svg-to-figma": {
    label: "Import SVG",
    description: "Import SVG content into current document.",
    category: "Import",
    signal: "high",
  },
  "export-palette": {
    label: "Export Palette",
    description: "Export generated palette variation.",
    category: "Palette",
    signal: "high",
  },
  "export-color-schemes": {
    label: "Export Color Schemes",
    description: "Export selected color scheme variation.",
    category: "Palette",
    signal: "high",
  },
  "export-selected-options": {
    label: "Export Selected Options",
    description: "Batch export selected palette options.",
    category: "Palette",
    signal: "high",
  },
  "start-eyedropper": {
    label: "Start Eyedropper",
    description: "Activate eyedropper color pick flow.",
    category: "Palette",
    signal: "high",
  },
  notify: {
    label: "Notify",
    description: "Show user notification from UI.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "copy-link": {
    label: "Copy Link",
    description: "Copy generated link to clipboard.",
    category: "Interaction",
    signal: "medium",
  },
  "color-copied": {
    label: "Copy Color Code",
    description: "Copy color value from palette workflow.",
    category: "Palette",
    signal: "high",
  },
  emailEntered: {
    label: "Email Entered",
    description: "User provided email in palette flow.",
    category: "Identity",
    signal: "high",
  },
  "get-font": {
    label: "Get Font",
    description: "Request a specific font for import preprocessing.",
    category: "Import",
    signal: "medium",
  },
  "oauth-token": {
    label: "OAuth Token Received",
    description: "OAuth token was received by plugin.",
    category: "Auth",
    signal: "high",
  },
  "open-auth-url": {
    label: "Open Auth URL",
    description: "Launch external auth URL for login.",
    category: "Auth",
    signal: "high",
  },
  "copy-to-clipboard": {
    label: "Copy To Clipboard",
    description: "Copy value from auth/profile section.",
    category: "Interaction",
    signal: "medium",
  },
  "get-token": {
    label: "Get Token",
    description: "Check if auth token exists.",
    category: "Auth",
    signal: "low",
  },
  "store-token": {
    label: "Store Token",
    description: "Persist token and fetch user profile.",
    category: "Auth",
    signal: "high",
  },
  "clear-token": {
    label: "Clear Token",
    description: "Remove auth token from storage.",
    category: "Auth",
    signal: "medium",
  },
  "get-session": {
    label: "Get Session",
    description: "Read current auth session metadata.",
    category: "Auth",
    signal: "low",
  },
  "store-session": {
    label: "Store Session",
    description: "Persist auth session details.",
    category: "Auth",
    signal: "medium",
  },
  "clear-session": {
    label: "Clear Session",
    description: "Clear auth session data.",
    category: "Auth",
    signal: "medium",
  },
  "store-avatar": {
    label: "Store Avatar",
    description: "Persist selected avatar URL.",
    category: "Profile",
    signal: "low",
  },
  "avatar-selected": {
    label: "Avatar Selected",
    description: "User selected profile avatar.",
    category: "Profile",
    signal: "medium",
  },
  "set-analytics-endpoint": {
    label: "Set Analytics Endpoint",
    description: "Update analytics ingest endpoint/token from UI override.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "analytics-event": {
    label: "Analytics Event Forward",
    description: "Single analytics event passed from UI to main.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "analytics-batch": {
    label: "Analytics Batch Forward",
    description: "Batch analytics events passed from UI to main.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "analytics-identify": {
    label: "Analytics Identify",
    description: "Update analytics identity context from UI.",
    category: "Identity",
    signal: "low",
    passive: true,
  },
  "analytics-flush": {
    label: "Analytics Flush",
    description: "Force flush queued analytics events.",
    category: "System",
    signal: "low",
    passive: true,
  },
  "lg-refresh": {
    label: "Liquid Glass Refresh",
    description: "Refresh liquid glass capture.",
    category: "Liquid Glass",
    signal: "medium",
  },
}

const state = {
  from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
  to: new Date(),
  tool: "all",
  auth: "all",
  action: "all",
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
let latestHeatmapPayload = null
let heatmapResizeObserver = null

if (typeof window !== "undefined" && window.Chart) {
  window.Chart.defaults.color = "#a9b7e5"
  window.Chart.defaults.borderColor = "rgba(159, 183, 255, 0.18)"
}

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

function normalizeKey(value) {
  return String(value || "").trim().toLowerCase()
}

function labelTool(toolId) {
  const key = normalizeKey(toolId)
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

function inferActionMeta(actionKey) {
  const key = String(actionKey || "").trim()
  const normalized = normalizeKey(key)

  if (!key) {
    return {
      key: "unknown_action",
      label: "Unknown Action",
      description: "Action key missing in payload.",
      category: "Unmapped",
      signal: "low",
      passive: false,
    }
  }

  if (EVENT_METADATA[key]) {
    const meta = getEventMeta(key)
    return {
      key,
      ...meta,
    }
  }

  if (ACTION_METADATA[key]) {
    const mapped = ACTION_METADATA[key]
    return {
      key,
      ...mapped,
      passive: Boolean(mapped.passive),
    }
  }

  if (normalized.startsWith("tab:")) {
    const tabId = key.slice(4)
    return {
      key,
      label: `Tab: ${humanizeIdentifier(tabId)}`,
      description: "User switched in-tool tab/view.",
      category: "Navigation",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("click:")) {
    const targetId = key.slice(6)
    return {
      key,
      label: `Click: ${humanizeIdentifier(targetId)}`,
      description: "User clicked an interactive control.",
      category: "Interaction",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("input:")) {
    const targetId = key.slice(6)
    return {
      key,
      label: `Input: ${humanizeIdentifier(targetId)}`,
      description: "User changed an input/dropdown/toggle value.",
      category: "Interaction",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("key:")) {
    const targetId = key.slice(4)
    return {
      key,
      label: `Keyboard: ${humanizeIdentifier(targetId)}`,
      description: "Keyboard-triggered interaction on a control.",
      category: "Interaction",
      signal: "medium",
      passive: false,
    }
  }

  if (normalized.startsWith("palette:export-scheme:")) {
    const scheme = key.split(":").slice(3).join(":")
    return {
      key,
      label: `Export Scheme: ${humanizeIdentifier(scheme)}`,
      description: "Palette scheme exported from palette tool.",
      category: "Palette",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("palette:export-variation:")) {
    const mode = key.split(":").slice(3).join(":")
    return {
      key,
      label: `Export Variation: ${humanizeIdentifier(mode)}`,
      description: "Palette variation exported from palette tool.",
      category: "Palette",
      signal: "high",
      passive: false,
    }
  }

  if (normalized === "palette:export-selected-options") {
    return {
      key,
      label: "Export Selected Palette Options",
      description: "Batch export of selected palette variation/scheme options.",
      category: "Palette",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("favorite:")) {
    const action = normalized.endsWith(":add") ? "Add Favorite" : normalized.endsWith(":remove") ? "Remove Favorite" : "Favorite Updated"
    return {
      key,
      label: action,
      description: "Favorite preference changed in dashboard tools.",
      category: "Engagement",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("importer-favorite:")) {
    const action =
      normalized.endsWith(":add")
        ? "Add Importer Favorite"
        : normalized.endsWith(":remove")
          ? "Remove Importer Favorite"
          : "Importer Favorite Updated"
    return {
      key,
      label: action,
      description: "Favorite preference changed in import tool.",
      category: "Engagement",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("import:conversion:")) {
    const importer = key.split(":").slice(2).join(":")
    return {
      key,
      label: `Import Conversion: ${humanizeIdentifier(importer)}`,
      description: "Pre-import conversion completed for selected file.",
      category: "Import",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("import:file-selected:")) {
    const importer = key.split(":").slice(2).join(":")
    return {
      key,
      label: `Import Selected: ${humanizeIdentifier(importer)}`,
      description: "User selected import file for processing.",
      category: "Import",
      signal: "high",
      passive: false,
    }
  }

  if (normalized.startsWith("export:pdf:")) {
    const phase = key.split(":").slice(2).join(":")
    return {
      key,
      label: `PDF Export ${humanizeIdentifier(phase)}`,
      description: "PDF export workflow lifecycle event.",
      category: "Export",
      signal: "high",
      passive: false,
    }
  }

  let category = "Custom"
  let signal = "medium"
  let passive = false

  if (normalized.startsWith("toggle-") || normalized.includes("collapse")) {
    category = "Navigation"
    signal = "high"
  } else if (normalized.startsWith("get-") || normalized.startsWith("load-")) {
    category = "Read"
    signal = "low"
  } else if (normalized.startsWith("save-") || normalized.startsWith("store-") || normalized.startsWith("delete-") || normalized.startsWith("clear-")) {
    category = "Write"
    signal = "medium"
  } else if (normalized.startsWith("export-")) {
    category = "Export"
    signal = "high"
  } else if (normalized.includes("auth") || normalized.includes("token") || normalized.includes("session")) {
    category = "Auth"
    signal = "medium"
  } else if (normalized.startsWith("analytics-")) {
    category = "System"
    signal = "low"
    passive = true
  }

  return {
    key,
    label: humanizeIdentifier(key),
    description: "Action observed from plugin message handling.",
    category,
    signal,
    passive,
  }
}

function getActionMeta(actionKey) {
  return inferActionMeta(actionKey)
}

function resolveEventAction(event) {
  const payload = event && event.payload && typeof event.payload === "object" ? event.payload : {}
  return String(
    payload.action ||
      payload.messageType ||
      payload.interactionAction ||
      payload.type ||
      event.eventType ||
      "unknown_event"
  ).trim()
}

function eventIsPassive(event) {
  const eventMeta = getEventMeta(event && event.eventType)
  if (eventMeta.passive) return true
  const actionMeta = getActionMeta(resolveEventAction(event))
  return Boolean(actionMeta.passive)
}

function signalPillClass(signal) {
  if (signal === "high") return "pill-signal-high"
  if (signal === "medium") return "pill-signal-medium"
  return "pill-signal-low"
}

function labelUser(user) {
  const u = user && typeof user === "object" ? user : null
  if (u && u.isAuthenticated) {
    return u.name || u.email || u.userId || "Authenticated"
  }

  const anonymousId = u && u.anonymousId ? String(u.anonymousId) : ""
  const identitySource = u && u.identitySource ? String(u.identitySource) : ""
  if (anonymousId) {
    const suffix = anonymousId.slice(-8)
    const sourceTag = identitySource.includes("figma")
      ? "Figma"
      : identitySource.includes("device")
      ? "Device"
      : ""
    return sourceTag ? `Anon ${suffix} (${sourceTag})` : `Anon ${suffix}`
  }

  return "Anonymous"
}

function eventDetailText(event, meta) {
  const payload = event && event.payload && typeof event.payload === "object" ? event.payload : {}
  const action = resolveEventAction(event)

  if (meta.key === "tool_time_spent") {
    return `Time tracked: ${durationLabel(payload.durationMs)} in ${labelTool(event.tool)}`
  }

  if (meta.key === "palette_export_performed") {
    const exportType = payload.exportType ? humanizeIdentifier(payload.exportType) : "Palette"
    const mode = payload.mode ? ` (${humanizeIdentifier(payload.mode)})` : ""
    const count = Number(payload.colorCount || payload.optionCount || 0)
    return `${exportType} export${mode}${count > 0 ? ` • ${count} values` : ""}`
  }

  if (meta.key === "tool_favorite_changed" || meta.key === "importer_favorite_changed") {
    const target = payload.toolLabel || payload.toolId || payload.importerId || "tool"
    const state = payload.isFavorited === true ? "added to favorites" : "removed from favorites"
    return `${humanizeIdentifier(target)} ${state}`
  }

  if (meta.key === "import_file_selected") {
    const importer = payload.importer ? humanizeIdentifier(payload.importer) : "Importer"
    const bucket = payload.fileSizeBucket ? ` • ${payload.fileSizeBucket}` : ""
    return `${importer} file selected${bucket}`
  }

  if (meta.key === "import_conversion_completed") {
    const importer = payload.importer ? humanizeIdentifier(payload.importer) : "Importer"
    const bucket = payload.outputSizeBucket ? ` • ${payload.outputSizeBucket}` : ""
    return `${importer} conversion completed${bucket}`
  }

  if (meta.key === "pdf_export_requested") {
    const dpi = Number(payload.dpi || 0)
    const compression = payload.compression ? humanizeIdentifier(payload.compression) : "Unknown"
    const colorMode = payload.colorMode ? String(payload.colorMode).toUpperCase() : "Unknown"
    const passwordState = payload.passwordEnabled ? "password protected" : "no password"
    return `Export requested • ${dpi || "?"} DPI • ${compression} • ${colorMode} • ${passwordState}`
  }

  if (meta.key === "pdf_merge_group_exported") {
    const pages = Number(payload.pageCount || 0)
    const name = payload.groupName ? humanizeIdentifier(payload.groupName) : "Merged group"
    return `${name} exported • ${pages} pages`
  }

  if (meta.key === "pdf_individual_exported") {
    const frameName = payload.frameName ? humanizeIdentifier(payload.frameName) : "Frame"
    return `${frameName} exported as individual PDF`
  }

  if (meta.key === "pdf_export_completed") {
    const count = Number(payload.outputPdfCount || 0)
    const zipBucket = payload.zipSizeBucket || payload.totalPdfSizeBucket || "unknown"
    return `Export completed • ${count} PDFs • ${zipBucket} output`
  }

  if (meta.key === "session_heartbeat") {
    return `Session active in ${labelTool(payload.activeTool || event.tool)}`
  }

  if (meta.key === "ui_click") {
    const element = payload.element && typeof payload.element === "object" ? payload.element : {}
    const target = payload.actionLabel || element.id || element.toolId || element.tag || "UI control"
    return `Clicked: ${humanizeIdentifier(target)}`
  }

  if (meta.key === "ui_tab_changed") {
    const fromTab = payload.fromTab ? humanizeIdentifier(payload.fromTab) : "Unknown"
    const toTab = payload.toTab ? humanizeIdentifier(payload.toTab) : "Unknown"
    return `Tab switch: ${fromTab} -> ${toTab}`
  }

  if (meta.key === "ui_input_changed") {
    const input = payload.input && typeof payload.input === "object" ? payload.input : {}
    const target = payload.actionLabel || "Input"
    if (typeof input.valueLength === "number") {
      return `${humanizeIdentifier(target)} updated (length ${input.valueLength})`
    }
    if (typeof input.checked === "boolean") {
      return `${humanizeIdentifier(target)} set to ${input.checked ? "ON" : "OFF"}`
    }
    return `${humanizeIdentifier(target)} changed`
  }

  if (meta.key === "ui_keyboard_action") {
    const keyName = payload.key || "Key"
    const target = payload.actionLabel || "control"
    return `Keyboard ${keyName}: ${humanizeIdentifier(target)}`
  }

  if (meta.key === "tool_action") {
    const actionMeta = getActionMeta(action)
    return `${actionMeta.label}${payload.tool ? ` on ${labelTool(payload.tool)}` : ""}`
  }

  if (meta.key === "tool_opened" || meta.key === "tool_closed" || meta.key === "tool_context_changed") {
    const toolFromPayload = payload.tool || payload.uiTool || event.tool
    return `${meta.label} for ${labelTool(toolFromPayload)}`
  }

  if (meta.key === "plugin_message") {
    const messageType = payload.messageType || payload.type || payload.action
    if (messageType) {
      const actionMeta = getActionMeta(String(messageType))
      if (payload.handled === false) {
        return `Unhandled message: ${actionMeta.label}`
      }
      return `Message: ${actionMeta.label}`
    }
    return "Internal bridge message between UI and plugin runtime"
  }

  if (meta.key === "user_context_changed") {
    const identitySource = payload.user && payload.user.identitySource
      ? String(payload.user.identitySource)
      : ""
    return identitySource
      ? `Identity context updated via ${humanizeIdentifier(identitySource)}`
      : "Identity context updated"
  }

  if (action && action !== meta.key && action !== event.eventType) {
    const actionMeta = getActionMeta(action)
    return `Action: ${actionMeta.label}`
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
      userCount: Number(toolRow.userCount || 0),
      authenticatedUserCount: Number(toolRow.authenticatedUserCount || 0),
      anonymousUserCount: Number(toolRow.anonymousUserCount || 0),
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
  const activeToolCount = sortedTools.filter((toolRow) => toolRow.activeEventCount > 0).length
  const authenticatedUsers = Number(kpis.authenticatedUsers || 0)
  const anonymousUsers = Number(kpis.anonymousUsers || 0)
  const totalUsers = authenticatedUsers + anonymousUsers
  const authenticatedShare = totalUsers > 0 ? authenticatedUsers / totalUsers : 0

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
    activeToolCount,
    authenticatedUsers,
    anonymousUsers,
    authenticatedShare,
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

  if (state.action && state.action !== "all") {
    params.set("action", state.action)
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
      sub: `${numberLabel(kpis.authenticatedUsers)} auth • ${numberLabel(kpis.anonymousUsers)} identified anonymous`,
    },
    {
      label: "Active Tools",
      value: numberLabel(analysis.activeToolCount),
      sub: analysis.topTool ? `Top: ${labelTool(analysis.topTool.tool)}` : "Top: -",
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
    {
      label: "Clicks",
      value: numberLabel(analysis.clickCount),
      sub: `${analysis.clickPerSession.toFixed(2)} per session`,
    },
    {
      label: "Authenticated Share",
      value: percentLabel(analysis.authenticatedShare),
      sub: `${numberLabel(analysis.authenticatedUsers)} authenticated users`,
      tone: analysis.authenticatedShare > 0.2 ? "good" : "neutral",
    },
    {
      label: "Top Tool Concentration",
      value: percentLabel(analysis.topToolShare),
      sub: "Meaningful actions from #1 workflow",
      tone: analysis.topToolShare > 0.6 ? "warn" : "neutral",
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

  cards.push({
    tone: analysis.anonymousUsers > 0 ? "good" : "warn",
    title: "Anonymous Identification",
    value: analysis.anonymousUsers > 0 ? `${numberLabel(analysis.anonymousUsers)} identified users` : "No anon identifiers yet",
    text: "Anonymous users are shown as pseudonymous IDs when available from runtime context.",
  })

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
      const meta = getActionMeta(actionKey)
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
    .slice(0, 18)
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
      const user = labelUser(session.user)

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
  const rows = (events || []).filter((event) => state.includeSystemEvents || !eventIsPassive(event))

  if (!rows.length) {
    eventsBody.innerHTML = `<tr><td colspan="6">No user-intent events in this range.</td></tr>`
    return
  }

  eventsBody.innerHTML = rows
    .slice(0, 120)
    .map((event) => {
      const user = labelUser(event.user)
      const sessionId = String(event.sessionId || "unknown-session")
      const meta = getEventMeta(event.eventType)
      const detail = eventDetailText(event, meta)
      const actionMeta = getActionMeta(resolveEventAction(event))

      return `
        <tr>
          <td>${escapeHtml(new Date(event.eventAt).toLocaleString())}</td>
          <td>
            <div class="event-title">${escapeHtml(meta.label)}</div>
            <div class="event-detail">${escapeHtml(detail)}</div>
            <div class="event-detail mono">${escapeHtml(actionMeta.label)}</div>
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
          x: {
            ticks: { maxRotation: 18, minRotation: 18, color: "#9db0e7" },
            grid: { color: "rgba(159, 183, 255, 0.12)" },
          },
          y: {
            beginAtZero: true,
            ticks: { color: "#9db0e7" },
            grid: { color: "rgba(159, 183, 255, 0.12)" },
          },
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
        plugins: {
          legend: {
            labels: { color: "#a9b7e5" },
          },
        },
        scales: {
          x: {
            ticks: { color: "#9db0e7" },
            grid: { color: "rgba(159, 183, 255, 0.08)" },
          },
          y: {
            beginAtZero: true,
            ticks: { color: "#9db0e7" },
            grid: { color: "rgba(159, 183, 255, 0.12)" },
          },
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

function ensureHeatmapCanvasSize(minHeight = 260) {
  const parent = heatmapCanvas && heatmapCanvas.parentElement
  const rect = parent ? parent.getBoundingClientRect() : null
  const cssWidth = Math.max(220, Math.floor((rect && rect.width) || heatmapCanvas.clientWidth || 220))
  const cssHeight = Math.max(
    minHeight,
    Math.floor((rect && rect.height) || heatmapCanvas.clientHeight || minHeight)
  )
  const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1))
  const nextWidth = Math.floor(cssWidth * dpr)
  const nextHeight = Math.floor(cssHeight * dpr)

  if (heatmapCanvas.width !== nextWidth || heatmapCanvas.height !== nextHeight) {
    heatmapCanvas.width = nextWidth
    heatmapCanvas.height = nextHeight
  }
  heatmapCanvas.style.width = `${cssWidth}px`
  heatmapCanvas.style.height = `${cssHeight}px`

  const ctx = heatmapCanvas.getContext("2d")
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
  return { ctx, width: cssWidth, height: cssHeight }
}

function drawHeatmap(heatmap) {
  latestHeatmapPayload = heatmap || {}
  const { ctx, width, height } = ensureHeatmapCanvasSize(260)

  ctx.clearRect(0, 0, width, height)
  ctx.fillStyle = "#0d1428"
  ctx.fillRect(0, 0, width, height)

  const isCompact = Boolean(heatmap && heatmap.compact)

  if (isCompact) {
    const bins = Array.isArray(heatmap.bins) ? heatmap.bins : []
    const grid = heatmap.grid || { x: 96, y: 24 }
    const maxCount = Number(heatmap.maxCount || 0)
    const totalPoints = Number(heatmap.totalPoints || 0)

    if (!bins.length || maxCount <= 0) {
      ctx.fillStyle = "#9aa8d4"
      ctx.font = "500 13px 'Space Grotesk'"
      ctx.fillText("No click data for this range.", 18, 26)
      heatmapCount.textContent = "0 points"
      return
    }

    const cellWidth = width / Math.max(1, Number(grid.x || 96))
    const cellHeight = height / Math.max(1, Number(grid.y || 24))

    for (const bin of bins) {
      const intensity = Math.min(1, Number(bin.count || 0) / maxCount)
      if (intensity <= 0) continue
      const hue = 34 - intensity * 26
      const alpha = 0.1 + intensity * 0.66
      ctx.fillStyle = `hsla(${hue}, 100%, 58%, ${alpha})`
      ctx.fillRect(
        Math.floor(Number(bin.x || 0) * cellWidth),
        Math.floor(Number(bin.y || 0) * cellHeight),
        Math.ceil(cellWidth),
        Math.ceil(cellHeight)
      )
    }

    ctx.strokeStyle = "rgba(159, 183, 255, 0.14)"
    ctx.lineWidth = 1
    const columns = Math.max(8, Math.min(20, Math.floor(width / 72)))
    const rows = Math.max(4, Math.min(12, Math.floor(height / 36)))
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

    heatmapCount.textContent = `${numberLabel(totalPoints)} points`
    return
  }

  const points = Array.isArray(heatmap && heatmap.points) ? heatmap.points : []
  if (!points.length) {
    ctx.fillStyle = "#9aa8d4"
    ctx.font = "500 13px 'Space Grotesk'"
    ctx.fillText("No click data for this range.", 18, 26)
    heatmapCount.textContent = "0 points"
    return
  }

  const sampled = points.slice(0, 1400)

  sampled.forEach((point) => {
    const px = Number.isFinite(Number(point.normalizedX))
      ? Number(point.normalizedX) * width
      : Number(point.x || 0)
    const py = Number.isFinite(Number(point.normalizedY))
      ? Number(point.normalizedY) * height
      : Number(point.y || 0)

    if (!Number.isFinite(px) || !Number.isFinite(py)) return

    const gradient = ctx.createRadialGradient(px, py, 1.5, px, py, 22)
    gradient.addColorStop(0, "rgba(255, 208, 92, 0.24)")
    gradient.addColorStop(0.65, "rgba(255, 112, 64, 0.2)")
    gradient.addColorStop(1, "rgba(255, 112, 64, 0)")
    ctx.fillStyle = gradient
    ctx.beginPath()
    ctx.arc(px, py, 22, 0, Math.PI * 2)
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

function updateActionFilterOptions(catalog, fallbackActions = []) {
  const currentValue = actionFilterEl.value || state.action || "all"
  const preferredActions = Array.isArray(catalog) ? catalog : []
  const fallback = (fallbackActions || []).map((row) => ({
    action: row && row.action,
    count: row && row.count,
  }))

  const merged = preferredActions.length ? preferredActions : fallback
  const options = [{ action: "all", count: null }]
  const seen = new Set(["all"])

  for (const row of merged) {
    const action = String((row && row.action) || "").trim()
    if (!action || seen.has(action)) continue
    const meta = getActionMeta(action)
    if (!state.includeSystemEvents && meta.passive) continue
    seen.add(action)
    options.push({
      action,
      count: Number((row && row.count) || 0),
    })
  }

  actionFilterEl.innerHTML = options
    .map((row) => {
      if (row.action === "all") {
        return `<option value="all">All actions</option>`
      }
      const meta = getActionMeta(row.action)
      const label = `${meta.label}${row.count ? ` (${numberLabel(row.count)})` : ""}`
      return `<option value="${escapeHtml(row.action)}">${escapeHtml(label)}</option>`
    })
    .join("")

  if (seen.has(currentValue)) {
    actionFilterEl.value = currentValue
  } else {
    actionFilterEl.value = "all"
    state.action = "all"
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
  params.set("eventsLimit", "140")
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
    const actionCatalog = dashboard.actionCatalog || {}
    const analysis = buildDerivedAnalysis(dashboard)

    const nextRenderFingerprint = JSON.stringify({
      includeSystemEvents: state.includeSystemEvents,
      action: state.action,
      kpis: summary.kpis || {},
      topActions: summary.topActions || [],
      actionCatalog: actionCatalog.actions || [],
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
    updateActionFilterOptions(actionCatalog.actions || [], summary.topActions || [])

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

  actionFilterEl.addEventListener("change", () => {
    state.action = actionFilterEl.value
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

function bindHeatmapResize() {
  if (!heatmapCanvas) return
  if (heatmapResizeObserver) {
    heatmapResizeObserver.disconnect()
    heatmapResizeObserver = null
  }

  const redraw = () => {
    if (!latestHeatmapPayload) return
    drawHeatmap(latestHeatmapPayload)
  }

  if (typeof ResizeObserver !== "undefined") {
    heatmapResizeObserver = new ResizeObserver(() => {
      redraw()
    })
    if (heatmapCanvas.parentElement) {
      heatmapResizeObserver.observe(heatmapCanvas.parentElement)
    }
  }

  window.addEventListener("resize", redraw)
}

function init() {
  applyPreset("7d")
  authFilterEl.value = "all"
  actionFilterEl.value = "all"
  state.action = "all"
  showSystemEventsEl.checked = false
  state.includeSystemEvents = false
  bindControls()
  bindHeatmapResize()
  loadDashboard()
  startRealtimeRefresh()
}

init()
