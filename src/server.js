import path from "path";
import { fileURLToPath } from "url";

import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import helmet from "helmet";
import morgan from "morgan";

import {
  getBackendCreditLedgerCollection,
  ensureIndexes,
  getBackendUserBillingCollection,
  getBackendUsersCollection,
  getEventsCollection,
  getEngagementCollection,
  getSnapshotsCollection,
} from "./db.js";

dotenv.config();

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, "..", "public");

const PORT = Number(process.env.PORT || 4080);
const INGEST_TOKEN = (process.env.ANALYTICS_INGEST_TOKEN || "").trim();
const INGEST_TOKEN_REQUIRED =
  String(process.env.ANALYTICS_INGEST_TOKEN_REQUIRED || "")
    .trim()
    .toLowerCase() === "true";
const READ_USER = (process.env.DASHBOARD_BASIC_AUTH_USER || "").trim();
const READ_PASS = (process.env.DASHBOARD_BASIC_AUTH_PASS || "").trim();
let initializationPromise = null;
let isInitialized = false;
const PASSIVE_EVENT_TYPES = [
  "plugin_session_started",
  "plugin_session_ended",
  "session_heartbeat",
  "plugin_message",
  "backend_operation",
  "auth_lifecycle",
  "user_context_changed",
  "tool_context_changed",
  "tool_time_spent",
  "ui_session_started",
  "ui_heartbeat",
  "ui_state_snapshot",
  "ui_visibility_change",
  "ui_resize",
  "ui_before_unload",
  "analytics_transport_updated",
];
const PASSIVE_ACTION_KEYS = [
  "session_heartbeat",
  "ui_session_started",
  "ui_heartbeat",
  "ui_state_snapshot",
  "ui_visibility_change",
  "ui_resize",
  "ui_before_unload",
  "backend_operation",
  "plugin_session_started",
  "plugin_session_ended",
  "user_context_changed",
  "tool_context_changed",
  "tool_time_spent",
  "get-ui-state",
  "get-all-frames",
  "get-manual-selection-state",
  "get-token",
  "get-session",
  "get-font",
  "notify",
  "ui-loaded",
  "load-presets",
  "load-liked-presets",
  "load-comments",
  "set-analytics-endpoint",
  "analytics-event",
  "analytics-batch",
  "analytics-identify",
  "analytics-flush",
  "check-session-start",
  "cached-user-restored",
  "cached-user-token-valid",
  "cached-user-network-fallback",
  "token-expired",
  "token-validated",
  "session-poll-restored-token",
  "refresh-requested",
  "refresh-succeeded",
  "refresh-failed",
  "background-validate-start",
  "background-validate-succeeded",
  "background-validate-auth-error",
  "background-validate-refresh-failed",
  "auth-data-cleared",
  "start-auth-no-active-session",
  "token-invalid-refresh-attempt",
  "token-refresh-recovered",
];
const PASSIVE_EVENT_TYPE_SET = new Set(PASSIVE_EVENT_TYPES);
const PASSIVE_ACTION_KEY_SET = new Set(PASSIVE_ACTION_KEYS);
const MAX_EVENT_TYPE_BREAKDOWN = 40;

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json({ limit: "5mb" }));
app.use(morgan("tiny"));

function parseDate(value, fallback) {
  if (!value) return fallback;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? fallback : d;
}

function parseDateRange(query) {
  const now = new Date();
  const defaultFrom = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const from = parseDate(query.from, defaultFrom);
  const to = parseDate(query.to, now);

  if (from > to) {
    return { from: to, to: from };
  }
  return { from, to };
}

function parseLimit(value, fallback, max = 200) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}

function parseBool(value, fallback = false) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function safeString(value, maxLen = 180) {
  if (value === null || value === undefined) return null;
  const text = String(value);
  return text.length <= maxLen ? text : `${text.slice(0, maxLen)}…`;
}

function toDate(value, fallbackDate) {
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? fallbackDate : d;
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

const RUNOUT_CREDIT_THRESHOLD = Math.max(
  0,
  Number(process.env.RUNOUT_CREDIT_THRESHOLD || 50) || 50
);
const RUNOUT_CREDIT_DAYS = Math.max(
  1,
  Number(process.env.RUNOUT_CREDIT_DAYS || 14) || 14
);
const CREDIT_NEWSLETTER_FROM = safeString(process.env.CREDIT_NEWSLETTER_FROM, 240);
const CREDIT_NEWSLETTER_TO = safeString(process.env.CREDIT_NEWSLETTER_TO, 1000);
const RESEND_API_KEY = safeString(process.env.RESEND_API_KEY, 240);

function addMatchClause(match, clause) {
  if (!clause || typeof clause !== "object") return;
  if (!match.$and) {
    match.$and = [clause];
    return;
  }
  match.$and.push(clause);
}

function parseActionFilter(value) {
  const raw = safeString(value, 500);
  if (!raw || raw === "all") return [];
  const actions = raw
    .split(",")
    .map((item) => safeString(item, 120))
    .filter(Boolean)
    .map((item) => item.trim())
    .filter(Boolean);
  return Array.from(new Set(actions)).slice(0, 30);
}

function actionProjectionExpression() {
  return {
    $ifNull: [
      "$payload.action",
      {
        $ifNull: [
          "$payload.messageType",
          {
            $ifNull: [
              "$payload.interactionAction",
              {
                $ifNull: ["$payload.type", "$eventType"],
              },
            ],
          },
        ],
      },
    ],
  };
}

function passiveEventExpression() {
  const actionExpr = actionProjectionExpression();
  return {
    $or: [
      { $in: ["$eventType", PASSIVE_EVENT_TYPES] },
      { $in: [actionExpr, PASSIVE_ACTION_KEYS] },
      {
        $regexMatch: {
          input: { $toLower: { $ifNull: [actionExpr, ""] } },
          regex: "^analytics-",
        },
      },
    ],
  };
}

function stableHashId(input) {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash +=
      (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return Math.abs(hash >>> 0).toString(36);
}

const TOOL_ALIASES = {
  dashboard: "dashboard",
  "collapsed-dashboard": "collapsed-dashboard",
  collapsed_dashboard: "collapsed-dashboard",
  palettable: "palettable",
  palette: "palettable",
  "palette-tool": "palettable",
  "frame-gallery": "frame-gallery",
  frame_gallery: "frame-gallery",
  "frames-to-pdf": "frame-gallery",
  frames_to_pdf: "frame-gallery",
  "import-tool": "import-tool",
  import_tool: "import-tool",
  "unit-converter": "unit-converter",
  unit_converter: "unit-converter",
  "html-to-design": "html-to-design",
  html_to_design: "html-to-design",
  "comment-summarizer": "comment-summarizer",
  comment_summarizer: "comment-summarizer",
  profile: "profile",
  "wayfall-game": "wayfall-game",
  game: "wayfall-game",
  "liquid-glass": "liquid-glass",
  liquid_glass: "liquid-glass",
  ai: "ai",
  eps: "eps",
  pdf: "pdf",
  psd: "psd",
  unknown: "unknown",
};

const INTERNAL_TOOL_SET = new Set([
  "dashboard",
  "collapsed-dashboard",
  "profile",
  "unknown",
  "system",
]);

const TOOL_LABELS = {
  dashboard: "Dashboard",
  "collapsed-dashboard": "Collapsed Dashboard",
  palettable: "Palette Tool",
  "frame-gallery": "Frames to PDF",
  "import-tool": "Import Tool",
  "unit-converter": "Unit Converter",
  "html-to-design": "HTML to Design",
  "comment-summarizer": "Comment Summarizer",
  profile: "Profile",
  "wayfall-game": "Wayfall Game",
  "liquid-glass": "Liquid Glass",
  ai: "AI Import",
  eps: "EPS Import",
  pdf: "PDF Import",
  psd: "PSD Import",
  unknown: "Unknown",
  system: "System",
};

const ACTION_TO_TOOL = {
  "toggle-palette": "palettable",
  "export-palette": "palettable",
  "export-color-schemes": "palettable",
  "export-selected-options": "palettable",
  "start-eyedropper": "palettable",
  "color-copied": "palettable",
  "copy-link": "palettable",
  notify: "palettable",
  emailentered: "palettable",
  "toggle-frame-gallery": "frame-gallery",
  "toggle-manual-selection": "frame-gallery",
  "clear-manual-selection": "frame-gallery",
  "get-manual-selection-state": "frame-gallery",
  "get-all-frames": "frame-gallery",
  "export-frames-with-dpi": "frame-gallery",
  "export-zip-with-password": "frame-gallery",
  "toggle-import-tool": "import-tool",
  "toggle-favorite": "import-tool",
  "import-svg-to-figma": "import-tool",
  "check-font-availability": "import-tool",
  "get-font": "import-tool",
  "toggle-unit-converter": "unit-converter",
  "apply-preset": "unit-converter",
  "create-frame": "unit-converter",
  "save-preset": "unit-converter",
  "delete-preset": "unit-converter",
  "load-presets": "unit-converter",
  "load-liked-presets": "unit-converter",
  "save-liked-presets": "unit-converter",
  "convert-units": "unit-converter",
  "export-frame": "unit-converter",
  "toggle-html-to-design": "html-to-design",
  "import-html-design-layers": "html-to-design",
  "toggle-comment-summarizer": "comment-summarizer",
  "open-comment-summary": "comment-summarizer",
  "load-comments": "comment-summarizer",
  "run-summarization": "comment-summarizer",
  "start-new-summary": "comment-summarizer",
  "show-comment-summarizer-tutorial": "comment-summarizer",
  "toggle-profile": "profile",
  "avatar-selected": "profile",
  "store-avatar": "profile",
  "oauth-token": "profile",
  "store-token": "profile",
  "clear-token": "profile",
  "open-auth-url": "profile",
  "copy-to-clipboard": "profile",
  "get-token": "profile",
  "get-session": "profile",
  "store-session": "profile",
  "clear-session": "profile",
  "toggle-game": "wayfall-game",
  "toggle-liquid-glass": "liquid-glass",
  "lg-refresh": "liquid-glass",
  "toggle-collapse": "collapsed-dashboard",
  "tool-collapsed": "collapsed-dashboard",
  "palette-to-collapsed": "collapsed-dashboard",
  "resize-expanded": "dashboard",
  "resize-window": "dashboard",
  "get-ui-state": "dashboard",
  "analytics-event": "dashboard",
  "analytics-batch": "dashboard",
  "analytics-identify": "dashboard",
  "analytics-flush": "dashboard",
  "set-analytics-endpoint": "dashboard",
};

const FEATURE_EVENT_DEFINITIONS = [
  {
    key: "palette_export_performed",
    label: "Palette Export Performed",
    category: "Palette",
    tool: "palettable",
    source: "feature-event",
  },
  {
    key: "tool_favorite_changed",
    label: "Tool Favorite Changed",
    category: "Engagement",
    tool: "dashboard",
    source: "feature-event",
  },
  {
    key: "import_file_selected",
    label: "Import File Selected",
    category: "Import",
    tool: "import-tool",
    source: "feature-event",
  },
  {
    key: "import_conversion_completed",
    label: "Import Conversion Completed",
    category: "Import",
    tool: "import-tool",
    source: "feature-event",
  },
  {
    key: "pdf_export_requested",
    label: "PDF Export Requested",
    category: "Export",
    tool: "frame-gallery",
    source: "feature-event",
  },
  {
    key: "pdf_merge_group_exported",
    label: "Merged PDF Group Exported",
    category: "Export",
    tool: "frame-gallery",
    source: "feature-event",
  },
  {
    key: "pdf_individual_exported",
    label: "Individual PDF Exported",
    category: "Export",
    tool: "frame-gallery",
    source: "feature-event",
  },
  {
    key: "pdf_export_completed",
    label: "PDF Export Completed",
    category: "Export",
    tool: "frame-gallery",
    source: "feature-event",
  },
  {
    key: "ui_tab_changed",
    label: "In-Tool Tab Changed",
    category: "Interaction",
    tool: "dashboard",
    source: "interaction-event",
  },
  {
    key: "ui_input_changed",
    label: "Input Changed",
    category: "Interaction",
    tool: "dashboard",
    source: "interaction-event",
  },
  {
    key: "ui_click",
    label: "UI Click",
    category: "Interaction",
    tool: "dashboard",
    source: "interaction-event",
  },
  {
    key: "ui_keyboard_action",
    label: "Keyboard Action",
    category: "Interaction",
    tool: "dashboard",
    source: "interaction-event",
  },
];

const ACTION_LABEL_OVERRIDES = {
  "toggle-palette": "Toggle Palette Tool",
  "toggle-frame-gallery": "Toggle Frames to PDF",
  "toggle-import-tool": "Toggle Import Tool",
  "toggle-unit-converter": "Toggle Unit Converter",
  "toggle-html-to-design": "Toggle HTML to Design",
  "import-html-design-layers": "Import HTML Design Layers",
  "toggle-comment-summarizer": "Toggle Comment Summarizer",
  "open-comment-summary": "Open Comment Summary",
  "load-comments": "Load Comments",
  "run-summarization": "Run Summarization",
  "start-new-summary": "Start New Summary",
  "show-comment-summarizer-tutorial": "Show Comment Summarizer Tutorial",
  "toggle-profile": "Toggle Profile",
  "toggle-game": "Toggle Wayfall Game",
  "toggle-liquid-glass": "Toggle Liquid Glass",
  "toggle-manual-selection": "Toggle Manual Selection",
  "clear-manual-selection": "Clear Manual Selection",
  "get-manual-selection-state": "Get Manual Selection State",
  "get-all-frames": "Load Frames to PDF",
  "export-frames-with-dpi": "Preview Frames at DPI",
  "export-zip-with-password": "Export ZIP With Password",
  "export-palette": "Export Palette",
  "export-color-schemes": "Export Color Schemes",
  "export-selected-options": "Export Selected Options",
  "start-eyedropper": "Start Eyedropper",
  "color-copied": "Color Copied",
  "copy-link": "Copy Link",
  "check-font-availability": "Check Font Availability",
  "import-svg-to-figma": "Import SVG To Figma",
  "load-liked-presets": "Load Liked Presets",
  "save-liked-presets": "Save Liked Presets",
  "load-presets": "Load Presets",
  "save-preset": "Save Preset",
  "delete-preset": "Delete Preset",
  "apply-preset": "Apply Preset",
  "create-frame": "Create Frame",
  "convert-units": "Convert Units",
  "export-frame": "Export Frame",
  "toggle-collapse": "Toggle Collapse",
  "tool-collapsed": "Tool Collapsed",
  "palette-to-collapsed": "Palette To Collapsed",
  "resize-expanded": "Resize Expanded",
  "resize-window": "Resize Window",
  "open-auth-url": "Open Auth URL",
  "oauth-token": "OAuth Token",
  "get-token": "Get Token",
  "store-token": "Store Token",
  "clear-token": "Clear Token",
  "get-session": "Get Session",
  "store-session": "Store Session",
  "clear-session": "Clear Session",
  "store-avatar": "Store Avatar",
  "avatar-selected": "Avatar Selected",
  "copy-to-clipboard": "Copy To Clipboard",
  "get-font": "Get Font",
  "get-ui-state": "Get UI State",
  "set-analytics-endpoint": "Set Analytics Endpoint",
  "analytics-event": "Analytics Event Relay",
  "analytics-batch": "Analytics Batch Relay",
  "analytics-identify": "Analytics Identify",
  "analytics-flush": "Analytics Flush",
  "lg-refresh": "Liquid Glass Refresh",
};

function humanizeFeatureKey(value) {
  const raw = String(value || "").trim();
  if (!raw) return "Unknown";
  return raw
    .replace(/[:._-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function inferFeatureCategory(key) {
  const normalized = String(key || "").toLowerCase();
  if (!normalized) return "Custom";
  if (normalized.includes("comment") || normalized.includes("summary")) {
    return "Comment Summarizer";
  }
  if (normalized.startsWith("toggle-") || normalized.includes("collapsed")) {
    return "Navigation";
  }
  if (
    normalized.startsWith("export-") ||
    normalized.startsWith("export:") ||
    normalized.startsWith("pdf_export_") ||
    normalized.includes("zip")
  ) {
    return "Export";
  }
  if (normalized.startsWith("import:")) {
    return "Import";
  }
  if (normalized.startsWith("palette:")) {
    return "Palette";
  }
  if (
    normalized.includes("palette") ||
    normalized.includes("eyedropper") ||
    normalized.includes("color")
  ) {
    return "Palette";
  }
  if (normalized.includes("import") || normalized.includes("font")) {
    return "Import";
  }
  if (normalized.includes("html") && normalized.includes("design")) {
    return "Import";
  }
  if (normalized.includes("frame") || normalized.includes("manual-selection")) {
    return "Frame Workflow";
  }
  if (
    normalized.includes("preset") ||
    normalized.includes("unit") ||
    normalized === "convert-units"
  ) {
    return "Unit Converter";
  }
  if (
    normalized.includes("auth") ||
    normalized.includes("token") ||
    normalized.includes("session") ||
    normalized.includes("avatar") ||
    normalized.includes("profile")
  ) {
    return "Auth/Profile";
  }
  if (normalized.startsWith("analytics-") || normalized === "set-analytics-endpoint") {
    return "System";
  }
  if (
    normalized.startsWith("get-") ||
    normalized.startsWith("load-") ||
    normalized.startsWith("ui_")
  ) {
    return "Read";
  }
  if (
    normalized.startsWith("save-") ||
    normalized.startsWith("store-") ||
    normalized.startsWith("delete-") ||
    normalized.startsWith("clear-")
  ) {
    return "Write";
  }
  if (normalized.startsWith("click:") || normalized.startsWith("input:")) {
    return "Interaction";
  }
  if (normalized.startsWith("tab:")) {
    return "Navigation";
  }
  if (normalized.startsWith("key:")) {
    return "Interaction";
  }
  if (
    normalized.startsWith("favorite:") ||
    normalized.startsWith("importer-favorite:") ||
    normalized.includes("favorite")
  ) {
    return "Engagement";
  }
  return "Interaction";
}

function isPassiveActionKey(actionKey) {
  const normalized = String(actionKey || "").trim().toLowerCase();
  if (!normalized) return false;
  if (PASSIVE_ACTION_KEY_SET.has(normalized)) return true;
  if (normalized.startsWith("analytics-")) return true;
  return false;
}

function isPassiveEventType(eventType) {
  const normalized = String(eventType || "").trim();
  if (!normalized) return false;
  return PASSIVE_EVENT_TYPE_SET.has(normalized);
}

function buildFeatureDefinitions() {
  const actionDefinitions = Object.keys(ACTION_TO_TOOL)
    .filter((actionKey) => !isPassiveActionKey(actionKey))
    .map((actionKey) => {
    const normalized = String(actionKey).toLowerCase();
    const mappedTool = ACTION_TO_TOOL[normalized] || "unknown";
    return {
      kind: "action",
      key: normalized,
      label: ACTION_LABEL_OVERRIDES[normalized] || humanizeFeatureKey(normalized),
      category: inferFeatureCategory(normalized),
      tool: mappedTool,
      source: "plugin-action",
    };
  });

  const eventDefinitions = FEATURE_EVENT_DEFINITIONS.map((entry) => ({
    kind: "event",
    key: String(entry.key),
    label: entry.label,
    category: entry.category,
    tool: entry.tool,
    source: entry.source,
  }));

  const all = actionDefinitions.concat(eventDefinitions);
  const deduped = new Map();
  for (const row of all) {
    const dedupeKey = `${row.kind}:${row.key}`;
    if (!deduped.has(dedupeKey)) {
      deduped.set(dedupeKey, row);
    }
  }
  return Array.from(deduped.values());
}

const DASHBOARD_DEFAULT_EVENTS = new Set([
  "plugin_session_started",
  "plugin_session_ended",
  "session_heartbeat",
  "ui_session_started",
  "ui_heartbeat",
  "ui_resize",
  "ui_visibility_change",
  "ui_scroll",
  "ui_before_unload",
  "ui_state_snapshot",
  "ui_user_authenticated",
  "ui_user_unauthenticated",
  "user_context_changed",
  "analytics_transport_updated",
  "plugin_message",
]);

function normalizeToolId(toolId) {
  const key = safeString(toolId, 120);
  if (!key) return null;
  const normalized = key.trim().toLowerCase();
  return TOOL_ALIASES[normalized] || normalized;
}

function isInternalTool(toolId) {
  const normalized = normalizeToolId(toolId);
  return Boolean(normalized && INTERNAL_TOOL_SET.has(normalized));
}

function getToolLabel(toolId) {
  const normalized = normalizeToolId(toolId) || "unknown";
  return TOOL_LABELS[normalized] || normalized;
}

function inferToolFromAction(action) {
  const key = safeString(action, 120);
  if (!key) return null;
  const normalized = key.toLowerCase();
  if (ACTION_TO_TOOL[normalized]) return ACTION_TO_TOOL[normalized];

  if (normalized.includes("palette") || normalized.includes("color")) {
    return "palettable";
  }
  if (normalized.includes("frame") || normalized.includes("zip")) {
    return "frame-gallery";
  }
  if (normalized.includes("import") || normalized.includes("font")) {
    return "import-tool";
  }
  if (normalized.includes("unit") || normalized.includes("preset")) {
    return "unit-converter";
  }
  if (
    normalized.includes("auth") ||
    normalized.includes("token") ||
    normalized.includes("session") ||
    normalized.includes("profile") ||
    normalized.includes("avatar")
  ) {
    return "profile";
  }
  if (normalized.includes("game")) return "wayfall-game";
  if (normalized.includes("liquid") || normalized.includes("lg-")) {
    return "liquid-glass";
  }
  return null;
}

function inferToolForEvent(eventType, payload, rawTool) {
  const payloadObj = payload && typeof payload === "object" ? payload : {};
  const payloadElement =
    payloadObj.element && typeof payloadObj.element === "object"
      ? payloadObj.element
      : {};

  const candidates = [
    rawTool,
    payloadObj.uiTool,
    payloadObj.tool,
    payloadElement.toolId,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeToolId(candidate);
    if (normalized && normalized !== "unknown") return normalized;
  }

  const inferredFromAction = inferToolFromAction(
    payloadObj.action || payloadObj.messageType || payloadObj.type
  );
  if (inferredFromAction) return inferredFromAction;

  const normalizedEventType = String(eventType || "").trim().toLowerCase();
  if (DASHBOARD_DEFAULT_EVENTS.has(normalizedEventType)) {
    return "dashboard";
  }

  return normalizeToolId(rawTool) || "unknown";
}

function anonymousIdFromSeed(seed) {
  const value = safeString(seed, 240);
  if (!value) return null;
  return `device_${stableHashId(value)}`;
}

function userIdentityExpression() {
  return {
    $cond: [
      { $eq: ["$user.isAuthenticated", true] },
      "$user.userId",
      {
        $ifNull: ["$user.anonymousId", null],
      },
    ],
  };
}

function normalizeUser(user, fallbackAnonymousSeed = null) {
  if (!user || typeof user !== "object") {
    const fallbackAnonymousId = anonymousIdFromSeed(fallbackAnonymousSeed);
    return {
      isAuthenticated: false,
      userId: null,
      anonymousId: fallbackAnonymousId,
      name: null,
      email: null,
      identitySource: fallbackAnonymousId ? "device-fallback" : null,
    };
  }

  const inferredUserId =
    safeString(user.userId) ||
    safeString(user.id) ||
    safeString(user._id) ||
    safeString(user.email);
  const explicitIsAuthenticated =
    typeof user.isAuthenticated === "boolean" ? user.isAuthenticated : null;
  const isAuthenticated =
    explicitIsAuthenticated !== null
      ? explicitIsAuthenticated
      : Boolean(inferredUserId || user.email);
  const fallbackAnonymousId = anonymousIdFromSeed(fallbackAnonymousSeed);
  const anonymousIdInput =
    safeString(user.anonymousId) ||
    safeString(user.anonId) ||
    (isAuthenticated ? null : safeString(user.userId || user.id)) ||
    (isAuthenticated ? null : fallbackAnonymousId);
  const userId = isAuthenticated ? inferredUserId : null;
  const anonymousId = isAuthenticated ? null : anonymousIdInput;
  const identitySource =
    safeString(user.identitySource, 80) ||
    (isAuthenticated
      ? "authenticated"
      : safeString(user.anonymousId) || safeString(user.anonId)
      ? "anonymous"
      : fallbackAnonymousId
      ? "device-fallback"
      : null);
  const creditsRemainingValue = Number(
    user.creditsRemaining ??
      (user.billing &&
      typeof user.billing === "object" &&
      user.billing.wallet &&
      typeof user.billing.wallet === "object"
        ? user.billing.wallet.availableCredits
        : null)
  );

  return {
    isAuthenticated,
    userId,
    anonymousId,
    name: isAuthenticated ? safeString(user.name, 120) : null,
    email: isAuthenticated ? safeString(user.email, 160) : null,
    identitySource,
    creditsRemaining: Number.isFinite(creditsRemainingValue)
      ? Math.max(0, creditsRemainingValue)
      : null,
  };
}

function sanitizePayload(payload) {
  if (!payload || typeof payload !== "object") return {};
  const result = {};
  const keys = Object.keys(payload).slice(0, 40);

  for (const key of keys) {
    const value = payload[key];
    if (typeof value === "string") {
      result[key] = safeString(value, 800);
    } else if (
      typeof value === "number" ||
      typeof value === "boolean" ||
      value === null
    ) {
      result[key] = value;
    } else if (Array.isArray(value)) {
      result[key] = value.slice(0, 30);
    } else if (typeof value === "object") {
      result[key] = value;
    }
  }

  return result;
}

function buildMatch(query, options = {}) {
  const includeAction = options.includeAction !== false;
  const { from, to } = parseDateRange(query);
  const match = {
    eventAt: { $gte: from, $lte: to },
    eventType: {
      $nin: [
        "session_heartbeat",
        "plugin_message",
        "analytics_transport_updated",
        "tool_context_changed",
        "backend_operation"
      ]
    }
  };

  const tool = safeString(query.tool, 80);
  if (tool && tool !== "all") {
    match.tool = tool;
  }

  const auth = safeString(query.auth, 20);
  if (auth === "authenticated") {
    match["user.isAuthenticated"] = true;
  } else if (auth === "anonymous") {
    match["user.isAuthenticated"] = false;
  }

  if (includeAction) {
    const actions = parseActionFilter(query.action);
    if (actions.length) {
      addMatchClause(match, {
        $or: [
          { eventType: { $in: actions } },
          { "payload.action": { $in: actions } },
          { "payload.messageType": { $in: actions } },
          { "payload.interactionAction": { $in: actions } },
          { "payload.type": { $in: actions } },
        ],
      });
    }
  }

  return { match, from, to };
}

function normalizeHeatmapPoint(point) {
  const normalizedX = Number(point && point.normalizedX);
  const normalizedY = Number(point && point.normalizedY);
  const x = Number(point && point.x);
  const y = Number(point && point.y);
  const viewportWidth = Number(point && point.viewportWidth);
  const viewportHeight = Number(point && point.viewportHeight);

  let nx = Number.isFinite(normalizedX) ? normalizedX : null;
  let ny = Number.isFinite(normalizedY) ? normalizedY : null;

  if (nx === null && Number.isFinite(x) && Number.isFinite(viewportWidth) && viewportWidth > 0) {
    nx = x / viewportWidth;
  }
  if (ny === null && Number.isFinite(y) && Number.isFinite(viewportHeight) && viewportHeight > 0) {
    ny = y / viewportHeight;
  }

  if (nx === null || ny === null) {
    return null;
  }
  if (!Number.isFinite(nx) || !Number.isFinite(ny)) {
    return null;
  }

  return {
    nx: Math.max(0, Math.min(1, nx)),
    ny: Math.max(0, Math.min(1, ny)),
  };
}

function buildHeatmapBins(points, gridX, gridY) {
  const cells = new Map();
  let maxCount = 0;
  let total = 0;

  for (const point of points) {
    const normalized = normalizeHeatmapPoint(point);
    if (!normalized) continue;

    const x = Math.min(gridX - 1, Math.floor(normalized.nx * gridX));
    const y = Math.min(gridY - 1, Math.floor(normalized.ny * gridY));
    const key = `${x}:${y}`;
    const next = (cells.get(key) || 0) + 1;
    cells.set(key, next);
    if (next > maxCount) maxCount = next;
    total += 1;
  }

  const bins = Array.from(cells.entries()).map(([key, count]) => {
    const [x, y] = key.split(":").map(Number);
    return { x, y, count };
  });

  return { bins, maxCount, totalPoints: total };
}

function readAuthGate(req, res, next) {
  if (!READ_USER || !READ_PASS) {
    return next();
  }

  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith("Basic ")) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Plugin Dashboard"');
    return res.status(401).json({ error: "Authentication required" });
  }

  const decoded = Buffer.from(auth.slice(6), "base64").toString("utf8");
  const sepIndex = decoded.indexOf(":");
  const user = sepIndex >= 0 ? decoded.slice(0, sepIndex) : "";
  const pass = sepIndex >= 0 ? decoded.slice(sepIndex + 1) : "";

  if (user !== READ_USER || pass !== READ_PASS) {
    return res.status(403).json({ error: "Invalid credentials" });
  }

  return next();
}

function cronAuthGate(req, res, next) {
  const expected = safeString(process.env.CRON_SECRET, 240);
  if (!expected) {
    return res.status(503).json({ error: "CRON_SECRET is not configured" });
  }

  const authHeader = safeString(req.headers.authorization, 320);
  if (authHeader !== `Bearer ${expected}`) {
    return res.status(401).json({ error: "Unauthorized cron invocation" });
  }

  return next();
}

function ingestAuthGate(req, res, next) {
  if (!INGEST_TOKEN) return next();

  const provided = safeString(req.headers["x-plugin-ingest-token"], 240);
  if (provided === INGEST_TOKEN) {
    return next();
  }

  if (!INGEST_TOKEN_REQUIRED) {
    return next();
  }

  if (provided !== INGEST_TOKEN) {
    return res.status(401).json({ error: "Invalid ingest token" });
  }

  return next();
}

export async function initializeServer() {
  if (isInitialized) return;
  if (!initializationPromise) {
    initializationPromise = ensureIndexes()
      .then(() => recordDailySnapshot())
      .then(() => {
        isInitialized = true;
      })
      .catch((error) => {
        initializationPromise = null;
        throw error;
      });
  }

  await initializationPromise;
}

async function buildCurrentStatsSnapshot(now = new Date()) {
  const dateString = now.toISOString().slice(0, 10);
  const eventsCollection = await getEventsCollection();
  const engagementCollection = await getEngagementCollection();
  const todayStart = new Date(dateString + "T00:00:00.000Z");
  const todayEnd = new Date(dateString + "T23:59:59.999Z");
  const [stats, creditCount, activeSessionsArr] = await Promise.all([
    computePublicStatsMetrics(eventsCollection, engagementCollection, now),
    eventsCollection.countDocuments({ eventType: "credit_consumed" }),
    eventsCollection.distinct("sessionId", {
      eventAt: { $gte: todayStart, $lte: todayEnd },
    }),
  ]);

  return {
    _id: dateString,
    date: new Date(dateString),
    mau: stats.mau || 0,
    authenticatedUsers: stats.authenticatedUsers || 0,
    likes: stats.likes || 0,
    saves: stats.saves || 0,
    follows: stats.follows || 0,
    reuses: stats.reused || 0,
    creditsConsumed: creditCount,
    activeSessions: activeSessionsArr.length,
    recordedAt: now,
  };
}

async function recordDailySnapshot() {
  try {
    const snapshot = await buildCurrentStatsSnapshot(new Date());
    const snapshotsCollection = await getSnapshotsCollection();

    await snapshotsCollection.updateOne(
      { _id: snapshot._id },
      { $set: snapshot },
      { upsert: true }
    );
    console.log(`Daily snapshot refreshed for ${snapshot._id}`);
  } catch (error) {
    console.error("Failed to record daily snapshot:", error);
  }
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "plugin-data-dashboard",
    initialized: isInitialized,
  });
});

app.get("/api/ops/credit-digest", cronAuthGate, async (_req, res) => {
  try {
    await initializeServer();
    await recordDailySnapshot();

    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    const result = await sendCreditNewsletter(intelligence.newsletter);

    return res.json({
      ok: true,
      snapshotDate: new Date().toISOString().slice(0, 10),
      newsletter: result,
      summary: intelligence.summary,
    });
  } catch (error) {
    console.error("Credit digest cron failed:", error);
    return res.status(500).json({ error: "Credit digest cron failed" });
  }
});

app.use("/api/plugin-analytics", async (_req, res, next) => {
  try {
    await initializeServer();
    next();
  } catch (error) {
    console.error("Database initialization failed:", error);
    res.status(500).json({
      error: "Database initialization failed",
      detail: safeString(error && error.message, 320),
    });
  }
});

async function fetchSummaryData(eventsCollection, match) {
  const [
    totalEvents,
    uniqueSessionsRaw,
    uniqueUsersRaw,
    uniqueAnonymousUsersRaw,
    anonymousEvents,
    sessionDurationAgg,
    topTools,
    topActions,
    eventsByDay,
  ] = await Promise.all([
    eventsCollection.countDocuments(match),
    eventsCollection.distinct("sessionId", match),
    eventsCollection.distinct("user.userId", {
      ...match,
      "user.isAuthenticated": true,
      "user.userId": { $ne: null },
    }),
    eventsCollection.distinct("user.anonymousId", {
      ...match,
      "user.isAuthenticated": false,
      "user.anonymousId": { $ne: null },
    }),
    eventsCollection.countDocuments({
      ...match,
      "user.isAuthenticated": false,
    }),
    eventsCollection
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: "$sessionId",
            startedAt: { $min: "$eventAt" },
            endedAt: { $max: "$eventAt" },
          },
        },
        {
          $project: {
            durationMs: { $subtract: ["$endedAt", "$startedAt"] },
          },
        },
        {
          $group: {
            _id: null,
            avgSessionDurationMs: { $avg: "$durationMs" },
            maxSessionDurationMs: { $max: "$durationMs" },
          },
        },
      ])
      .toArray(),
    eventsCollection
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: "$tool",
            events: { $sum: 1 },
            sessions: { $addToSet: "$sessionId" },
            activeEvents: {
              $sum: {
                $cond: [passiveEventExpression(), 0, 1],
              },
            },
            passiveEvents: {
              $sum: {
                $cond: [passiveEventExpression(), 1, 0],
              },
            },
            timeSpentMs: {
              $sum: {
                $cond: [
                  { $eq: ["$eventType", "tool_time_spent"] },
                  {
                    $convert: {
                      input: "$payload.durationMs",
                      to: "double",
                      onError: 0,
                      onNull: 0,
                    },
                  },
                  0,
                ],
              },
            },
          },
        },
        {
          $project: {
            _id: 0,
            tool: "$_id",
            events: 1,
            sessionCount: { $size: "$sessions" },
            activeEvents: 1,
            passiveEvents: 1,
            timeSpentMs: { $round: ["$timeSpentMs", 2] },
          },
        },
        { $sort: { activeEvents: -1, events: -1 } },
        { $limit: 12 },
      ])
      .toArray(),
    eventsCollection
      .aggregate([
        { $match: match },
        {
          $project: {
            action: actionProjectionExpression(),
          },
        },
        {
          $group: {
            _id: "$action",
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 30 },
        {
          $project: {
            _id: 0,
            action: "$_id",
            count: 1,
          },
        },
      ])
      .toArray(),
    eventsCollection
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: "$eventAt",
              },
            },
            events: { $sum: 1 },
            sessions: { $addToSet: "$sessionId" },
          },
        },
        {
          $project: {
            _id: 0,
            day: "$_id",
            events: 1,
            sessions: { $size: "$sessions" },
          },
        },
        { $sort: { day: 1 } },
      ])
      .toArray(),
  ]);

  const durationInfo = sessionDurationAgg[0] || {
    avgSessionDurationMs: 0,
    maxSessionDurationMs: 0,
  };

  return {
    kpis: {
      totalEvents,
      totalSessions: uniqueSessionsRaw.length,
      authenticatedUsers: uniqueUsersRaw.length,
      anonymousUsers: uniqueAnonymousUsersRaw.length,
      anonymousEvents,
      avgSessionDurationMs: Math.round(toNumber(durationInfo.avgSessionDurationMs, 0)),
      maxSessionDurationMs: Math.round(toNumber(durationInfo.maxSessionDurationMs, 0)),
    },
    topTools,
    topActions,
    eventsByDay,
  };
}

async function fetchActionCatalog(eventsCollection, match, limit = 180) {
  return eventsCollection
    .aggregate([
      { $match: match },
      {
        $project: {
          action: actionProjectionExpression(),
          eventType: 1,
        },
      },
      {
        $group: {
          _id: "$action",
          count: { $sum: 1 },
          eventTypes: { $addToSet: "$eventType" },
        },
      },
      { $sort: { count: -1, _id: 1 } },
      { $limit: limit },
      {
        $project: {
          _id: 0,
          action: "$_id",
          count: 1,
          eventTypes: 1,
        },
      },
    ])
    .toArray();
}

async function fetchToolUsageData(eventsCollection, match) {
  return eventsCollection
    .aggregate([
      { $match: match },
      {
        $group: {
          _id: "$tool",
          eventCount: { $sum: 1 },
          activeEventCount: {
            $sum: {
              $cond: [passiveEventExpression(), 0, 1],
            },
          },
          passiveEventCount: {
            $sum: {
              $cond: [passiveEventExpression(), 1, 0],
            },
          },
          clickCount: {
            $sum: {
              $cond: [{ $eq: ["$eventType", "ui_click"] }, 1, 0],
            },
          },
          sessionIds: { $addToSet: "$sessionId" },
          authUsers: { $addToSet: "$user.userId" },
          anonymousUsers: { $addToSet: "$user.anonymousId" },
          timeSpentMs: {
            $sum: {
              $cond: [
                { $eq: ["$eventType", "tool_time_spent"] },
                {
                  $convert: {
                    input: "$payload.durationMs",
                    to: "double",
                    onError: 0,
                    onNull: 0,
                  },
                },
                0,
              ],
            },
          },
        },
      },
      {
        $project: {
          _id: 0,
          tool: "$_id",
          eventCount: 1,
          activeEventCount: 1,
          passiveEventCount: 1,
          clickCount: 1,
          sessionCount: { $size: "$sessionIds" },
          authenticatedUserCount: {
            $size: {
              $setDifference: ["$authUsers", [null]],
            },
          },
          anonymousUserCount: {
            $size: {
              $setDifference: ["$anonymousUsers", [null]],
            },
          },
          timeSpentMs: { $round: ["$timeSpentMs", 2] },
        },
      },
      {
        $addFields: {
          userCount: { $add: ["$authenticatedUserCount", "$anonymousUserCount"] },
        },
      },
      { $sort: { activeEventCount: -1, eventCount: -1 } },
    ])
    .toArray();
}

async function fetchHeatmapData(eventsCollection, match, options = {}) {
  const {
    toolFilter = null,
    limit = 4000,
    compact = false,
    gridX = 96,
    gridY = 24,
  } = options;

  const heatmapMatch = {
    ...match,
    eventType: "ui_click",
  };

  if (toolFilter && toolFilter !== "all") {
    heatmapMatch.$or = [
      { tool: toolFilter },
      { "payload.element.toolId": toolFilter },
      { "payload.uiTool": toolFilter },
    ];
  }

  const points = await eventsCollection
    .aggregate([
      { $match: heatmapMatch },
      { $sort: { eventAt: -1 } },
      { $limit: limit },
      {
        $project: {
          _id: 0,
          eventAt: 1,
          tool: 1,
          x: "$payload.x",
          y: "$payload.y",
          normalizedX: "$payload.normalizedX",
          normalizedY: "$payload.normalizedY",
          viewportWidth: "$payload.viewportWidth",
          viewportHeight: "$payload.viewportHeight",
          action: "$payload.action",
          actionLabel: "$payload.actionLabel",
          elementTag: "$payload.element.tag",
          elementId: "$payload.element.id",
          elementToolId: "$payload.element.toolId",
        },
      },
    ])
    .toArray();

  if (!compact) {
    return { points, count: points.length };
  }

  const binning = buildHeatmapBins(points, gridX, gridY);
  return {
    compact: true,
    grid: { x: gridX, y: gridY },
    bins: binning.bins,
    maxCount: binning.maxCount,
    totalPoints: binning.totalPoints,
    sampleCount: points.length,
  };
}

async function fetchSessionsData(eventsCollection, match, limit = 60) {
  return eventsCollection
    .aggregate([
      { $match: match },
      { $sort: { eventAt: 1 } },
      {
        $group: {
          _id: "$sessionId",
          startedAt: { $first: "$eventAt" },
          endedAt: { $last: "$eventAt" },
          eventCount: { $sum: 1 },
          activeEventCount: {
            $sum: {
              $cond: [passiveEventExpression(), 0, 1],
            },
          },
          passiveEventCount: {
            $sum: {
              $cond: [passiveEventExpression(), 1, 0],
            },
          },
          user: { $last: "$user" },
          tools: { $addToSet: "$tool" },
          lastSource: { $last: "$source" },
        },
      },
      {
        $project: {
          _id: 0,
          sessionId: "$_id",
          startedAt: 1,
          endedAt: 1,
          durationMs: { $subtract: ["$endedAt", "$startedAt"] },
          eventCount: 1,
          activeEventCount: 1,
          passiveEventCount: 1,
          user: 1,
          tools: 1,
          lastSource: 1,
        },
      },
      { $sort: { endedAt: -1 } },
      { $limit: limit },
    ])
    .toArray();
}

async function fetchRecentEventsData(eventsCollection, match, limit = 120) {
  return eventsCollection
    .find(match)
    .project({
      _id: 0,
      eventAt: 1,
      eventType: 1,
      tool: 1,
      source: 1,
      sessionId: 1,
      user: 1,
      payload: 1,
    })
    .sort({ eventAt: -1 })
    .limit(limit)
    .toArray();
}

async function fetchEventTypeBreakdown(eventsCollection, match, limit = MAX_EVENT_TYPE_BREAKDOWN) {
  return eventsCollection
    .aggregate([
      { $match: match },
      {
        $group: {
          _id: "$eventType",
          count: { $sum: 1 },
          sessionIds: { $addToSet: "$sessionId" },
        },
      },
      {
        $project: {
          _id: 0,
          eventType: "$_id",
          count: 1,
          sessionCount: { $size: "$sessionIds" },
        },
      },
      { $sort: { count: -1 } },
      { $limit: limit },
    ])
    .toArray();
}

function normalizeCompressionLevel(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "unknown";
  if (raw === "optimal" || raw === "medium") return "medium";
  if (raw === "low") return "low";
  if (raw === "high") return "high";
  return raw;
}

function normalizeColorMode(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (raw === "CMYK") return "CMYK";
  if (raw === "RGB") return "RGB";
  return "UNKNOWN";
}

function sizeBucketLabel(bytes) {
  const size = Number(bytes || 0);
  if (!Number.isFinite(size) || size <= 0) return "unknown";
  if (size < 5 * 1024 * 1024) return "<5MB";
  if (size < 10 * 1024 * 1024) return "5-10MB";
  if (size < 20 * 1024 * 1024) return "10-20MB";
  return ">20MB";
}

function toFiniteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function incrementCounter(counter, key, by = 1) {
  if (!key) return;
  counter[key] = toFiniteNumber(counter[key], 0) + by;
}

function sortCounter(counter) {
  return Object.entries(counter)
    .map(([key, count]) => ({
      key,
      count: toFiniteNumber(count, 0),
    }))
    .sort((a, b) => b.count - a.count);
}

function sortNamedCounter(counter, labelKey = "label") {
  return Object.entries(counter)
    .map(([label, count]) => ({
      [labelKey]: label,
      count: toFiniteNumber(count, 0),
    }))
    .sort((a, b) => b.count - a.count);
}

function orderedNamedCounter(counter, labelKey, order) {
  const seen = new Set();
  const rows = [];

  for (const label of order) {
    rows.push({
      [labelKey]: label,
      count: toFiniteNumber(counter[label], 0),
    });
    seen.add(label);
  }

  const extras = Object.entries(counter)
    .filter(([label]) => !seen.has(label))
    .map(([label, count]) => ({
      [labelKey]: label,
      count: toFiniteNumber(count, 0),
    }))
    .sort((a, b) => b.count - a.count);

  return rows.concat(extras);
}

function roundNumber(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Number(n.toFixed(digits));
}

function parseEmailList(value) {
  const raw = safeString(value, 2000);
  if (!raw) return [];
  return Array.from(
    new Set(
      raw
        .split(/[;,]/)
        .map((item) => item.trim())
        .filter(Boolean)
    )
  );
}

function normalizeEmailKey(value) {
  const email = safeString(value, 160);
  return email ? email.toLowerCase() : null;
}

function calculateDepletionDays(creditsRemaining, estimatedDailyBurn) {
  if (!Number.isFinite(creditsRemaining) || creditsRemaining <= 0) return null;
  if (!Number.isFinite(estimatedDailyBurn) || estimatedDailyBurn <= 0) return null;
  return Math.ceil(creditsRemaining / estimatedDailyBurn);
}

function classifyCreditRisk(user) {
  const remaining =
    user.creditsRemaining === null || user.creditsRemaining === undefined
      ? null
      : Number(user.creditsRemaining);
  const depletionDays =
    user.depletionDays === null || user.depletionDays === undefined
      ? null
      : Number(user.depletionDays);

  if (Number.isFinite(remaining) && remaining < RUNOUT_CREDIT_THRESHOLD / 2) {
    return "critical";
  }
  if (Number.isFinite(depletionDays) && depletionDays <= RUNOUT_CREDIT_DAYS) {
    return "warning";
  }
  if (Number.isFinite(remaining) && remaining < RUNOUT_CREDIT_THRESHOLD) {
    return "warning";
  }
  return "healthy";
}

async function countProjectedActions(eventsCollection, actionKeys) {
  const normalizedActions = Array.from(
    new Set((actionKeys || []).map((key) => String(key || "").trim().toLowerCase()).filter(Boolean))
  );
  if (!normalizedActions.length) return 0;

  const rows = await eventsCollection
    .aggregate([
      {
        $project: {
          action: {
            $toLower: {
              $ifNull: [actionProjectionExpression(), ""],
            },
          },
        },
      },
      {
        $match: {
          action: { $in: normalizedActions },
        },
      },
      {
        $count: "count",
      },
    ])
    .toArray();

  return Number(rows[0] && rows[0].count ? rows[0].count : 0);
}

async function countFavoriteAdds(eventsCollection) {
  const rows = await eventsCollection
    .aggregate([
      {
        $project: {
          eventType: 1,
          isFavorited: "$payload.isFavorited",
          action: {
            $toLower: {
              $ifNull: [actionProjectionExpression(), ""],
            },
          },
        },
      },
      {
        $match: {
          $or: [
            {
              eventType: { $in: ["tool_favorite_changed", "importer_favorite_changed"] },
              isFavorited: true,
            },
            {
              action: { $in: ["favorite:add", "importer-favorite:add"] },
            },
          ],
        },
      },
      { $count: "count" },
    ])
    .toArray();

  return Number(rows[0] && rows[0].count ? rows[0].count : 0);
}

async function countReusedUsers(eventsCollection) {
  const rows = await eventsCollection
    .aggregate([
      {
        $addFields: {
          normalizedUserId: userIdentityExpression(),
        },
      },
      {
        $match: {
          normalizedUserId: { $ne: null },
        },
      },
      {
        $group: {
          _id: "$normalizedUserId",
          sessionIds: { $addToSet: "$sessionId" },
        },
      },
      {
        $project: {
          sessionCount: {
            $size: {
              $setDifference: ["$sessionIds", [null]],
            },
          },
        },
      },
      {
        $match: {
          sessionCount: { $gt: 1 },
        },
      },
      {
        $count: "count",
      },
    ])
    .toArray();

  return Number(rows[0] && rows[0].count ? rows[0].count : 0);
}

async function computePublicStatsMetrics(eventsCollection, engagementCollection, now = new Date()) {
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  const [
    authMAU,
    anonMAU,
    manualStats,
    likes,
    saves,
    follows,
    reused,
  ] = await Promise.all([
    eventsCollection.distinct("user.userId", {
      eventAt: { $gte: thirtyDaysAgo },
      "user.isAuthenticated": true,
      "user.userId": { $ne: null },
    }),
    eventsCollection.distinct("user.anonymousId", {
      eventAt: { $gte: thirtyDaysAgo },
      "user.isAuthenticated": false,
      "user.anonymousId": { $ne: null },
    }),
    engagementCollection.findOne({ _id: "global_stats" }),
    countFavoriteAdds(eventsCollection),
    countProjectedActions(eventsCollection, ["save-preset"]),
    countProjectedActions(eventsCollection, ["emailentered", "emailEntered"]),
    countReusedUsers(eventsCollection),
  ]);

  const manual = manualStats || {};
  const authenticatedUsers = authMAU.length;
  const mau = authenticatedUsers + anonMAU.length;

  const installs = Math.max(
    Number(manual.installs || 0),
    authenticatedUsers
  );

  return {
    mau,
    authenticatedUsers,
    anonymousUsers: anonMAU.length,
    likes: Math.max(Number(manual.likes || 0), likes),
    saves: Math.max(Number(manual.saves || 0), saves),
    follows: Math.max(Number(manual.follows || 0), follows),
    reused: Math.max(Number(manual.reused || 0), reused),
    installs,
    reuses: Math.max(Number(manual.reused || 0), reused),
  };
}

function startOfUtcDay(value) {
  const date = value instanceof Date ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function addUtcDays(date, days) {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function dayKeyFromDate(value) {
  const date = startOfUtcDay(value);
  return date ? date.toISOString().slice(0, 10) : null;
}

function dayKeyToDate(dayKey) {
  return new Date(`${dayKey}T00:00:00.000Z`);
}

function enumerateDayKeys(startDate, endDate) {
  const keys = [];
  if (!startDate || !endDate || startDate > endDate) return keys;
  for (let cursor = new Date(startDate); cursor <= endDate; cursor = addUtcDays(cursor, 1)) {
    keys.push(dayKeyFromDate(cursor));
  }
  return keys;
}

async function buildPublicStatsHistory(eventsCollection, range = "90d", now = new Date()) {
  const endDay = startOfUtcDay(now);
  const endExclusive = addUtcDays(endDay, 1);
  const rangeMap = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
    "180d": 180,
    "365d": 365,
  };

  const minEventRows = await eventsCollection
    .aggregate([
      {
        $group: {
          _id: null,
          minAt: { $min: "$eventAt" },
        },
      },
    ])
    .toArray();

  const minEventAt = minEventRows[0] && minEventRows[0].minAt ? new Date(minEventRows[0].minAt) : null;
  const earliestDay = minEventAt ? startOfUtcDay(minEventAt) : endDay;
  const rangeDays = rangeMap[range] || null;
  const startDay = rangeDays
    ? (addUtcDays(endDay, -(rangeDays - 1)) > earliestDay ? addUtcDays(endDay, -(rangeDays - 1)) : earliestDay)
    : earliestDay;

  const [dailyActivityRows, dailyActionRows] = await Promise.all([
    eventsCollection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: earliestDay, $lt: endExclusive },
          },
        },
        {
          $addFields: {
            normalizedUserId: userIdentityExpression(),
            dayKey: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: "$eventAt",
              },
            },
          },
        },
        {
          $match: {
            normalizedUserId: { $ne: null },
          },
        },
        {
          $group: {
            _id: {
              dayKey: "$dayKey",
              userId: "$normalizedUserId",
            },
            isAuthenticated: {
              $max: {
                $cond: [{ $eq: ["$user.isAuthenticated", true] }, 1, 0],
              },
            },
            sessionIds: { $addToSet: "$sessionId" },
          },
        },
        {
          $project: {
            _id: 0,
            dayKey: "$_id.dayKey",
            userId: "$_id.userId",
            isAuthenticated: 1,
            sessionIds: {
              $setDifference: ["$sessionIds", [null]],
            },
          },
        },
        {
          $sort: {
            dayKey: 1,
          },
        },
      ])
      .toArray(),
    eventsCollection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: earliestDay, $lt: endExclusive },
          },
        },
        {
          $project: {
            dayKey: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: "$eventAt",
              },
            },
            eventType: 1,
            isFavorited: "$payload.isFavorited",
            action: {
              $toLower: {
                $ifNull: [actionProjectionExpression(), ""],
              },
            },
          },
        },
        {
          $group: {
            _id: "$dayKey",
            likes: {
              $sum: {
                $cond: [
                  {
                    $or: [
                      {
                        $and: [
                          {
                            $in: [
                              "$eventType",
                              ["tool_favorite_changed", "importer_favorite_changed"],
                            ],
                          },
                          { $eq: ["$isFavorited", true] },
                        ],
                      },
                      {
                        $in: ["$action", ["favorite:add", "importer-favorite:add"]],
                      },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
            saves: {
              $sum: {
                $cond: [{ $eq: ["$action", "save-preset"] }, 1, 0],
              },
            },
            follows: {
              $sum: {
                $cond: [{ $eq: ["$action", "emailentered"] }, 1, 0],
              },
            },
          },
        },
        {
          $project: {
            _id: 0,
            dayKey: "$_id",
            likes: 1,
            saves: 1,
            follows: 1,
          },
        },
        {
          $sort: {
            dayKey: 1,
          },
        },
      ])
      .toArray(),
  ]);

  const activityByDay = new Map();
  for (const row of dailyActivityRows) {
    const dayKey = safeString(row.dayKey);
    const userId = safeString(row.userId);
    if (!dayKey || !userId) continue;
    const bucket = activityByDay.get(dayKey) || {
      authUsers: new Set(),
      anonUsers: new Set(),
      sessionRows: [],
      sessionIds: new Set(),
    };
    if (Number(row.isAuthenticated || 0) === 1) {
      bucket.authUsers.add(userId);
    } else {
      bucket.anonUsers.add(userId);
    }
    const sessionIds = Array.isArray(row.sessionIds)
      ? row.sessionIds.map((value) => safeString(value)).filter(Boolean)
      : [];
    for (const sessionId of sessionIds) {
      bucket.sessionIds.add(sessionId);
    }
    bucket.sessionRows.push({
      userId,
      sessionIds,
    });
    activityByDay.set(dayKey, bucket);
  }

  const actionsByDay = new Map();
  for (const row of dailyActionRows) {
    const dayKey = safeString(row.dayKey);
    if (!dayKey) continue;
    actionsByDay.set(dayKey, {
      likes: Number(row.likes || 0),
      saves: Number(row.saves || 0),
      follows: Number(row.follows || 0),
    });
  }

  const allDayKeys = enumerateDayKeys(earliestDay, endDay);
  const rollingAuthBuckets = [];
  const rollingAnonBuckets = [];
  const cumulativeUserSessions = new Map();
  let runningLikes = 0;
  let runningSaves = 0;
  let runningFollows = 0;
  let runningReused = 0;
  const outputRows = [];
  const startDayKey = dayKeyFromDate(startDay);

  for (let index = 0; index < allDayKeys.length; index += 1) {
    const dayKey = allDayKeys[index];
    const bucket = activityByDay.get(dayKey) || {
      authUsers: new Set(),
      anonUsers: new Set(),
      sessionRows: [],
      sessionIds: new Set(),
    };

    rollingAuthBuckets.push(bucket.authUsers);
    rollingAnonBuckets.push(bucket.anonUsers);
    if (rollingAuthBuckets.length > 30) rollingAuthBuckets.shift();
    if (rollingAnonBuckets.length > 30) rollingAnonBuckets.shift();

    for (const row of bucket.sessionRows) {
      const userSessions = cumulativeUserSessions.get(row.userId) || new Set();
      const previousSize = userSessions.size;
      for (const sessionId of row.sessionIds) {
        userSessions.add(sessionId);
      }
      cumulativeUserSessions.set(row.userId, userSessions);
      if (previousSize <= 1 && userSessions.size > 1) {
        runningReused += 1;
      }
    }

    const actionBucket = actionsByDay.get(dayKey) || { likes: 0, saves: 0, follows: 0 };
    runningLikes += actionBucket.likes;
    runningSaves += actionBucket.saves;
    runningFollows += actionBucket.follows;

    if (dayKey < startDayKey) continue;

    const authWindow = new Set();
    const anonWindow = new Set();
    for (const authSet of rollingAuthBuckets) {
      for (const userId of authSet) authWindow.add(userId);
    }
    for (const anonSet of rollingAnonBuckets) {
      for (const userId of anonSet) anonWindow.add(userId);
    }

    outputRows.push({
      _id: dayKey,
      date: dayKeyToDate(dayKey),
      mau: authWindow.size + anonWindow.size,
      authenticatedUsers: authWindow.size,
      anonymousUsers: anonWindow.size,
      likes: runningLikes,
      saves: runningSaves,
      follows: runningFollows,
      reused: runningReused,
      reuses: runningReused,
      creditsConsumed: 0,
      activeSessions: bucket.sessionIds.size,
      recordedAt: now,
    });
  }

  return outputRows;
}

async function fetchBackendCreditUsers() {
  const [usersCollection, billingCollection] = await Promise.all([
    getBackendUsersCollection(),
    getBackendUserBillingCollection(),
  ]);

  const rows = await usersCollection
    .aggregate([
      {
        $lookup: {
          from: billingCollection.collectionName,
          localField: "_id",
          foreignField: "user",
          as: "billingRecords",
        },
      },
      {
        $addFields: {
          latestBilling: {
            $arrayElemAt: ["$billingRecords", 0],
          },
        },
      },
      {
        $project: {
          _id: {
            $toString: "$_id",
          },
          isAuthenticated: {
            $literal: true,
          },
          name: "$name",
          email: "$email",
          firstSeen: "$createdAt",
          lastSeen: "$updatedAt",
          creditsRemaining: {
            $ifNull: [
              {
                $convert: {
                  input: "$latestBilling.availableCredits",
                  to: "double",
                  onError: null,
                  onNull: null,
                },
              },
              {
                $convert: {
                  input: "$creditsRemaining",
                  to: "double",
                  onError: null,
                  onNull: null,
                },
              },
            ],
          },
          heldCredits: {
            $convert: {
              input: "$latestBilling.heldCredits",
              to: "double",
              onError: 0,
              onNull: 0,
            },
          },
          lifetimeSpentCredits: {
            $convert: {
              input: "$latestBilling.lifetimeSpentCredits",
              to: "double",
              onError: 0,
              onNull: 0,
            },
          },
          subscriptionStatus: "$latestBilling.subscriptionStatus",
        },
      },
      {
        $sort: {
          email: 1,
          _id: 1,
        },
      },
    ])
    .toArray();

  return rows.map((row) => ({
    _id: safeString(row._id),
    isAuthenticated: true,
    name: safeString(row.name, 160) || null,
    email: safeString(row.email, 160) || null,
    firstSeen: row.firstSeen || null,
    lastSeen: row.lastSeen || null,
    creditsRemaining: Number.isFinite(Number(row.creditsRemaining))
      ? Math.max(0, Number(row.creditsRemaining))
      : null,
    heldCredits: Number.isFinite(Number(row.heldCredits))
      ? Math.max(0, Number(row.heldCredits))
      : 0,
    lifetimeSpentCredits: Number.isFinite(Number(row.lifetimeSpentCredits))
      ? Math.max(0, Number(row.lifetimeSpentCredits))
      : 0,
    subscriptionStatus: safeString(row.subscriptionStatus, 80) || null,
  }));
}

async function fetchBackendCreditLedgerSpendRows() {
  const ledgerCollection = await getBackendCreditLedgerCollection();

  const rows = await ledgerCollection
    .aggregate([
      {
        $match: {
          toolCode: { $ne: null },
          reason: {
            $in: ["reservation_hold", "reservation_release", "reservation_commit"],
          },
        },
      },
      {
        $group: {
          _id: {
            user: {
              $toString: "$user",
            },
            tool: "$toolCode",
          },
          count: { $sum: 1 },
          netSpent: {
            $sum: {
              $multiply: [
                {
                  $convert: {
                    input: "$deltaCredits",
                    to: "double",
                    onError: 0,
                    onNull: 0,
                  },
                },
                -1,
              ],
            },
          },
        },
      },
      {
        $project: {
          _id: 0,
          userId: "$_id.user",
          tool: "$_id.tool",
          count: 1,
          totalAmount: { $round: ["$netSpent", 2] },
        },
      },
      {
        $match: {
          totalAmount: { $gt: 0 },
        },
      },
      {
        $sort: {
          totalAmount: -1,
          count: -1,
        },
      },
    ])
    .toArray();

  return rows.map((row) => ({
    userId: safeString(row.userId),
    tool: normalizeToolId(row.tool) || safeString(row.tool) || "unknown",
    count: Number(row.count || 0),
    totalAmount: roundNumber(row.totalAmount, 2),
  }));
}

async function fetchCreditIntelligence(eventsCollection) {
  const now = new Date();
  const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
  const aggregateOptions = { allowDiskUse: true };
  const creditAmountExpression = {
    $convert: {
      input: "$payload.amount",
      to: "double",
      onError: 1,
      onNull: 1,
    },
  };
  const durationExpression = {
    $convert: {
      input: "$payload.durationMs",
      to: "double",
      onError: 0,
      onNull: 0,
    },
  };

  const backendUsers = await fetchBackendCreditUsers();
  const backendLedgerSpendRows = await fetchBackendCreditLedgerSpendRows();
  const activityRows = await eventsCollection
      .aggregate([
        {
          $addFields: {
            normalizedUserId: userIdentityExpression(),
          },
        },
        {
          $match: {
            normalizedUserId: { $ne: null },
          },
        },
        {
          $group: {
            _id: "$normalizedUserId",
            isAuthenticated: {
              $max: {
                $cond: [{ $eq: ["$user.isAuthenticated", true] }, 1, 0],
              },
            },
            name: { $max: "$user.name" },
            email: { $max: "$user.email" },
            lastSeen: { $max: "$eventAt" },
            firstSeen: { $min: "$eventAt" },
            sessionIds: { $addToSet: "$sessionId" },
          },
        },
        {
          $project: {
            _id: 1,
            isAuthenticated: 1,
            name: 1,
            email: 1,
            lastSeen: 1,
            firstSeen: 1,
            sessionCount: {
              $size: {
                $setDifference: ["$sessionIds", [null]],
              },
            },
          },
        },
      ], aggregateOptions)
      .toArray();
  const balanceRows = await eventsCollection
      .aggregate([
        {
          $match: {
            "user.creditsRemaining": { $exists: true, $ne: null },
          },
        },
        {
          $addFields: {
            normalizedUserId: userIdentityExpression(),
            creditsRemainingValue: {
              $convert: {
                input: "$user.creditsRemaining",
                to: "double",
                onError: null,
                onNull: null,
              },
            },
          },
        },
        {
          $match: {
            normalizedUserId: { $ne: null },
          },
        },
        {
          $group: {
            _id: "$normalizedUserId",
            creditsRemaining: { $max: "$creditsRemainingValue" },
            balanceSeenAt: { $max: "$eventAt" },
          },
        },
      ], aggregateOptions)
      .toArray();
  const spendRows = await eventsCollection
      .aggregate([
        {
          $match: {
            eventType: "credit_consumed",
          },
        },
        {
          $addFields: {
            normalizedUserId: userIdentityExpression(),
            creditAmount: creditAmountExpression,
          },
        },
        {
          $match: {
            normalizedUserId: { $ne: null },
          },
        },
        {
          $group: {
            _id: "$normalizedUserId",
            totalConsumed: { $sum: "$creditAmount" },
            creditEventCount: { $sum: 1 },
            consumed7d: {
              $sum: {
                $cond: [{ $gte: ["$eventAt", sevenDaysAgo] }, "$creditAmount", 0],
              },
            },
            consumed30d: {
              $sum: {
                $cond: [{ $gte: ["$eventAt", thirtyDaysAgo] }, "$creditAmount", 0],
              },
            },
            firstCreditAt: { $min: "$eventAt" },
            lastCreditAt: { $max: "$eventAt" },
          },
        },
      ], aggregateOptions)
      .toArray();
  const topToolRows = await eventsCollection
      .aggregate([
        {
          $match: {
            eventType: "tool_time_spent",
          },
        },
        {
          $addFields: {
            normalizedUserId: userIdentityExpression(),
            totalTimeMsValue: durationExpression,
          },
        },
        {
          $match: {
            normalizedUserId: { $ne: null },
            tool: { $nin: [null, "unknown"] },
          },
        },
        {
          $group: {
            _id: {
              user: "$normalizedUserId",
              tool: "$tool",
            },
            totalTimeMs: { $sum: "$totalTimeMsValue" },
          },
        },
        {
          $project: {
            _id: 0,
            userId: "$_id.user",
            tool: "$_id.tool",
            timeSpentMs: { $round: ["$totalTimeMs", 2] },
          },
        },
      ], aggregateOptions)
      .toArray();

  const users = new Map();
  const identityAliases = new Map();
  const emailIndex = new Map();

  function createEmptyUser(key) {
    return {
      _id: key,
      isAuthenticated: false,
      name: null,
      email: null,
      firstSeen: null,
      lastSeen: null,
      sessionCount: 0,
      creditsRemaining: null,
      heldCredits: 0,
      totalConsumed: 0,
      creditEventCount: 0,
      consumed7d: 0,
      consumed30d: 0,
      totalTimeSpentMs: 0,
      topTools: [],
      topCreditTools: [],
      lifetimeSpentCredits: 0,
      subscriptionStatus: null,
    };
  }

  function registerEmailIdentity(key, email) {
    const emailKey = normalizeEmailKey(email);
    if (!emailKey) return;
    emailIndex.set(emailKey, key);
    if (emailKey !== key) {
      identityAliases.set(emailKey, key);
    }
  }

  function resolveUserKey(rawId, email) {
    const normalizedId = safeString(rawId);
    if (normalizedId) {
      if (users.has(normalizedId)) return normalizedId;
      if (identityAliases.has(normalizedId)) return identityAliases.get(normalizedId);
    }

    const emailKey = normalizeEmailKey(email);
    if (emailKey && emailIndex.has(emailKey)) {
      const resolvedKey = emailIndex.get(emailKey);
      if (normalizedId && normalizedId !== resolvedKey) {
        identityAliases.set(normalizedId, resolvedKey);
      }
      return resolvedKey;
    }

    return null;
  }

  function getExistingUser(rawId, email) {
    const resolvedKey = resolveUserKey(rawId, email);
    if (!resolvedKey) return null;

    const existing = users.get(resolvedKey);
    if (!existing) return null;

    const normalizedId = safeString(rawId);
    if (normalizedId && normalizedId !== resolvedKey) {
      identityAliases.set(normalizedId, resolvedKey);
    }
    registerEmailIdentity(resolvedKey, email || existing.email);

    return existing;
  }

  for (const row of backendUsers) {
    if (!row || !row._id) continue;
    const current = createEmptyUser(row._id);
    current.isAuthenticated = true;
    current.name = row.name || null;
    current.email = row.email || null;
    current.firstSeen = row.firstSeen || null;
    current.lastSeen = row.lastSeen || null;
    current.creditsRemaining = Number.isFinite(Number(row.creditsRemaining))
      ? Math.max(0, Number(row.creditsRemaining))
      : null;
    current.heldCredits = Number.isFinite(Number(row.heldCredits))
      ? Math.max(0, Number(row.heldCredits))
      : 0;
    current.lifetimeSpentCredits = roundNumber(row.lifetimeSpentCredits, 2);
    current.totalConsumed = roundNumber(row.lifetimeSpentCredits, 2);
    current.subscriptionStatus = row.subscriptionStatus || null;
    users.set(row._id, current);
    registerEmailIdentity(row._id, row.email);
  }

  for (const row of activityRows) {
    const current = getExistingUser(row._id, row.email);
    if (!current) continue;
    current.isAuthenticated = current.isAuthenticated || row.isAuthenticated === 1;
    current.name = current.name || row.name || null;
    current.email = current.email || row.email || null;
    current.firstSeen = current.firstSeen || row.firstSeen || null;
    current.lastSeen = row.lastSeen || current.lastSeen || null;
    current.sessionCount = Math.max(Number(current.sessionCount || 0), Number(row.sessionCount || 0));
    registerEmailIdentity(current._id, current.email);
  }

  for (const row of balanceRows) {
    const current = getExistingUser(row._id, null);
    if (!current) continue;
    current.creditsRemaining = Number.isFinite(Number(row.creditsRemaining))
      ? Math.max(0, Number(row.creditsRemaining))
      : current.creditsRemaining;
    current.lastSeen = current.lastSeen || row.balanceSeenAt || null;
  }

  for (const row of spendRows) {
    const current = getExistingUser(row._id, null);
    if (!current) continue;
    current.totalConsumed = Math.max(
      Number(current.totalConsumed || 0),
      roundNumber(row.totalConsumed, 2)
    );
    current.creditEventCount = Number(row.creditEventCount || 0);
    current.consumed7d = roundNumber(row.consumed7d, 2);
    current.consumed30d = roundNumber(row.consumed30d, 2);
    current.firstCreditAt = row.firstCreditAt || null;
    current.lastCreditAt = row.lastCreditAt || null;
    current.firstSeen = current.firstSeen || row.firstCreditAt || null;
    current.lastSeen = current.lastSeen || row.lastCreditAt || null;
  }

  const topToolMap = new Map();
  for (const row of topToolRows) {
    const userId = safeString(row.userId);
    if (!userId) continue;
    const entries = topToolMap.get(userId) || [];
    entries.push({
      tool: normalizeToolId(row.tool) || safeString(row.tool) || "unknown",
      timeSpentMs: roundNumber(row.timeSpentMs, 2),
    });
    topToolMap.set(userId, entries);
  }

  for (const [userId, toolRows] of topToolMap.entries()) {
    const current = getExistingUser(userId, null);
    if (!current) continue;
    const sortedTools = toolRows
      .filter((toolRow) => toolRow && toolRow.tool)
      .sort((a, b) => Number(b.timeSpentMs || 0) - Number(a.timeSpentMs || 0));
    const meaningfulTools = sortedTools.filter((toolRow) => !isInternalTool(toolRow.tool));
    const visibleTools = meaningfulTools.length ? meaningfulTools : sortedTools;
    current.totalTimeSpentMs = roundNumber(
      visibleTools.reduce((sum, toolRow) => sum + Number(toolRow.timeSpentMs || 0), 0),
      2
    );
    current.topTools = visibleTools.slice(0, 2);
  }

  const toolBreakdownMap = new Map();
  const userCreditToolMap = new Map();
  for (const row of backendLedgerSpendRows) {
    if (!row || !row.userId || !row.tool) continue;
    const current = getExistingUser(row.userId, null);
    if (!current) continue;

    const userTools = userCreditToolMap.get(current._id) || [];
    userTools.push({
      tool: row.tool,
      totalAmount: roundNumber(row.totalAmount, 2),
      count: Number(row.count || 0),
    });
    userCreditToolMap.set(current._id, userTools);

    const existingTool = toolBreakdownMap.get(row.tool) || {
      tool: row.tool,
      count: 0,
      totalAmount: 0,
      userIds: new Set(),
    };
    existingTool.count += Number(row.count || 0);
    existingTool.totalAmount += Number(row.totalAmount || 0);
    existingTool.userIds.add(current._id);
    toolBreakdownMap.set(row.tool, existingTool);
  }

  for (const [userId, toolRows] of userCreditToolMap.entries()) {
    const current = getExistingUser(userId, null);
    if (!current) continue;
    const sortedCreditTools = toolRows
      .filter((toolRow) => toolRow && toolRow.tool && Number(toolRow.totalAmount || 0) > 0)
      .sort((a, b) => Number(b.totalAmount || 0) - Number(a.totalAmount || 0));
    current.topCreditTools = sortedCreditTools.slice(0, 2);
    const ledgerTotal = sortedCreditTools.reduce(
      (sum, toolRow) => sum + Number(toolRow.totalAmount || 0),
      0
    );
    current.totalConsumed = Math.max(
      Number(current.totalConsumed || 0),
      roundNumber(ledgerTotal, 2)
    );
  }

  const toolBreakdown = Array.from(toolBreakdownMap.values())
    .map((row) => ({
      tool: row.tool,
      count: Number(row.count || 0),
      totalAmount: roundNumber(row.totalAmount, 2),
      userCount: row.userIds.size,
    }))
    .filter((row) => Number(row.totalAmount || 0) > 0)
    .sort((a, b) => {
      const amountDiff = Number(b.totalAmount || 0) - Number(a.totalAmount || 0);
      if (amountDiff !== 0) return amountDiff;
      return Number(b.count || 0) - Number(a.count || 0);
    });

  const mergedUsers = Array.from(users.values()).map((user) => {
    const activeDays = Math.max(
      1,
      user.firstSeen && user.lastSeen
        ? (new Date(user.lastSeen).getTime() - new Date(user.firstSeen).getTime()) / 86400000
        : 1
    );
    const estimatedDailyBurn =
      user.consumed30d > 0
        ? user.consumed30d / 30
        : user.consumed7d > 0
        ? user.consumed7d / 7
        : user.totalConsumed > 0
        ? user.totalConsumed / activeDays
        : 0;
    const depletionDays = calculateDepletionDays(user.creditsRemaining, estimatedDailyBurn);

    return {
      ...user,
      estimatedDailyBurn: roundNumber(estimatedDailyBurn, 2),
      depletionDays,
      riskLevel: classifyCreditRisk({
        creditsRemaining: user.creditsRemaining,
        depletionDays,
      }),
    };
  });

  mergedUsers.sort((a, b) => {
    const riskOrder = { critical: 0, warning: 1, healthy: 2 };
    const riskDiff = (riskOrder[a.riskLevel] || 99) - (riskOrder[b.riskLevel] || 99);
    if (riskDiff !== 0) return riskDiff;
    const depletionA = Number.isFinite(a.depletionDays) ? a.depletionDays : Number.MAX_SAFE_INTEGER;
    const depletionB = Number.isFinite(b.depletionDays) ? b.depletionDays : Number.MAX_SAFE_INTEGER;
    if (depletionA !== depletionB) return depletionA - depletionB;
    return Number(b.totalConsumed || 0) - Number(a.totalConsumed || 0);
  });

  const trackedUsers = mergedUsers.length;
  const authenticatedUsers = mergedUsers.filter((user) => user.isAuthenticated).length;
  const anonymousUsers = trackedUsers - authenticatedUsers;
  const lowCreditUsers = mergedUsers.filter((user) => {
    if (user.creditsRemaining === null || user.creditsRemaining === undefined) {
      return false;
    }
    const remaining = Number(user.creditsRemaining);
    return Number.isFinite(remaining) && remaining < RUNOUT_CREDIT_THRESHOLD;
  }).length;
  const runoutSoonUsers = mergedUsers.filter((user) => user.riskLevel !== "healthy").length;
  const creditsConsumedTotal = roundNumber(
    mergedUsers.reduce((sum, user) => sum + Number(user.totalConsumed || 0), 0),
    2
  );
  const creditsRemainingTotal = roundNumber(
    mergedUsers.reduce((sum, user) => sum + Number(user.creditsRemaining || 0), 0),
    2
  );
  const averageDailyBurn = roundNumber(
    trackedUsers > 0
      ? mergedUsers.reduce((sum, user) => sum + Number(user.estimatedDailyBurn || 0), 0) / trackedUsers
      : 0,
    2
  );

  const topPowerUsers = mergedUsers
    .filter((user) => Number(user.totalTimeSpentMs || 0) > 0)
    .sort((a, b) => Number(b.totalTimeSpentMs || 0) - Number(a.totalTimeSpentMs || 0))
    .slice(0, 8);

  const newsletter = buildCreditNewsletterPreview({
    summary: {
      trackedUsers,
      lowCreditUsers,
      runoutSoonUsers,
      creditsConsumedTotal,
    },
    users: mergedUsers,
    toolBreakdown,
    topPowerUsers,
  });

  return {
    summary: {
      trackedUsers,
      authenticatedUsers,
      anonymousUsers,
      lowCreditUsers,
      runoutSoonUsers,
      creditsConsumedTotal,
      creditsRemainingTotal,
      averageDailyBurn,
    },
    users: mergedUsers,
    toolBreakdown,
    topPowerUsers,
    newsletter,
  };
}

function escapeHtmlFragment(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function buildCreditNewsletterPreview(intelligence) {
  const users = Array.isArray(intelligence.users) ? intelligence.users : [];
  const toolBreakdown = Array.isArray(intelligence.toolBreakdown)
    ? intelligence.toolBreakdown
    : [];
  const runoutCandidates = users.filter((user) => user.riskLevel !== "healthy").slice(0, 10);
  const topPowerUsers = Array.isArray(intelligence.topPowerUsers)
    ? intelligence.topPowerUsers
    : [];
  const recipients = parseEmailList(CREDIT_NEWSLETTER_TO);
  const subject = `Waysorted credit digest: ${runoutCandidates.length} at-risk users, ${intelligence.summary.creditsConsumedTotal} credits spent`;

  const textLines = [
    "Waysorted credit digest",
    "",
    `Tracked users: ${intelligence.summary.trackedUsers}`,
    `Low-credit users: ${intelligence.summary.lowCreditUsers}`,
    `Runout-soon users: ${intelligence.summary.runoutSoonUsers}`,
    `Credits consumed: ${intelligence.summary.creditsConsumedTotal}`,
    "",
    "Runout candidates:",
    ...(runoutCandidates.length
      ? runoutCandidates.map((user) => {
          const label = user.name || user.email || user._id;
          const topTool = user.topTools && user.topTools[0] ? getToolLabel(user.topTools[0].tool) : "Unknown";
          return `- ${label}: ${user.creditsRemaining ?? "?"} credits left, ${user.depletionDays ?? "?"} days left, top tool ${topTool}`;
        })
      : ["- None"]),
    "",
    "Top tool spend:",
    ...(toolBreakdown.length
      ? toolBreakdown.slice(0, 5).map((row) => `- ${getToolLabel(row.tool)}: ${row.totalAmount} credits across ${row.count} ledger entries`)
      : ["- No attributed ledger spend yet"]),
    "",
    "Power users by time spent:",
    ...(topPowerUsers.length
      ? topPowerUsers.map((user) => {
          const label = user.name || user.email || user._id;
          const topTool = user.topTools && user.topTools[0] ? user.topTools[0].tool : "unknown";
          const minutes = roundNumber(Number(user.totalTimeSpentMs || 0) / 60000, 1);
          return `- ${label}: ${minutes} min, top tool ${topTool}`;
        })
      : ["- No time-spent data yet"]),
  ];

  const html = `
    <div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.5">
      <h1 style="margin-bottom:8px">Waysorted credit digest</h1>
      <p style="margin-top:0;color:#475569">
        ${intelligence.summary.lowCreditUsers} low-credit users, ${intelligence.summary.runoutSoonUsers} users likely to run out soon,
        ${intelligence.summary.creditsConsumedTotal} total credits consumed.
      </p>
      <h2>Runout candidates</h2>
      <ul>
        ${
          runoutCandidates.length
            ? runoutCandidates
                .map((user) => {
                  const label = escapeHtmlFragment(user.name || user.email || user._id);
                  const topTool = escapeHtmlFragment(
                    user.topTools && user.topTools[0] ? getToolLabel(user.topTools[0].tool) : "Unknown"
                  );
                  return `<li><strong>${label}</strong>: ${user.creditsRemaining ?? "?"} credits left, ${
                    user.depletionDays ?? "?"
                  } estimated days left, top tool ${topTool}</li>`;
                })
                .join("")
            : "<li>No users currently look at risk.</li>"
        }
      </ul>
      <h2>Top tool spend</h2>
      <ul>
        ${
          toolBreakdown.length
            ? toolBreakdown
                .slice(0, 5)
                .map(
                  (row) =>
                    `<li><strong>${escapeHtmlFragment(getToolLabel(row.tool))}</strong>: ${row.totalAmount} credits across ${row.count} ledger entries</li>`
                )
                .join("")
            : "<li>No attributed ledger spend yet.</li>"
        }
      </ul>
      <h2>Power users by time spent</h2>
      <ul>
        ${
          topPowerUsers.length
            ? topPowerUsers
                .map((user) => {
                  const label = escapeHtmlFragment(user.name || user.email || user._id);
                  const topTool = escapeHtmlFragment(
                    user.topTools && user.topTools[0] ? getToolLabel(user.topTools[0].tool) : "Unknown"
                  );
                  const minutes = roundNumber(Number(user.totalTimeSpentMs || 0) / 60000, 1);
                  return `<li><strong>${label}</strong>: ${minutes} minutes, top tool ${topTool}</li>`;
                })
                .join("")
            : "<li>No tool time data yet.</li>"
        }
      </ul>
    </div>
  `;

  return {
    subject,
    text: textLines.join("\n"),
    html,
    recipients,
    runoutCandidates,
    topToolSpend: toolBreakdown.slice(0, 5),
    powerUsers: topPowerUsers,
    canSend: Boolean(RESEND_API_KEY && CREDIT_NEWSLETTER_FROM && recipients.length > 0),
  };
}

async function sendCreditNewsletter(preview, options = {}) {
  const recipients = Array.isArray(options.to) && options.to.length
    ? options.to
    : Array.isArray(preview.recipients)
    ? preview.recipients
    : [];

  if (!RESEND_API_KEY) {
    return { sent: false, reason: "RESEND_API_KEY is not configured" };
  }
  if (!CREDIT_NEWSLETTER_FROM) {
    return { sent: false, reason: "CREDIT_NEWSLETTER_FROM is not configured" };
  }
  if (!recipients.length) {
    return { sent: false, reason: "No newsletter recipients configured" };
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `waysorted-credit-digest-${new Date().toISOString().slice(0, 10)}`,
    },
    body: JSON.stringify({
      from: CREDIT_NEWSLETTER_FROM,
      to: recipients,
      subject: preview.subject,
      html: preview.html,
      text: preview.text,
    }),
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    return {
      sent: false,
      reason: payload && payload.message ? payload.message : `Resend request failed (${response.status})`,
    };
  }

  return {
    sent: true,
    recipients,
    id: payload && payload.id ? payload.id : null,
  };
}

function resolveEventAction(event) {
  const payload = event && event.payload && typeof event.payload === "object"
    ? event.payload
    : {};
  return (
    safeString(payload.action, 160) ||
    safeString(payload.messageType, 160) ||
    safeString(payload.interactionAction, 160) ||
    safeString(payload.type, 160) ||
    safeString(event && event.eventType, 160) ||
    "unknown_event"
  );
}

function resolveNestedPayload(event) {
  const payload = event && event.payload && typeof event.payload === "object"
    ? event.payload
    : {};
  if (payload.payload && typeof payload.payload === "object") {
    return payload.payload;
  }
  return payload;
}

async function fetchFeatureAnalytics(eventsCollection, match, options = {}) {
  const limit = parseLimit(options.limit, 150000, 250000);
  const catalogLimit = parseLimit(options.catalogLimit, 600, 2400);
  const cursor = eventsCollection
    .find(match)
    .project({
      _id: 0,
      eventAt: 1,
      eventType: 1,
      tool: 1,
      payload: 1,
      sessionId: 1,
      user: 1,
    })
    .sort({ eventAt: 1 })
    .limit(limit);

  const paletteExports = {};
  const favoriteAdds = {};
  const importSizeBuckets = { "<5MB": 0, "5-10MB": 0, "10-20MB": 0, ">20MB": 0, unknown: 0 };
  const exportSizeBuckets = { "<5MB": 0, "5-10MB": 0, "10-20MB": 0, ">20MB": 0, unknown: 0 };
  const dpiBreakdown = { "72": 0, "150": 0, "300": 0, other: 0 };
  const compressionBreakdown = { low: 0, medium: 0, high: 0, unknown: 0 };
  const colorModeBreakdown = { RGB: 0, CMYK: 0, UNKNOWN: 0 };
  const mergePagesDistribution = {};

  let favoriteAddCount = 0;
  let favoriteRemoveCount = 0;
  let collapsedModeMs = 0;
  let expandedModeMs = 0;
  let passwordEnabledRuns = 0;
  let passwordDisabledRuns = 0;
  let exportRunCount = 0;
  let mergedPdfGroups = 0;
  let mergedPagesTotal = 0;
  let maxPagesPerMerge = 0;
  const featureDefinitions = buildFeatureDefinitions();
  const definitionByCompositeKey = new Map(
    featureDefinitions.map((item) => [`${item.kind}:${item.key}`, item])
  );
  const featureStats = new Map();

  function ensureFeatureStat(kind, key, defaults = {}) {
    const compositeKey = `${kind}:${key}`;
    if (!featureStats.has(compositeKey)) {
      featureStats.set(compositeKey, {
        kind,
        key,
        label: defaults.label || humanizeFeatureKey(key),
        category: defaults.category || inferFeatureCategory(key),
        tool: defaults.tool || "unknown",
        source: defaults.source || "runtime-observed",
        count: 0,
        sessionSet: new Set(),
        authUserSet: new Set(),
        anonymousUserSet: new Set(),
        lastSeen: null,
      });
    }
    return featureStats.get(compositeKey);
  }

  for await (const event of cursor) {
    const payload =
      event && event.payload && typeof event.payload === "object"
        ? event.payload
        : {};
    const nestedPayload = resolveNestedPayload(event);
    const action = resolveEventAction(event);
    const normalizedAction = String(action || "").toLowerCase();
    const eventType = String(event && event.eventType ? event.eventType : "");
    const sessionId = safeString(event && event.sessionId, 160);
    const user = event && event.user && typeof event.user === "object" ? event.user : {};
    const userId = user && user.isAuthenticated ? safeString(user.userId, 160) : null;
    const anonymousId = !userId ? safeString(user.anonymousId, 160) : null;
    const passiveAction = isPassiveActionKey(normalizedAction);
    const passiveEvent = isPassiveEventType(eventType);

    if (normalizedAction && !passiveAction) {
      const actionCompositeKey = `action:${normalizedAction}`;
      const actionDefinition =
        definitionByCompositeKey.get(actionCompositeKey) || null;
      const actionStat = ensureFeatureStat("action", normalizedAction, {
        label:
          (actionDefinition && actionDefinition.label) ||
          ACTION_LABEL_OVERRIDES[normalizedAction] ||
          humanizeFeatureKey(normalizedAction),
        category:
          (actionDefinition && actionDefinition.category) ||
          inferFeatureCategory(normalizedAction),
        tool:
          (actionDefinition && actionDefinition.tool) ||
          inferToolFromAction(normalizedAction) ||
          normalizeToolId(event && event.tool) ||
          "unknown",
        source: (actionDefinition && actionDefinition.source) || "runtime-observed",
      });

      actionStat.count += 1;
      if (sessionId) actionStat.sessionSet.add(sessionId);
      if (userId) actionStat.authUserSet.add(userId);
      if (anonymousId) actionStat.anonymousUserSet.add(anonymousId);
      actionStat.lastSeen = event && event.eventAt ? event.eventAt : actionStat.lastSeen;
    }

    const eventCompositeKey = `event:${eventType}`;
    if (!passiveEvent && definitionByCompositeKey.has(eventCompositeKey)) {
      const eventDefinition = definitionByCompositeKey.get(eventCompositeKey);
      const eventStat = ensureFeatureStat("event", eventType, {
        label: eventDefinition.label,
        category: eventDefinition.category,
        tool: eventDefinition.tool || normalizeToolId(event && event.tool) || "unknown",
        source: eventDefinition.source,
      });
      eventStat.count += 1;
      if (sessionId) eventStat.sessionSet.add(sessionId);
      if (userId) eventStat.authUserSet.add(userId);
      if (anonymousId) eventStat.anonymousUserSet.add(anonymousId);
      eventStat.lastSeen = event && event.eventAt ? event.eventAt : eventStat.lastSeen;
    }

    if (eventType === "tool_time_spent") {
      const durationMs = toFiniteNumber(payload.durationMs || nestedPayload.durationMs, 0);
      const modeTool = String(payload.tool || nestedPayload.tool || event.tool || "").toLowerCase();
      if (modeTool === "collapsed-dashboard") {
        collapsedModeMs += Math.max(durationMs, 0);
      } else if (modeTool === "dashboard") {
        expandedModeMs += Math.max(durationMs, 0);
      }
    }

    const isFavoriteEvent =
      eventType === "tool_favorite_changed" ||
      eventType === "importer_favorite_changed" ||
      normalizedAction.startsWith("favorite:") ||
      normalizedAction.startsWith("importer-favorite:");

    if (isFavoriteEvent) {
      const isFavorited =
        payload.isFavorited === true ||
        normalizedAction.endsWith(":add");
      const toolId =
        safeString(
          payload.toolId ||
            payload.importerId ||
            payload.toolLabel ||
            payload.importerName ||
            event.tool,
          160
        ) || "unknown";
      if (isFavorited) {
        incrementCounter(favoriteAdds, toolId, 1);
        favoriteAddCount += 1;
      } else {
        favoriteRemoveCount += 1;
      }
    }

    if (
      eventType === "palette_export_performed" ||
      normalizedAction === "export-palette" ||
      normalizedAction === "export-color-schemes" ||
      normalizedAction === "export-selected-options"
    ) {
      if (eventType === "palette_export_performed") {
        const exportType = safeString(payload.exportType, 80) || "unknown";
        if (exportType === "multi" && Array.isArray(payload.selectedOptions)) {
          for (const option of payload.selectedOptions) {
            incrementCounter(paletteExports, `selected:${safeString(option, 80) || "unknown"}`, 1);
          }
        } else {
          const mode = safeString(payload.mode, 80) || "unknown";
          incrementCounter(paletteExports, `${exportType}:${mode}`, 1);
        }
      } else if (normalizedAction === "export-palette") {
        const mode = safeString(nestedPayload.mode, 80) || "unknown";
        incrementCounter(paletteExports, `variation:${mode}`, 1);
      } else if (normalizedAction === "export-color-schemes") {
        const scheme = safeString(nestedPayload.schemeType, 80) || "unknown";
        incrementCounter(paletteExports, `scheme:${scheme}`, 1);
      } else if (normalizedAction === "export-selected-options") {
        if (Array.isArray(nestedPayload.selectedOptions)) {
          for (const option of nestedPayload.selectedOptions) {
            incrementCounter(paletteExports, `selected:${safeString(option, 80) || "unknown"}`, 1);
          }
        } else {
          incrementCounter(paletteExports, "selected:unknown", 1);
        }
      }
    }

    if (eventType === "import_file_selected") {
      const sizeBytes = toFiniteNumber(payload.fileSizeBytes, 0);
      incrementCounter(importSizeBuckets, sizeBucketLabel(sizeBytes), 1);
    }

    if (eventType === "pdf_export_requested") {
      const dpiValue = toFiniteNumber(payload.dpi, 0);
      if (dpiValue === 72 || dpiValue === 150 || dpiValue === 300) {
        incrementCounter(dpiBreakdown, String(dpiValue), 1);
      } else {
        incrementCounter(dpiBreakdown, "other", 1);
      }

      const compression = normalizeCompressionLevel(payload.compression);
      incrementCounter(compressionBreakdown, compression, 1);

      const colorMode = normalizeColorMode(payload.colorMode);
      incrementCounter(colorModeBreakdown, colorMode, 1);

      if (payload.passwordEnabled === true) {
        passwordEnabledRuns += 1;
      } else {
        passwordDisabledRuns += 1;
      }
    } else if (normalizedAction === "export-frames-with-dpi") {
      const dpiValue = toFiniteNumber(nestedPayload.dpi, 0);
      if (dpiValue === 72 || dpiValue === 150 || dpiValue === 300) {
        incrementCounter(dpiBreakdown, String(dpiValue), 1);
      } else {
        incrementCounter(dpiBreakdown, "other", 1);
      }
    }

    if (eventType === "pdf_merge_group_exported") {
      const pageCount = Math.max(0, toFiniteNumber(payload.pageCount, 0));
      if (pageCount > 0) {
        incrementCounter(mergePagesDistribution, String(pageCount), 1);
        maxPagesPerMerge = Math.max(maxPagesPerMerge, pageCount);
      }
    }

    if (eventType === "pdf_export_completed") {
      exportRunCount += 1;
      const exportBytes = toFiniteNumber(payload.zipBytes || payload.totalPdfBytes, 0);
      incrementCounter(exportSizeBuckets, sizeBucketLabel(exportBytes), 1);

      const mergedGroups = Math.max(0, toFiniteNumber(payload.mergedGroupCount, 0));
      const pagesTotal = Math.max(0, toFiniteNumber(payload.mergedPagesTotal, 0));
      const maxPages = Math.max(0, toFiniteNumber(payload.maxPagesPerMergedPdf, 0));
      mergedPdfGroups += mergedGroups;
      mergedPagesTotal += pagesTotal;
      maxPagesPerMerge = Math.max(maxPagesPerMerge, maxPages);
    }
  }

  const paletteRows = sortNamedCounter(paletteExports, "palette");
  const favoriteRows = sortNamedCounter(favoriteAdds, "tool");
  const importBucketRows = orderedNamedCounter(importSizeBuckets, "bucket", [
    "<5MB",
    "5-10MB",
    "10-20MB",
    ">20MB",
    "unknown",
  ]);
  const exportBucketRows = orderedNamedCounter(exportSizeBuckets, "bucket", [
    "<5MB",
    "5-10MB",
    "10-20MB",
    ">20MB",
    "unknown",
  ]);
  const dpiRows = orderedNamedCounter(dpiBreakdown, "dpi", ["72", "150", "300", "other"]);
  const compressionRows = orderedNamedCounter(compressionBreakdown, "compression", [
    "low",
    "medium",
    "high",
    "unknown",
  ]);
  const colorModeRows = orderedNamedCounter(colorModeBreakdown, "colorMode", [
    "RGB",
    "CMYK",
    "UNKNOWN",
  ]);
  const mergeDistributionRows = Object.entries(mergePagesDistribution)
    .map(([pages, count]) => ({
      pages: Number(pages),
      count: toFiniteNumber(count, 0),
    }))
    .filter((row) => Number.isFinite(row.pages))
    .sort((a, b) => a.pages - b.pages);

  const featureCatalogRows = [];

  for (const definition of featureDefinitions) {
    const compositeKey = `${definition.kind}:${definition.key}`;
    const stat = featureStats.get(compositeKey);
    const count = stat ? toFiniteNumber(stat.count, 0) : 0;
    const sessionCount = stat ? stat.sessionSet.size : 0;
    const authenticatedUsers = stat ? stat.authUserSet.size : 0;
    const anonymousUsers = stat ? stat.anonymousUserSet.size : 0;
    const userCount = authenticatedUsers + anonymousUsers;
    featureCatalogRows.push({
      kind: definition.kind,
      key: definition.key,
      label: definition.label,
      category: definition.category,
      tool: definition.tool,
      source: definition.source,
      count,
      sessionCount,
      authenticatedUsers,
      anonymousUsers,
      userCount,
      lastSeen: stat && stat.lastSeen ? stat.lastSeen : null,
      status: count > 0 ? "active" : "inactive",
    });
  }

  for (const stat of featureStats.values()) {
    const compositeKey = `${stat.kind}:${stat.key}`;
    if (definitionByCompositeKey.has(compositeKey)) continue;
    const authenticatedUsers = stat.authUserSet.size;
    const anonymousUsers = stat.anonymousUserSet.size;
    featureCatalogRows.push({
      kind: stat.kind,
      key: stat.key,
      label: stat.label || humanizeFeatureKey(stat.key),
      category: stat.category || inferFeatureCategory(stat.key),
      tool: stat.tool || "unknown",
      source: stat.source || "runtime-observed",
      count: toFiniteNumber(stat.count, 0),
      sessionCount: stat.sessionSet.size,
      authenticatedUsers,
      anonymousUsers,
      userCount: authenticatedUsers + anonymousUsers,
      lastSeen: stat.lastSeen || null,
      status: toFiniteNumber(stat.count, 0) > 0 ? "active" : "inactive",
    });
  }

  featureCatalogRows.sort((a, b) => {
    const countDiff = toFiniteNumber(b.count, 0) - toFiniteNumber(a.count, 0);
    if (countDiff !== 0) return countDiff;
    return String(a.label || "").localeCompare(String(b.label || ""));
  });

  const activeFeatureCount = featureCatalogRows.filter(
    (item) => item.status === "active"
  ).length;
  const trackableFeatureCount = featureDefinitions.length;
  const featureCoverageRate =
    trackableFeatureCount > 0
      ? Number((activeFeatureCount / trackableFeatureCount).toFixed(4))
      : 0;
  const topFeature = featureCatalogRows.find((item) => item.count > 0) || null;
  const limitedFeatureCatalogRows = featureCatalogRows.slice(0, catalogLimit);

  return {
    kpis: {
      paletteExportEvents: paletteRows.reduce((sum, row) => sum + row.count, 0),
      topPaletteExport: paletteRows[0] || null,
      favoriteAdds: favoriteAddCount,
      favoriteRemoves: favoriteRemoveCount,
      topFavoritedTool: favoriteRows[0] || null,
      collapsedModeMs: Math.round(collapsedModeMs),
      expandedModeMs: Math.round(expandedModeMs),
      exportRuns: exportRunCount,
      passwordEnabledRuns,
      passwordDisabledRuns,
      mergedPdfGroups,
      mergedPagesTotal,
      avgPagesPerMerge:
        mergedPdfGroups > 0 ? Number((mergedPagesTotal / mergedPdfGroups).toFixed(2)) : 0,
      maxPagesPerMerge,
      activeFeatureCount,
      trackableFeatureCount,
      featureCoverageRate,
      topFeature,
    },
    paletteExports: paletteRows,
    favoritedTools: favoriteRows,
    modeTime: {
      collapsedMs: Math.round(collapsedModeMs),
      expandedMs: Math.round(expandedModeMs),
      collapsedShare:
        collapsedModeMs + expandedModeMs > 0
          ? Number((collapsedModeMs / (collapsedModeMs + expandedModeMs)).toFixed(4))
          : 0,
      expandedShare:
        collapsedModeMs + expandedModeMs > 0
          ? Number((expandedModeMs / (collapsedModeMs + expandedModeMs)).toFixed(4))
          : 0,
    },
    importSizeBuckets: importBucketRows,
    exportSizeBuckets: exportBucketRows,
    dpiBreakdown: dpiRows,
    compressionBreakdown: compressionRows,
    colorModeBreakdown: colorModeRows,
    mergeDistribution: mergeDistributionRows,
    featureCatalog: limitedFeatureCatalogRows,
  };
}

app.post("/api/plugin-analytics/ingest", ingestAuthGate, async (req, res) => {
  try {
    const body = req.body || {};
    const inputEvents = Array.isArray(body.events) ? body.events : [];

    if (!inputEvents.length) {
      return res.status(400).json({ error: "events[] is required" });
    }

    const now = new Date();
    const envelope = {
      source: safeString(body.source, 80) || "unknown",
      sessionId: safeString(body.sessionId, 120),
      deviceId: safeString(body.deviceId, 120),
      sentAt: toDate(body.sentAt, now),
      runtime: body.runtime && typeof body.runtime === "object" ? body.runtime : {},
      plugin: body.plugin && typeof body.plugin === "object" ? body.plugin : {},
      user: normalizeUser(body.user, safeString(body.deviceId, 120) || safeString(body.sessionId, 120)),
    };

    const ignoredEventTypes = new Set([
      "session_heartbeat",
      "plugin_message",
      "analytics_transport_updated",
      "tool_context_changed",
      "backend_operation"
    ]);

    const cleanEvents = inputEvents.filter(event => {
      const type = safeString(event && (event.eventType || event.type), 120);
      return !ignoredEventTypes.has(type);
    });

    const docs = cleanEvents.slice(0, 1000).map((event) => {
      const eventType = safeString(event && (event.eventType || event.type), 120) || "unknown_event";
      const eventAt = toDate(event && (event.eventAt || event.timestamp), envelope.sentAt || now);
      const eventDeviceId =
        safeString(event && event.deviceId, 120) || envelope.deviceId || "unknown-device";
      const eventSessionId =
        safeString(event && event.sessionId, 120) || envelope.sessionId || "unknown-session";
      const payload = sanitizePayload(event && event.payload);
      const user = normalizeUser(
        (event && event.user) || envelope.user,
        eventDeviceId || eventSessionId
      );
      const rawTool =
        safeString(event && event.tool, 120) ||
        safeString(payload && payload.uiTool, 120) ||
        null;
      const tool = inferToolForEvent(eventType, payload, rawTool);

      return {
        sessionId: eventSessionId,
        deviceId: eventDeviceId,
        eventType,
        eventAt,
        receivedAt: now,
        source: safeString(event && event.source, 80) || envelope.source,
        tool,
        payload,
        user,
        runtime: envelope.runtime,
        plugin: envelope.plugin,
      };
    });

    const eventsCollection = await getEventsCollection();
    const insertResult = await eventsCollection.insertMany(docs, {
      ordered: false,
    });

    return res.status(202).json({
      accepted: docs.length,
      inserted: Object.keys(insertResult.insertedIds).length,
    });
  } catch (error) {
    console.error("Ingest error:", error);
    return res.status(500).json({ error: "Failed to ingest analytics events" });
  }
});

app.use(readAuthGate);
app.use(express.static(publicDir));

app.get("/api/plugin-analytics/summary", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);
    const summary = await fetchSummaryData(eventsCollection, match);
    return res.json({ from, to, ...summary });
  } catch (error) {
    console.error("Summary query failed:", error);
    return res.status(500).json({ error: "Failed to load summary" });
  }
});

app.get("/api/plugin-analytics/tool-usage", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);
    const tools = await fetchToolUsageData(eventsCollection, match);
    return res.json({ from, to, tools });
  } catch (error) {
    console.error("Tool usage query failed:", error);
    return res.status(500).json({ error: "Failed to load tool usage" });
  }
});

app.get("/api/plugin-analytics/heatmap", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);
    const compact = parseBool(req.query.compact, false);
    const limit = compact
      ? parseLimit(req.query.limit, 12000, 25000)
      : parseLimit(req.query.limit, 3000, 12000);
    const gridX = parseLimit(req.query.gridX, 96, 256);
    const gridY = parseLimit(req.query.gridY, 24, 128);
    const toolFilter = safeString(req.query.tool, 120);
    const heatmap = await fetchHeatmapData(eventsCollection, match, {
      toolFilter,
      limit,
      compact,
      gridX,
      gridY,
    });
    return res.json({ from, to, ...heatmap });
  } catch (error) {
    console.error("Heatmap query failed:", error);
    return res.status(500).json({ error: "Failed to load heatmap" });
  }
});

app.get("/api/plugin-analytics/sessions", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);
    const limit = parseLimit(req.query.limit, 60, 300);
    const sessions = await fetchSessionsData(eventsCollection, match, limit);
    return res.json({ from, to, sessions });
  } catch (error) {
    console.error("Session query failed:", error);
    return res.status(500).json({ error: "Failed to load sessions" });
  }
});

app.get("/api/plugin-analytics/recent-events", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);
    const limit = parseLimit(req.query.limit, 150, 1000);
    const events = await fetchRecentEventsData(eventsCollection, match, limit);
    return res.json({ from, to, events });
  } catch (error) {
    console.error("Recent events query failed:", error);
    return res.status(500).json({ error: "Failed to load recent events" });
  }
});

app.get("/api/plugin-analytics/features", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);
    const featureLimit = parseLimit(req.query.featureLimit, 120000, 250000);
    const featureCatalogLimit = parseLimit(req.query.featureCatalogLimit, 600, 2400);
    const features = await fetchFeatureAnalytics(eventsCollection, match, {
      limit: featureLimit,
      catalogLimit: featureCatalogLimit,
    });
    return res.json({ from, to, ...features });
  } catch (error) {
    console.error("Feature analytics query failed:", error);
    return res.status(500).json({ error: "Failed to load feature analytics" });
  }
});

app.get("/api/plugin-analytics/dashboard", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const engagementCollection = await getEngagementCollection();
    const { match, from, to } = buildMatch(req.query);
    const { match: baseMatch } = buildMatch(req.query, { includeAction: false });
    const toolFilter = safeString(req.query.tool, 120);
    const heatmapCompact = parseBool(req.query.heatmapCompact, true);
    const heatmapLimit = heatmapCompact
      ? parseLimit(req.query.heatmapLimit, 12000, 25000)
      : parseLimit(req.query.heatmapLimit, 3000, 12000);
    const heatmapGridX = parseLimit(req.query.heatmapGridX, 96, 256);
    const heatmapGridY = parseLimit(req.query.heatmapGridY, 24, 128);
    const sessionsLimit = parseLimit(req.query.sessionsLimit, 60, 300);
    const recentEventsLimit = parseLimit(req.query.eventsLimit, 100, 1000);

    const [summary, tools, heatmap, sessions, events, eventTypeBreakdown, actionCatalog, creditIntelligence, publicStats] = await Promise.all([
      fetchSummaryData(eventsCollection, match),
      fetchToolUsageData(eventsCollection, match),
      fetchHeatmapData(eventsCollection, match, {
        toolFilter,
        compact: heatmapCompact,
        limit: heatmapLimit,
        gridX: heatmapGridX,
        gridY: heatmapGridY,
      }),
      fetchSessionsData(eventsCollection, match, sessionsLimit),
      fetchRecentEventsData(eventsCollection, match, recentEventsLimit),
      fetchEventTypeBreakdown(eventsCollection, match),
      fetchActionCatalog(eventsCollection, baseMatch),
      fetchCreditIntelligence(eventsCollection),
      computePublicStatsMetrics(eventsCollection, engagementCollection),
    ]);

    return res.json({
      from,
      to,
      summary,
      actionCatalog: { actions: actionCatalog },
      toolUsage: { tools },
      eventTypeBreakdown,
      heatmap,
      sessions: { sessions },
      recentEvents: { events },
      creditIntelligence: {
        summary: creditIntelligence.summary,
        topPowerUsers: creditIntelligence.topPowerUsers,
        newsletter: creditIntelligence.newsletter,
      },
      publicStats,
    });
  } catch (error) {
    console.error("Dashboard query failed:", error);
    return res.status(500).json({ error: "Failed to load dashboard" });
  }
});

app.get("/api/plugin-analytics/mau", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    
    const [authMAU, anonMAU] = await Promise.all([
      eventsCollection.distinct("user.userId", {
        eventAt: { $gte: thirtyDaysAgo },
        "user.isAuthenticated": true,
        "user.userId": { $ne: null }
      }),
      eventsCollection.distinct("user.anonymousId", {
        eventAt: { $gte: thirtyDaysAgo },
        "user.isAuthenticated": false,
        "user.anonymousId": { $ne: null }
      })
    ]);

    res.json({
      mau: authMAU.length + anonMAU.length,
      authenticated: authMAU.length,
      anonymous: anonMAU.length,
      since: thirtyDaysAgo.toISOString()
    });
  } catch (error) {
    console.error("MAU query failed:", error);
    res.status(500).json({ error: "Failed to load MAU" });
  }
});

app.get("/api/plugin-analytics/credit-consumption", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    res.json({ users: intelligence.users });
  } catch (error) {
    console.error("Credit consumption query failed:", error);
    res.status(500).json({ error: "Failed to load credit consumption" });
  }
});

app.get("/api/plugin-analytics/user-top-tools", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    const users = intelligence.users
      .filter((user) => Array.isArray(user.topTools) && user.topTools.length > 0)
      .map((user) => ({
        _id: user._id,
        name: user.name || null,
        email: user.email || null,
        topTools: user.topTools,
      }));
    res.json({ users });
  } catch (error) {
    console.error("User top tools query failed:", error);
    res.status(500).json({ error: "Failed to load user top tools" });
  }
});

app.get("/api/plugin-analytics/credit-intelligence", readAuthGate, async (_req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    res.json(intelligence);
  } catch (error) {
    console.error("Credit intelligence query failed:", error);
    res.status(500).json({ error: "Failed to load credit intelligence" });
  }
});

app.get("/api/plugin-analytics/newsletter/runout-preview", readAuthGate, async (_req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    res.json(intelligence.newsletter);
  } catch (error) {
    console.error("Credit newsletter preview failed:", error);
    res.status(500).json({ error: "Failed to build newsletter preview" });
  }
});

app.post("/api/plugin-analytics/newsletter/runout-send", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    const recipients = parseEmailList(req.body && req.body.to ? req.body.to : null);
    const result = await sendCreditNewsletter(intelligence.newsletter, {
      to: recipients.length ? recipients : intelligence.newsletter.recipients,
    });
    res.json({
      ...result,
      preview: intelligence.newsletter,
    });
  } catch (error) {
    console.error("Credit newsletter send failed:", error);
    res.status(500).json({ error: "Failed to send newsletter" });
  }
});

app.get("/api/plugin-analytics/stats", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const engagementCollection = await getEngagementCollection();
    const stats = await computePublicStatsMetrics(eventsCollection, engagementCollection);
    const history = await buildPublicStatsHistory(eventsCollection, "7d");
    const today = history[history.length - 1] || null;
    const yesterday = history.length > 1 ? history[history.length - 2] : null;

    if (today && yesterday) {
      stats.deltas = {
        mau: Number(today.mau || 0) - Number(yesterday.mau || 0),
        likes: Number(today.likes || 0) - Number(yesterday.likes || 0),
        saves: Number(today.saves || 0) - Number(yesterday.saves || 0),
        follows: Number(today.follows || 0) - Number(yesterday.follows || 0),
        reused: Number(today.reuses || today.reused || 0) - Number(yesterday.reuses || yesterday.reused || 0),
      };
    } else {
      stats.deltas = null;
    }

    res.json(stats);
  } catch (error) {
    console.error("Stats query failed:", error);
    res.status(500).json({ error: "Failed to load stats" });
  }
});

app.post("/api/plugin-analytics/stats/:action", async (req, res) => {
  try {
    const { action } = req.params;
    const allowedActions = ["like", "save", "follow", "install", "reuse"];
    if (!allowedActions.includes(action)) {
      return res.status(400).json({ error: "Invalid action" });
    }

    const engagementCollection = await getEngagementCollection();
    const incrementFieldMap = {
      like: "likes",
      save: "saves",
      follow: "follows",
      install: "installs",
      reuse: "reused",
    };
    const incrementField = incrementFieldMap[action];
    const updateQuery = { $inc: { [incrementField]: 1 } };
    
    await engagementCollection.updateOne(
      { _id: "global_stats" },
      updateQuery,
      { upsert: true }
    );
    
    res.json({ success: true, action });
  } catch (error) {
    console.error("Stats update failed:", error);
    res.status(500).json({ error: "Failed to update stats" });
  }
});

app.get("/api/plugin-analytics/stats/history", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const range = safeString(req.query.range, 10) || "90d";
    const history = await buildPublicStatsHistory(eventsCollection, range);
    res.json(history);
  } catch (error) {
    console.error("Stats history query failed:", error);
    res.status(500).json({ error: "Failed to load stats history" });
  }
});

app.get("/api/plugin-analytics/stats/deltas", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const history = await buildPublicStatsHistory(eventsCollection, "30d");
    const today = history[history.length - 1] || null;
    const yesterday = history.length > 1 ? history[history.length - 2] : null;
    const lastWeek = history.length > 7 ? history[history.length - 8] : null;

    const metricKeys = ["mau", "authenticatedUsers", "likes", "saves", "follows", "reuses", "creditsConsumed", "activeSessions"];
    function extractMetrics(snap) {
      if (!snap) return null;
      const out = {};
      for (const key of metricKeys) {
        out[key] = snap[key] || 0;
      }
      return out;
    }

    function computeChanges(current, previous) {
      if (!current || !previous) return null;
      const out = {};
      for (const key of metricKeys) {
        out[key] = (current[key] || 0) - (previous[key] || 0);
      }
      return out;
    }

    const todayMetrics = extractMetrics(today);
    const yesterdayMetrics = extractMetrics(yesterday);
    const lastWeekMetrics = extractMetrics(lastWeek);

    res.json({
      today: todayMetrics,
      yesterday: yesterdayMetrics,
      lastWeek: lastWeekMetrics,
      changes: {
        daily: computeChanges(todayMetrics, yesterdayMetrics),
        weekly: computeChanges(todayMetrics, lastWeekMetrics),
      },
    });
  } catch (error) {
    console.error("Stats deltas query failed:", error);
    res.status(500).json({ error: "Failed to load stats deltas" });
  }
});

app.get("/api/plugin-analytics/credit-consumption/by-tool", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const intelligence = await fetchCreditIntelligence(eventsCollection);
    res.json({ tools: intelligence.toolBreakdown });
  } catch (error) {
    console.error("Credit consumption by tool query failed:", error);
    res.status(500).json({ error: "Failed to load credit consumption by tool" });
  }
});

app.get("/api/plugin-analytics/retention", readAuthGate, async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

    // Find each user's first session date
    const userFirstSeen = await eventsCollection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: thirtyDaysAgo },
            sessionId: { $ne: null },
          },
        },
        {
          $group: {
            _id: {
              $cond: [
                { $eq: ["$user.isAuthenticated", true] },
                "$user.userId",
                "$user.anonymousId",
              ],
            },
            firstSeen: { $min: "$eventAt" },
            allDates: {
              $addToSet: {
                $dateToString: { format: "%Y-%m-%d", date: "$eventAt" },
              },
            },
          },
        },
        { $match: { _id: { $ne: null } } },
      ])
      .toArray();

    // Group by first-seen date and compute retention
    const cohortMap = new Map();
    for (const user of userFirstSeen) {
      const firstDate = new Date(user.firstSeen).toISOString().slice(0, 10);
      if (!cohortMap.has(firstDate)) {
        cohortMap.set(firstDate, { totalUsers: 0, day1: 0, day7: 0, day14: 0, day30: 0 });
      }
      const cohort = cohortMap.get(firstDate);
      cohort.totalUsers += 1;

      const firstMs = new Date(firstDate).getTime();
      const dateSet = new Set(user.allDates);

      for (const [dayLabel, dayOffset] of [["day1", 1], ["day7", 7], ["day14", 14], ["day30", 30]]) {
        const targetDate = new Date(firstMs + dayOffset * 24 * 60 * 60 * 1000)
          .toISOString()
          .slice(0, 10);
        if (dateSet.has(targetDate)) {
          cohort[dayLabel] += 1;
        }
      }
    }

    const cohorts = Array.from(cohortMap.entries())
      .map(([firstSeenDate, data]) => ({
        firstSeenDate,
        totalUsers: data.totalUsers,
        day1: data.totalUsers > 0 ? Number((data.day1 / data.totalUsers).toFixed(4)) : 0,
        day7: data.totalUsers > 0 ? Number((data.day7 / data.totalUsers).toFixed(4)) : 0,
        day14: data.totalUsers > 0 ? Number((data.day14 / data.totalUsers).toFixed(4)) : 0,
        day30: data.totalUsers > 0 ? Number((data.day30 / data.totalUsers).toFixed(4)) : 0,
      }))
      .sort((a, b) => a.firstSeenDate.localeCompare(b.firstSeenDate));

    res.json({ cohorts });
  } catch (error) {
    console.error("Retention query failed:", error);
    res.status(500).json({ error: "Failed to load retention data" });
  }
});

app.get("*", (_req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});

async function startServer() {
  try {
    await initializeServer();
    app.listen(PORT, () => {
      console.log(`plugin-data-dashboard listening on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

export default app;

const isDirectRun =
  Boolean(process.argv[1]) && path.resolve(process.argv[1]) === __filename;

if (isDirectRun) {
  startServer();
}
