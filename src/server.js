import path from "path";
import { fileURLToPath } from "url";

import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import helmet from "helmet";
import morgan from "morgan";

import { ensureIndexes, getEventsCollection } from "./db.js";

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
  "session_heartbeat",
  "ui_heartbeat",
  "ui_state_snapshot",
  "ui_visibility_change",
  "analytics_transport_updated",
];
const PASSIVE_ACTION_KEYS = [
  "get-ui-state",
  "notify",
  "ui-loaded",
  "set-analytics-endpoint",
  "analytics-event",
  "analytics-batch",
  "analytics-identify",
  "analytics-flush",
];
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
  "import-tool": "import-tool",
  import_tool: "import-tool",
  "unit-converter": "unit-converter",
  unit_converter: "unit-converter",
  profile: "profile",
  "wayfall-game": "wayfall-game",
  game: "wayfall-game",
  "liquid-glass": "liquid-glass",
  liquid_glass: "liquid-glass",
  unknown: "unknown",
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
};

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

  return {
    isAuthenticated,
    userId,
    anonymousId,
    name: isAuthenticated ? safeString(user.name, 120) : null,
    email: isAuthenticated ? safeString(user.email, 160) : null,
    identitySource,
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

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "plugin-data-dashboard",
    initialized: isInitialized,
  });
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
  const cursor = eventsCollection
    .find(match)
    .project({
      _id: 0,
      eventAt: 1,
      eventType: 1,
      tool: 1,
      payload: 1,
      sessionId: 1,
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

  for await (const event of cursor) {
    const payload =
      event && event.payload && typeof event.payload === "object"
        ? event.payload
        : {};
    const nestedPayload = resolveNestedPayload(event);
    const action = resolveEventAction(event);
    const normalizedAction = String(action || "").toLowerCase();
    const eventType = String(event && event.eventType ? event.eventType : "");

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

    const docs = inputEvents.slice(0, 1000).map((event) => {
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
    const features = await fetchFeatureAnalytics(eventsCollection, match, {
      limit: featureLimit,
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

    const [summary, tools, heatmap, sessions, events, eventTypeBreakdown, actionCatalog] = await Promise.all([
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
    });
  } catch (error) {
    console.error("Dashboard query failed:", error);
    return res.status(500).json({ error: "Failed to load dashboard" });
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
