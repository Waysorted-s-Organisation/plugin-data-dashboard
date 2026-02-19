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
const READ_USER = (process.env.DASHBOARD_BASIC_AUTH_USER || "").trim();
const READ_PASS = (process.env.DASHBOARD_BASIC_AUTH_PASS || "").trim();
let initializationPromise = null;
let isInitialized = false;

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

function normalizeUser(user) {
  if (!user || typeof user !== "object") {
    return {
      isAuthenticated: false,
      userId: null,
      name: null,
      email: null,
    };
  }

  const userId =
    safeString(user.userId) ||
    safeString(user.id) ||
    safeString(user._id) ||
    safeString(user.email);

  return {
    isAuthenticated: Boolean(user.isAuthenticated || userId || user.email),
    userId,
    name: safeString(user.name, 120),
    email: safeString(user.email, 160),
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

function buildMatch(query) {
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

  return { match, from, to };
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
      user: normalizeUser(body.user),
    };

    const docs = inputEvents.slice(0, 1000).map((event) => {
      const eventType = safeString(event && (event.eventType || event.type), 120) || "unknown_event";
      const eventAt = toDate(event && (event.eventAt || event.timestamp), envelope.sentAt || now);
      const user = normalizeUser((event && event.user) || envelope.user);

      return {
        sessionId: safeString(event && event.sessionId, 120) || envelope.sessionId || "unknown-session",
        deviceId: safeString(event && event.deviceId, 120) || envelope.deviceId || "unknown-device",
        eventType,
        eventAt,
        receivedAt: now,
        source: safeString(event && event.source, 80) || envelope.source,
        tool:
          safeString(event && event.tool, 120) ||
          safeString(event && event.payload && event.payload.uiTool, 120) ||
          "unknown",
        payload: sanitizePayload(event && event.payload),
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

    const [totalEvents, uniqueSessionsRaw, uniqueUsersRaw, anonymousEvents, sessionDurationAgg, topTools, topActions, eventsByDay] =
      await Promise.all([
        eventsCollection.countDocuments(match),
        eventsCollection.distinct("sessionId", match),
        eventsCollection.distinct("user.userId", {
          ...match,
          "user.isAuthenticated": true,
          "user.userId": { $ne: null },
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
                timeSpentMs: { $round: ["$timeSpentMs", 2] },
              },
            },
            { $sort: { events: -1 } },
            { $limit: 12 },
          ])
          .toArray(),
        eventsCollection
          .aggregate([
            { $match: match },
            {
              $project: {
                action: {
                  $ifNull: ["$payload.action", "$eventType"],
                },
              },
            },
            {
              $group: {
                _id: "$action",
                count: { $sum: 1 },
              },
            },
            { $sort: { count: -1 } },
            { $limit: 15 },
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

    return res.json({
      from,
      to,
      kpis: {
        totalEvents,
        totalSessions: uniqueSessionsRaw.length,
        authenticatedUsers: uniqueUsersRaw.length,
        anonymousEvents,
        avgSessionDurationMs: Math.round(toNumber(durationInfo.avgSessionDurationMs, 0)),
        maxSessionDurationMs: Math.round(toNumber(durationInfo.maxSessionDurationMs, 0)),
      },
      topTools,
      topActions,
      eventsByDay,
    });
  } catch (error) {
    console.error("Summary query failed:", error);
    return res.status(500).json({ error: "Failed to load summary" });
  }
});

app.get("/api/plugin-analytics/tool-usage", async (req, res) => {
  try {
    const eventsCollection = await getEventsCollection();
    const { match, from, to } = buildMatch(req.query);

    const tools = await eventsCollection
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: "$tool",
            eventCount: { $sum: 1 },
            sessionIds: { $addToSet: "$sessionId" },
            users: { $addToSet: "$user.userId" },
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
            sessionCount: { $size: "$sessionIds" },
            userCount: {
              $size: {
                $setDifference: ["$users", [null]],
              },
            },
            timeSpentMs: { $round: ["$timeSpentMs", 2] },
          },
        },
        { $sort: { eventCount: -1 } },
      ])
      .toArray();

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
    const limit = parseLimit(req.query.limit, 4000, 20000);

    const toolFilter = safeString(req.query.tool, 120);
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
            elementTag: "$payload.element.tag",
            elementId: "$payload.element.id",
            elementToolId: "$payload.element.toolId",
          },
        },
      ])
      .toArray();

    return res.json({ from, to, points, count: points.length });
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

    const sessions = await eventsCollection
      .aggregate([
        { $match: match },
        { $sort: { eventAt: 1 } },
        {
          $group: {
            _id: "$sessionId",
            startedAt: { $first: "$eventAt" },
            endedAt: { $last: "$eventAt" },
            eventCount: { $sum: 1 },
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
            user: 1,
            tools: 1,
            lastSource: 1,
          },
        },
        { $sort: { endedAt: -1 } },
        { $limit: limit },
      ])
      .toArray();

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

    const events = await eventsCollection
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

    return res.json({ from, to, events });
  } catch (error) {
    console.error("Recent events query failed:", error);
    return res.status(500).json({ error: "Failed to load recent events" });
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
