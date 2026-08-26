import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

import cors from "cors";
import dotenv from "dotenv";
import express from "express";
import helmet from "helmet";
import morgan from "morgan";

import {
  ensureIndexes,
  getBackendUsersCollection,
  getEventsCollection,
} from "./db.js";
import { enrichCampaignRecipients } from "./newsletter-recipients.js";
import {
  creditOverview,
  creditUserDetail,
  creditUsers,
  newsletterCreditProfile,
  operationsHealth,
  recentUsers,
} from "./operations.js";
import {
  productCommercial,
  productDataHealth,
  productFeedback,
  productLifecycle,
  productSummary,
  productToolDetail,
  productTools,
  productUserDetail,
  productUsers,
  NON_PERSISTED_EVENT_TYPES,
  SEMANTIC_EVENT_TYPES,
} from "./product-intelligence.js";

/**
 * Shared vocabulary between the ingest and the aggregations. Events outside it
 * are still stored — they are simply flagged so consumers can choose between
 * the curated set and the complete record.
 */
const SEMANTIC_EVENT_TYPE_SET = new Set(SEMANTIC_EVENT_TYPES);
const NON_PERSISTED_EVENT_TYPE_SET = new Set(NON_PERSISTED_EVENT_TYPES);

/**
 * Whether repetitive, information-free traffic (heartbeats, transport config
 * changes) is persisted.
 *
 * Defaults to false, matching the documented contract: these are excluded from
 * every metric anyway, so storing a heartbeat every 30 seconds per open plugin
 * only grows the collection. Set to true temporarily when debugging a plugin
 * build, where the heartbeat's queue depth and uptime are the useful signal.
 *
 * This is deliberately narrower than PASSIVE_EVENT_TYPES: events such as
 * backend_operation and user_context_changed do not count as user activity but
 * are still evidence of what happened, so they are always stored.
 */
function storePassiveEvents() {
  return String(process.env.ANALYTICS_STORE_PASSIVE_EVENTS || "").trim().toLowerCase() === "true";
}

dotenv.config();

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, "..", "public");
const PORT = Number(process.env.PORT || 4080);
let analyticsInitializationPromise = null;
const operationsApiMetrics = { startedAt: new Date(), requests: 0, failures: 0, totalDurationMs: 0 };

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json({ limit: "5mb" }));
morgan.token("safe-url", (req) => {
  try {
    const parsed = new URL(req.originalUrl || req.url, "http://dashboard.local");
    if (parsed.searchParams.has("search")) parsed.searchParams.set("search", "[redacted]");
    return `${parsed.pathname}${parsed.search}`;
  } catch {
    return req.path || "[unavailable]";
  }
});
app.use(morgan(":method :safe-url :status :res[content-length] - :response-time ms"));
app.use((req, res, next) => {
  if (!req.path.startsWith("/api/operations/")) return next();
  const startedAt = process.hrtime.bigint();
  res.once("finish", () => {
    operationsApiMetrics.requests += 1;
    operationsApiMetrics.totalDurationMs += Number(process.hrtime.bigint() - startedAt) / 1e6;
    if (res.statusCode >= 500) operationsApiMetrics.failures += 1;
  });
  return next();
});

function safeString(value, maxLength = 180) {
  if (value === null || value === undefined) return null;
  const text = String(value);
  return text.length <= maxLength ? text : `${text.slice(0, maxLength)}…`;
}

function safeDate(value, fallback) {
  const parsed = value ? new Date(value) : fallback;
  return parsed && !Number.isNaN(parsed.getTime()) ? parsed : fallback;
}

function sanitizeObject(value, depth = 0) {
  if (!value || typeof value !== "object" || depth > 2) return {};
  const output = {};
  for (const key of Object.keys(value).slice(0, 40)) {
    const item = value[key];
    if (typeof item === "string") output[key] = safeString(item, 800);
    else if (typeof item === "number" || typeof item === "boolean" || item === null) output[key] = item;
    else if (Array.isArray(item)) output[key] = item.slice(0, 30);
    else if (typeof item === "object") output[key] = sanitizeObject(item, depth + 1);
  }
  return output;
}

function anonymousId(seed) {
  if (!seed) return null;
  return `device_${crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 20)}`;
}

function normalizeUser(value, fallbackSeed) {
  const user = value && typeof value === "object" ? value : {};
  // Email is excluded from the id resolution on purpose. It used to be the last
  // fallback, so user.userId held an account id on some events and an email
  // address on others; the same person then resolved to two identities and
  // neither joined reliably against the backend users collection. Email is
  // still captured in its own field below and can be resolved to a real user
  // record there.
  const inferredId = safeString(user.userId || user.id || user._id, 180);
  const inferredEmail = safeString(user.email, 200);
  // An identifier the plugin actually sent is evidence, and outranks a flag
  // that says otherwise. The flag used to decide alone, and it is set from the
  // plugin's own auth state machine — which lags the token it already holds, so
  // the opening events of every launch arrived as isAuthenticated:false while
  // carrying a real account id. Those ids were then blanked on the way in and
  // the person became a signed-out visitor in their own session.
  const identified = Boolean(inferredId || inferredEmail);
  const isAuthenticated = identified || user.isAuthenticated === true;
  const creditValue = Number(
    user.creditsRemaining ?? user.billing?.wallet?.availableCredits
  );
  return {
    isAuthenticated,
    // Never discarded once sent. Blanking them on the strength of the flag threw
    // away the only join the dashboard has.
    userId: inferredId,
    anonymousId: identified
      ? null
      : safeString(user.anonymousId || user.anonId, 180) || anonymousId(fallbackSeed),
    // Kept for signed-out visitors too. Figma exposes their display name to
    // the plugin and names them in the publisher's usage notifications, so
    // discarding it here left a visitor recognisable in Figma but anonymous in
    // the dashboard — which is the gap that made them unreachable.
    name: safeString(user.name, 160),
    email: inferredEmail,
    identitySource: safeString(user.identitySource, 80) || (isAuthenticated ? "authenticated" : "anonymous"),
    creditsRemaining: Number.isFinite(creditValue) ? Math.max(0, creditValue) : null,
  };
}

function newsletterConfig() {
  return {
    apiUrl: String(process.env.NEWSLETTER_API_URL || "").trim().replace(/\/+$/, ""),
    token: String(process.env.NEWSLETTER_MANAGEMENT_TOKEN || "").trim(),
    timeoutMs: Math.max(1000, Number(process.env.NEWSLETTER_PROXY_TIMEOUT_MS || 15000)),
  };
}

/**
 * Whether an unauthenticated dashboard is a deliberate choice.
 *
 * Local development is the only legitimate reason to run without credentials,
 * and `npm run dev` sets this. It is deliberately NOT in `.env.example`: that
 * file gets copied onto servers, and an escape hatch that travels with it is
 * the same hole under a different name.
 */
function allowsUnauthenticated() {
  return String(process.env.ALLOW_UNAUTHENTICATED || "").trim().toLowerCase() === "true";
}

let warnedAboutMissingCredentials = false;

/**
 * Constant-time credential comparison.
 *
 * timingSafeEqual requires equal lengths and throws otherwise, which would leak
 * the length it refused to compare, so both sides are hashed to a fixed width
 * first.
 */
function matchesSecret(supplied, expected) {
  const digest = (value) => crypto.createHash("sha256").update(String(value), "utf8").digest();
  return crypto.timingSafeEqual(digest(supplied), digest(expected));
}

/**
 * The only authentication in front of the operations APIs, the newsletter
 * proxy and the dashboard UI.
 *
 * It used to call next() when either credential was empty, and `.env.example`
 * ships both empty — so a deployment that followed the README's copy-the-example
 * setup served the entire customer database, and a proxy holding a privileged
 * mass-send token, to anyone who knew the URL. Nothing said so: no error, no log
 * line, no startup check. Production had the credentials set and was never
 * exposed, but the failure mode is silent, so a rename, a lost environment or a
 * fresh preview project reopens everything without a signal.
 *
 * It now refuses to serve instead. 503 rather than 401 because the problem is
 * the deployment, not the caller — there are no credentials for them to send.
 *
 * The `WWW-Authenticate` realm below must not change: browsers cache Basic
 * credentials per realm, and renaming it signs every existing operator out.
 *
 * Everything the plugin depends on — the analytics session exchange, the ingest
 * endpoint — and the public `/health` check are all registered ahead of this
 * middleware, so refusing here cannot interrupt telemetry or health probes.
 */
function readAuthGate(req, res, next) {
  const expectedUser = String(process.env.DASHBOARD_BASIC_AUTH_USER || "").trim();
  const expectedPass = String(process.env.DASHBOARD_BASIC_AUTH_PASS || "").trim();
  if (!expectedUser || !expectedPass) {
    if (allowsUnauthenticated()) {
      if (!warnedAboutMissingCredentials) {
        warnedAboutMissingCredentials = true;
        console.warn(
          "ALLOW_UNAUTHENTICATED=true: serving the dashboard and every operations API without authentication. Never set this outside local development."
        );
      }
      return next();
    }
    if (!warnedAboutMissingCredentials) {
      warnedAboutMissingCredentials = true;
      console.error(
        "DASHBOARD_BASIC_AUTH_USER and DASHBOARD_BASIC_AUTH_PASS are not configured. Refusing to serve the dashboard, the operations APIs and the newsletter proxy. Set both, or set ALLOW_UNAUTHENTICATED=true for local development."
      );
    }
    return res.status(503).json({ error: "Dashboard authentication is not configured" });
  }
  const authorization = String(req.headers.authorization || "");
  if (!authorization.startsWith("Basic ")) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Waysorted Operations"');
    return res.status(401).json({ error: "Authentication required" });
  }
  const decoded = Buffer.from(authorization.slice(6), "base64").toString("utf8");
  const separator = decoded.indexOf(":");
  const user = separator >= 0 ? decoded.slice(0, separator) : "";
  const pass = separator >= 0 ? decoded.slice(separator + 1) : "";
  // Both halves are always compared, so the answer does not arrive sooner for a
  // wrong username than for a wrong password.
  const userMatches = matchesSecret(user, expectedUser);
  const passMatches = matchesSecret(pass, expectedPass);
  if (!userMatches || !passMatches) {
    return res.status(403).json({ error: "Invalid credentials" });
  }
  return next();
}

function ingestAuthGate(req, res, next) {
  const token = String(process.env.ANALYTICS_INGEST_TOKEN || "").trim();
  const required = String(process.env.ANALYTICS_INGEST_TOKEN_REQUIRED || "").trim().toLowerCase() === "true";
  const sessionToken = safeString(req.headers["x-plugin-ingest-session"], 2000);
  if (sessionToken && verifyAnalyticsSessionToken(sessionToken)) return next();
  if (!required) return next();
  if (token && safeString(req.headers["x-plugin-ingest-token"], 240) === token) return next();
  return res.status(401).json({ error: "Invalid ingest token" });
}

function analyticsSigningSecret() {
  return String(process.env.ANALYTICS_SIGNING_SECRET || "").trim();
}

function signAnalyticsSession(payload) {
  const secret = analyticsSigningSecret();
  if (!secret) return null;
  const encoded = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signature = crypto.createHmac("sha256", secret).update(encoded).digest("base64url");
  return `${encoded}.${signature}`;
}

function verifyAnalyticsSessionToken(token) {
  const secret = analyticsSigningSecret();
  if (!secret || typeof token !== "string") return null;
  const [encoded, signature] = token.split(".");
  if (!encoded || !signature) return null;
  const expected = crypto.createHmac("sha256", secret).update(encoded).digest();
  let supplied;
  try { supplied = Buffer.from(signature, "base64url"); } catch { return null; }
  if (expected.length !== supplied.length || !crypto.timingSafeEqual(expected, supplied)) return null;
  try {
    const payload = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (payload.aud !== "plugin-analytics" || Number(payload.exp) <= Math.floor(Date.now() / 1000)) return null;
    return payload;
  } catch { return null; }
}

app.post("/api/plugin-analytics/session", async (req, res) => {
  const base = String(process.env.WAYSORTED_API_URL || "").trim().replace(/\/+$/, "");
  const authorization = String(req.headers.authorization || "");
  if (!base || !analyticsSigningSecret()) return res.status(503).json({ error: "Semantic analytics sessions are not configured" });
  if (!authorization.startsWith("Bearer ")) return res.status(401).json({ error: "Waysorted authentication required" });
  try {
    const pathName = String(process.env.WAYSORTED_ANALYTICS_PROFILE_PATH || "/api/user/profile");
    const profileResponse = await fetch(new URL(pathName, `${base}/`), { headers: { Accept: "application/json", Authorization: authorization }, signal: AbortSignal.timeout(8000) });
    if (!profileResponse.ok) return res.status(401).json({ error: "Waysorted session is not valid" });
    const profile = await profileResponse.json();
    const subject = safeString(profile.id || profile._id || profile.userId || profile.email, 180);
    if (!subject) return res.status(401).json({ error: "Waysorted identity is unavailable" });
    const now = Math.floor(Date.now() / 1000); const ttl = Math.min(3600, Math.max(300, Number(process.env.ANALYTICS_SESSION_TTL_SECONDS || 900)));
    const session = signAnalyticsSession({ sub: subject, aud: "plugin-analytics", iat: now, exp: now + ttl, schema: 1 });
    res.setHeader("Cache-Control", "no-store");
    return res.json({ token: session, expiresAt: new Date((now + ttl) * 1000).toISOString(), ingestUrl: "/api/plugin-analytics/ingest", schemaVersion: 1 });
  } catch (error) {
    console.error("Analytics session bootstrap failed:", error?.message || error);
    return res.status(502).json({ error: "Waysorted authentication could not be verified" });
  }
});

async function ensureAnalyticsReady() {
  analyticsInitializationPromise ||= ensureIndexes().catch((error) => {
    analyticsInitializationPromise = null;
    throw error;
  });
  await analyticsInitializationPromise;
}

app.get("/health", (_req, res) => {
  const newsletter = newsletterConfig();
  res.json({
    ok: true,
    service: "waysorted-operations-dashboard",
    newsletterIntegrationConfigured: Boolean(newsletter.apiUrl && newsletter.token),
  });
});

app.post("/api/plugin-analytics/ingest", ingestAuthGate, async (req, res) => {
  try {
    const body = req.body || {};
    const inputEvents = Array.isArray(body.events) ? body.events : [];
    if (!inputEvents.length) return res.status(400).json({ error: "events[] is required" });
    await ensureAnalyticsReady();
    const now = new Date();
    const envelope = {
      source: safeString(body.source, 80) || "unknown",
      sessionId: safeString(body.sessionId, 120),
      deviceId: safeString(body.deviceId, 120),
      sentAt: safeDate(body.sentAt, now),
      runtime: sanitizeObject(body.runtime),
      plugin: sanitizeObject(body.plugin),
      user: normalizeUser(body.user, body.deviceId || body.sessionId),
    };
    const documents = inputEvents.slice(0, 1000).map((event) => {
      const sessionId = safeString(event?.sessionId, 120) || envelope.sessionId || "unknown-session";
      const deviceId = safeString(event?.deviceId, 120) || envelope.deviceId || "unknown-device";
      const payload = sanitizeObject(event?.payload);
      const eventAt = safeDate(event?.eventAt || event?.timestamp, envelope.sentAt);
      const eventType = safeString(event?.eventType || event?.type, 120) || "unknown_event";
      const eventId = safeString(event?.eventId, 180) || crypto.createHash("sha256").update(`${sessionId}|${eventType}|${eventAt.toISOString()}|${JSON.stringify(payload)}`).digest("hex");
      return {
        eventId,
        schemaVersion: Math.max(1, Number(event?.schemaVersion || body.schemaVersion || 1)),
        sessionId,
        deviceId,
        eventType,
        // Emitters from schemaVersion 2 onward send every event and mark which
        // belong to the curated semantic vocabulary. Events from older builds
        // predate the flag but were filtered to semantic types before sending,
        // so they are semantic by construction.
        isSemantic:
          typeof event?.isSemantic === "boolean"
            ? event.isSemantic
            : SEMANTIC_EVENT_TYPE_SET.has(eventType),
        eventAt,
        receivedAt: now,
        source: safeString(event?.source, 80) || envelope.source,
        tool: safeString(event?.tool || payload.uiTool, 120) || "unknown",
        payload,
        user: normalizeUser(event?.user || envelope.user, deviceId || sessionId),
        runtime: envelope.runtime,
        plugin: envelope.plugin,
      };
    });
    // Passive events are dropped before the write unless explicitly enabled.
    // They are excluded from every metric regardless, so persisting them only
    // grows the collection — but the counts are still reported so a plugin
    // build emitting nothing but noise is visible without storing the noise.
    const keepPassive = storePassiveEvents();
    const stored = keepPassive
      ? documents
      : documents.filter((document) => !NON_PERSISTED_EVENT_TYPE_SET.has(document.eventType));
    const dropped = documents.length - stored.length;
    const dropReasons = dropped ? { non_persisted_event_type: dropped } : {};

    const result = stored.length
      ? await (await getEventsCollection()).bulkWrite(stored.map((document) => ({ updateOne: { filter: { eventId: document.eventId }, update: { $setOnInsert: document }, upsert: true } })), { ordered: false })
      : { upsertedCount: 0 };
    return res.status(202).json({
      accepted: documents.length,
      stored: stored.length,
      inserted: result.upsertedCount,
      duplicates: stored.length - result.upsertedCount,
      dropped,
      dropReasons,
    });
  } catch (error) {
    console.error("Analytics ingest failed:", error?.message || error);
    return res.status(500).json({ error: "Failed to ingest analytics events" });
  }
});

const NEWSLETTER_PROXY_PATHS = [
  /^\/overview$/,
  /^\/analytics$/,
  /^\/system-health$/,
  /^\/automations$/,
  /^\/automations\/[a-z0-9_:-]+$/,
  /^\/campaigns$/,
  /^\/campaigns\/preview$/,
  /^\/campaigns\/audience-preview$/,
  /^\/campaigns\/\d+$/,
  /^\/campaigns\/\d+\/actions$/,
  /^\/campaigns\/\d+\/test-send$/,
  /^\/subscribers$/,
  /^\/subscribers\/\d+$/,
  /^\/subscribers\/import$/,
  /^\/content\/templates$/,
  /^\/templates$/,
  /^\/templates\/\d+$/,
  /^\/templates\/\d+\/preview$/,
];

function newsletterMutationHasTrustedOrigin(req) {
  if (!["POST", "PUT", "DELETE"].includes(req.method)) return true;
  const origin = safeString(req.headers.origin, 500);
  if (!origin) return true;
  const protocol = String(req.headers["x-forwarded-proto"] || req.protocol || "https").split(",")[0].trim();
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0].trim();
  return Boolean(host) && origin === `${protocol}://${host}`;
}

async function newsletterProxy(req, res) {
  const newsletter = newsletterConfig();
  if (!newsletter.apiUrl || !newsletter.token) return res.status(503).json({ error: "Newsletter integration is not configured" });
  if (!NEWSLETTER_PROXY_PATHS.some((pattern) => pattern.test(req.path))) return res.status(404).json({ error: "Unsupported newsletter endpoint" });
  if (!["GET", "POST", "PUT", "DELETE"].includes(req.method)) return res.status(405).json({ error: "Method not allowed" });
  if (!newsletterMutationHasTrustedOrigin(req)) return res.status(403).json({ error: "Cross-origin mutation blocked" });

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), newsletter.timeoutMs);
  const target = new URL(`/api/management${req.path}`, `${newsletter.apiUrl}/`);
  for (const [key, value] of Object.entries(req.query || {})) {
    if (Array.isArray(value)) value.forEach((item) => target.searchParams.append(key, String(item)));
    else if (value !== undefined && value !== null) target.searchParams.set(key, String(value));
  }
  try {
    const hasBody = ["POST", "PUT", "DELETE"].includes(req.method) && req.body && Object.keys(req.body).length;
    let requestBody = req.body;
    if (
      req.method === "POST"
      && req.path === "/campaigns"
      && req.body?.recipient_source
    ) {
      try {
        const users = await (await getBackendUsersCollection())
          .find(
            { email: { $type: "string" }, name: { $type: "string", $ne: "" } },
            { projection: { _id: 0, email: 1, name: 1 } }
          )
          .limit(10000)
          .toArray();
        requestBody = enrichCampaignRecipients(req.body, users);
      } catch (error) {
        console.error(
          "Newsletter recipient name enrichment failed:",
          error?.message || error
        );
        return res.status(503).json({
          error: "Recipient names could not be loaded; campaign was not created",
          code: "RECIPIENT_NAME_ENRICHMENT_UNAVAILABLE",
        });
      }
    }
    const upstream = await fetch(target, {
      method: req.method,
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${newsletter.token}`,
        ...(hasBody ? { "Content-Type": "application/json" } : {}),
      },
      body: hasBody ? JSON.stringify(requestBody) : undefined,
      signal: controller.signal,
    });
    const body = await upstream.text();
    const contentType = upstream.headers.get("content-type");
    res.setHeader("Cache-Control", "no-store");
    if (contentType) res.setHeader("Content-Type", contentType);
    return res.status(upstream.status).send(body);
  } catch (error) {
    const timeout = error?.name === "AbortError";
    console.error("Newsletter proxy failed:", error?.message || error);
    return res.status(timeout ? 504 : 502).json({ error: timeout ? "Newsletter service timed out" : "Newsletter service is unavailable" });
  } finally {
    clearTimeout(timer);
  }
}

async function newsletterCustomerProfile(req, res) {
  const newsletter = newsletterConfig();
  if (!newsletter.apiUrl || !newsletter.token) return res.status(503).json({ error: "Newsletter integration is not configured" });
  const subscriberId = Number(req.params.subscriberId);
  if (!Number.isInteger(subscriberId) || subscriberId <= 0) return res.status(400).json({ error: "Invalid subscriber id" });
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), newsletter.timeoutMs);
  try {
    const target = new URL(`/api/management/subscribers/${subscriberId}`, `${newsletter.apiUrl}/`);
    const upstream = await fetch(target, {
      headers: { Accept: "application/json", Authorization: `Bearer ${newsletter.token}` },
      signal: controller.signal,
    });
    const profile = await upstream.json().catch(() => ({}));
    if (!upstream.ok) return res.status(upstream.status).json(profile);
    const notification = profile.notification_profile || {};
    const credit = await newsletterCreditProfile(
      safeString(notification.external_user_id, 255),
      notification.email || profile.subscriber?.email || null
    );
    res.setHeader("Cache-Control", "no-store");
    return res.json({ ...profile, ...credit });
  } catch (error) {
    const timeout = error?.name === "AbortError";
    console.error("Newsletter customer aggregation failed:", error?.message || error);
    return res.status(timeout ? 504 : 502).json({ error: timeout ? "Customer profile request timed out" : "Customer profile is unavailable" });
  } finally {
    clearTimeout(timer);
  }
}

async function newsletterAudienceIndex() {
  const newsletter = newsletterConfig();
  if (!newsletter.apiUrl || !newsletter.token) return new Map();
  const index = new Map();
  for (let page = 1; page <= 20; page += 1) {
    const target = new URL("/api/management/subscribers", `${newsletter.apiUrl}/`);
    target.searchParams.set("page", String(page));
    target.searchParams.set("per_page", "100");
    const response = await fetch(target, {
      headers: { Accept: "application/json", Authorization: `Bearer ${newsletter.token}` },
      signal: AbortSignal.timeout(newsletter.timeoutMs),
    });
    if (!response.ok) throw new Error(`Newsletter audience request failed (${response.status})`);
    const body = await response.json();
    for (const subscriber of body.subscribers || []) {
      const email = String(subscriber.email || "").trim().toLowerCase();
      if (email) index.set(email, subscriber);
    }
    if (page >= Number(body.pages || 1)) break;
  }
  return index;
}

async function newsletterProfileForEmail(email) {
  const index = await newsletterAudienceIndex();
  const subscriber = index.get(String(email || "").trim().toLowerCase());
  if (!subscriber?.id) return null;
  const newsletter = newsletterConfig();
  const target = new URL(`/api/management/subscribers/${subscriber.id}`, `${newsletter.apiUrl}/`);
  const response = await fetch(target, {
    headers: { Accept: "application/json", Authorization: `Bearer ${newsletter.token}` },
    signal: AbortSignal.timeout(newsletter.timeoutMs),
  });
  return response.ok ? response.json() : { subscriber };
}

function operationsFailure(res, error) {
  const unavailable = error?.code === "BACKEND_DATABASE_NOT_CONFIGURED";
  console.error("Operations API failed:", error?.message || error);
  res.setHeader("Cache-Control", "no-store");
  return res.status(unavailable ? 503 : 500).json({
    error: unavailable ? "Backend database is not configured" : "Operations data is unavailable",
    code: unavailable ? "BACKEND_DATABASE_NOT_CONFIGURED" : "OPERATIONS_DATA_UNAVAILABLE",
  });
}

app.use(readAuthGate);

app.get("/api/operations/credits/overview", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await creditOverview(req.query.days || 30)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/credits/users", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await creditUsers(req.query)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/credits/users/:userId", async (req, res) => {
  try {
    const detail = await creditUserDetail(req.params.userId, req.query.days || 30);
    res.setHeader("Cache-Control", "no-store");
    return detail ? res.json(detail) : res.status(404).json({ error: "User not found" });
  } catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/activity/recent-users", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await recentUsers(req.query)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/summary", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await productSummary(req.query.days || 30)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/users", async (req, res) => {
  try {
    let newsletter = new Map();
    try { newsletter = await newsletterAudienceIndex(); } catch (error) { console.error("Newsletter audience join unavailable:", error?.message || error); }
    res.setHeader("Cache-Control", "no-store");
    return res.json(await productUsers(req.query, newsletter));
  } catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/users/:userId", async (req, res) => {
  try {
    const detail = await productUserDetail(req.params.userId);
    if (!detail) return res.status(404).json({ error: "User not found" });
    let newsletter = null;
    try { newsletter = await newsletterProfileForEmail(detail.user.email); } catch (error) { console.error("Newsletter profile join unavailable:", error?.message || error); }
    res.setHeader("Cache-Control", "no-store");
    return res.json({ ...detail, newsletter });
  } catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/tools", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await productTools(req.query.days || 30)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/tools/:toolCode", async (req, res) => {
  try { const detail = await productToolDetail(req.params.toolCode, req.query.days || 30); res.setHeader("Cache-Control", "no-store"); return detail ? res.json(detail) : res.status(404).json({ error: "Tool not found" }); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/lifecycle", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await productLifecycle(req.query.days || 90)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/commercial", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await productCommercial(req.query.days || 30)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/feedback", async (req, res) => {
  try { res.setHeader("Cache-Control", "no-store"); return res.json(await productFeedback(req.query.days || 90)); }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/data-health", async (_req, res) => {
  try {
    const newsletter = newsletterConfig();
    const health = await productDataHealth(Boolean(newsletter.apiUrl && newsletter.token));
    const completedRequests = operationsApiMetrics.requests;
    health.api = {
      status: operationsApiMetrics.failures ? "degraded" : "healthy",
      requestsSinceStart: completedRequests,
      failedRequests: operationsApiMetrics.failures,
      averageResponseMs: completedRequests ? Math.round(operationsApiMetrics.totalDurationMs / completedRequests) : null,
      measuringSince: operationsApiMetrics.startedAt,
    };
    res.setHeader("Cache-Control", "no-store");
    return res.json(health);
  }
  catch (error) { return operationsFailure(res, error); }
});
app.get("/api/operations/health", async (_req, res) => {
  try {
    const newsletter = newsletterConfig();
    res.setHeader("Cache-Control", "no-store");
    return res.json(await operationsHealth(Boolean(newsletter.apiUrl && newsletter.token)));
  } catch (error) { return operationsFailure(res, error); }
});

app.get("/api/newsletter/customers/:subscriberId", newsletterCustomerProfile);
app.use("/api/newsletter", newsletterProxy);

app.get("/", (_req, res) => res.sendFile(path.join(publicDir, "summary.html")));
app.get("/newsletter.html", (_req, res) => res.sendFile(path.join(publicDir, "newsletter-v2.html")));
app.get("/newsletter-v2.html", (_req, res) => res.redirect(302, "/newsletter.html"));
app.get("/stats.html", (_req, res) => res.redirect(302, "/users.html"));
app.use(express.static(publicDir));
app.use((_req, res) => res.status(404).json({ error: "Not found" }));

function startServer() {
  app.listen(PORT, () => console.log(`waysorted-operations-dashboard listening on http://localhost:${PORT}`));
}

export default app;

const isDirectRun = Boolean(process.argv[1]) && path.resolve(process.argv[1]) === __filename;
if (isDirectRun) startServer();
