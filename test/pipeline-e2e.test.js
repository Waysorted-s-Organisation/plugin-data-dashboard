import assert from "node:assert/strict";
import test from "node:test";

import { MongoClient, ObjectId } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb, getEventsCollection } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");

/**
 * End-to-end pipeline test.
 *
 * Posts the exact envelope the built plugin sends to the real ingest route,
 * then reads the stored documents and the served dashboard aggregations. The
 * event types below are the ones the plugin used to discard before they could
 * ever be sent; they are included here so a regression that reintroduces the
 * drop is caught by a failing assertion rather than by silence.
 */
async function bootMongo(t) {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  process.env.ANALYTICS_INGEST_TOKEN_REQUIRED = "false";
  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());
  return client;
}

const pluginEnvelope = (events, over = {}) => ({
  source: "figma-plugin-main",
  schemaVersion: 2,
  sentAt: new Date().toISOString(),
  sessionId: "session_e2e_1",
  deviceId: "device_e2e_1",
  plugin: { name: "waysorted-your-all-in-one-tools-playground", version: "1.0.1" },
  runtime: { editorType: "figma", mode: "default", command: null, fileKey: "file-1" },
  events,
  ...over,
});

const evt = (eventId, eventType, over = {}) => ({
  eventId,
  schemaVersion: 2,
  eventType,
  isSemantic: over.isSemantic ?? true,
  eventAt: new Date().toISOString(),
  source: "main",
  tool: "comment-summarizer",
  payload: {},
  sessionId: "session_e2e_1",
  deviceId: "device_e2e_1",
  user: { isAuthenticated: false, userId: null, anonymousId: "anon_e2e", email: null },
  ...over,
});

test("previously discarded event types now survive ingest and are stored", async (t) => {
  await bootMongo(t);

  // Every one of these was dropped by the plugin's allowlist before reaching
  // the network. They arrive flagged non-semantic and must be persisted.
  const recovered = [
    evt("e-backend", "backend_operation", { isSemantic: false, payload: { operation: "pages-load", status: "failed" } }),
    evt("e-navigation", "tool_context_changed", { isSemantic: false, payload: { fromTool: "dashboard", toTool: "comment-summarizer" } }),
    evt("e-heartbeat", "session_heartbeat", { isSemantic: false, payload: { uptimeMs: 30000, activeTool: "comment-summarizer" } }),
    evt("e-usercontext", "user_context_changed", { isSemantic: false, payload: { source: "stored-user" } }),
    evt("e-uisession", "ui_session_started", { isSemantic: false, source: "ui", payload: { viewportWidth: 480 } }),
    evt("e-opened", "tool_opened", { payload: { tool: "comment-summarizer" } }),
  ];

  const response = await request(app)
    .post("/api/plugin-analytics/ingest")
    .send(pluginEnvelope(recovered))
    .expect(202);

  assert.equal(response.body.accepted, 6);
  assert.equal(response.body.inserted, 6);

  const stored = await (await getEventsCollection()).find({}).toArray();
  assert.equal(stored.length, 6, "all six events are persisted, none filtered server-side");

  const byId = new Map(stored.map((row) => [row.eventId, row]));
  assert.equal(byId.get("e-backend").isSemantic, false, "non-semantic events are stored and flagged");
  assert.equal(byId.get("e-backend").payload.status, "failed", "payload survives intact");
  assert.equal(byId.get("e-opened").isSemantic, true, "curated events keep their semantic flag");
  assert.equal(byId.get("e-navigation").payload.toTool, "comment-summarizer");
  assert.equal(byId.get("e-uisession").source, "ui", "UI-thread events keep their source");
});

test("an email is never stored as a user id", async (t) => {
  await bootMongo(t);

  await request(app)
    .post("/api/plugin-analytics/ingest")
    .send(pluginEnvelope([
      // A signed-in user whose account id has not resolved yet. Email must
      // populate the email field, never masquerade as the identifier.
      evt("e-email", "feature_used", {
        user: { isAuthenticated: true, userId: null, anonymousId: null, email: "alice@example.com", name: "Alice" },
      }),
    ]))
    .expect(202);

  const stored = await (await getEventsCollection()).findOne({ eventId: "e-email" });
  assert.equal(stored.user.userId, null, "email must not be promoted to userId");
  assert.equal(stored.user.email, "alice@example.com", "email is retained in its own field");
  assert.equal(stored.user.isAuthenticated, true, "a user known only by email is still authenticated");
});

test("a recovered session end is deduplicated by its deterministic id", async (t) => {
  await bootMongo(t);

  // The plugin cannot flush on close, so the next launch re-emits the previous
  // session's end. Both the measured close and the recovered one use an id
  // derived from the session, so a session can never be counted as ending twice
  // however many times either is delivered.
  const recoveredEnd = evt("session_end_session_prev", "plugin_session_ended", {
    sessionId: "session_prev",
    payload: { endReason: "recovered_after_close", observedDurationMs: 120000 },
  });

  const first = await request(app).post("/api/plugin-analytics/ingest").send(pluginEnvelope([recoveredEnd])).expect(202);
  const second = await request(app).post("/api/plugin-analytics/ingest").send(pluginEnvelope([recoveredEnd])).expect(202);

  assert.equal(first.body.inserted, 1);
  assert.equal(second.body.inserted, 0, "the repeat is recognised as a duplicate");
  assert.equal(second.body.duplicates, 1);

  // A close that did manage to flush carries the same id, so the two collapse
  // rather than producing a real end and a phantom recovered one.
  const measuredEnd = evt("session_end_session_prev", "plugin_session_ended", {
    sessionId: "session_prev",
    payload: { endReason: "observed_close", totalDurationMs: 125000 },
  });
  const third = await request(app).post("/api/plugin-analytics/ingest").send(pluginEnvelope([measuredEnd])).expect(202);
  assert.equal(third.body.inserted, 0, "a measured close for the same session is not a second end");

  const ends = await (await getEventsCollection()).find({ eventType: "plugin_session_ended" }).toArray();
  assert.equal(ends.length, 1, "one session end, not two");
  assert.equal(ends[0].payload.endReason, "recovered_after_close", "first write wins");
});

test("a non-credit tool becomes visible on the dashboard after ingest", async (t) => {
  const client = await bootMongo(t);
  const backend = client.db("waysorted");
  await backend.collection("users").insertOne({
    _id: new ObjectId(), email: "carol@example.com", createdAt: new Date(),
  });

  // comment-summarizer charges no credits, so it produces zero reservations.
  // Its entire presence on the dashboard depends on this telemetry.
  await request(app)
    .post("/api/plugin-analytics/ingest")
    .send(pluginEnvelope([
      evt("t1", "tool_opened"),
      evt("t2", "tool_action_started"),
      evt("t3", "tool_action_completed"),
      evt("t4", "tool_action_failed"),
      evt("t5", "active_tool_time", { payload: { durationMs: 9000 } }),
      evt("t6", "feature_used", { isSemantic: false, sessionId: "session_e2e_2" }),
    ]))
    .expect(202);

  const tools = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const row = tools.body.items.find((item) => item.key === "comment-summarizer");
  assert.ok(row, "a tool with no credit system is now measurable");
  assert.equal(row.coverage, "telemetry");
  assert.equal(row.telemetry.opens, 1);
  assert.equal(row.telemetry.actionsCompleted, 1);
  assert.equal(row.telemetry.actionsFailed, 1);
  assert.equal(row.telemetry.activeMs, 9000);
  assert.equal(row.telemetry.sessions, 2, "both sessions counted");
  assert.equal(row.creditsConsumed, 0);
  assert.equal(tools.body.summary.telemetryOnlyTools, 1);
  assert.equal(tools.body.summary.observedToolEvents, 6);
});
