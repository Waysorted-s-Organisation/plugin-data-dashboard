import assert from "node:assert/strict";
import test from "node:test";

import { MongoClient, ObjectId } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");

/**
 * Tools without a credit system emit no usage reservations, so before plugin
 * telemetry was read they could only ever be reported as "unavailable". These
 * tests seed raw plugin events and assert the tool becomes measurable, that its
 * counts are derived from the events rather than assumed, and that credit-backed
 * tools keep reporting their reservation-derived numbers unchanged.
 */
test("tool activity is measured from plugin telemetry", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";

  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());

  const analytics = client.db("analytics");
  const backend = client.db("waysorted");
  const now = new Date();
  const minutesAgo = (n) => new Date(now.getTime() - n * 60 * 1000);

  const alice = new ObjectId();
  await backend.collection("users").insertOne({
    _id: alice, email: "alice@example.com", name: "Alice", createdAt: minutesAgo(60),
  });

  // comment-summarizer charges no credits, so it has no reservations at all.
  // Three distinct identities: one signed-in account, one anonymous visitor,
  // and one device-only visitor with no identity fields whatsoever.
  const event = (over) => ({
    schemaVersion: 2,
    isSemantic: true,
    sessionId: "s1",
    deviceId: "d1",
    source: "main",
    tool: "comment-summarizer",
    eventAt: minutesAgo(30),
    receivedAt: now,
    payload: {},
    user: { isAuthenticated: false, userId: null, anonymousId: "anon-1", email: null },
    ...over,
  });

  await analytics.collection("plugin_analytics_events").insertMany([
    event({ eventId: "e1", eventType: "tool_opened" }),
    event({ eventId: "e2", eventType: "tool_action_started" }),
    event({ eventId: "e3", eventType: "tool_action_completed" }),
    event({ eventId: "e4", eventType: "tool_action_failed" }),
    event({ eventId: "e5", eventType: "user_facing_error_displayed" }),
    event({ eventId: "e6", eventType: "active_tool_time", payload: { durationMs: 4000 } }),
    event({
      eventId: "e7", eventType: "feature_used", sessionId: "s2",
      user: { isAuthenticated: true, userId: String(alice), anonymousId: null, email: "alice@example.com" },
    }),
    event({
      eventId: "e8", eventType: "feature_used", sessionId: "s3", deviceId: "d3",
      user: { isAuthenticated: false, userId: null, anonymousId: null, email: null },
    }),
    // Outside the requested window; must not be counted.
    event({ eventId: "e9", eventType: "tool_opened", eventAt: new Date(now.getTime() - 40 * 24 * 60 * 60 * 1000) }),
    // Unattributed tool; must not create a phantom tool row.
    event({ eventId: "e10", eventType: "feature_used", tool: "unknown" }),
  ]);

  const response = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const summarizer = response.body.items.find((row) => row.key === "comment-summarizer");
  assert.ok(summarizer, "comment-summarizer should be measurable from telemetry alone");
  assert.equal(summarizer.coverage, "telemetry");

  const activity = summarizer.telemetry;
  assert.equal(activity.events, 8, "only in-window events for this tool are counted");
  assert.equal(activity.opens, 1);
  assert.equal(activity.actionsStarted, 1);
  assert.equal(activity.actionsCompleted, 1);
  assert.equal(activity.actionsFailed, 1);
  assert.equal(activity.errors, 1);
  assert.equal(activity.featureUses, 2);
  assert.equal(activity.activeMs, 4000, "duration is summed from the event payload");
  assert.equal(activity.sessions, 3, "s1, s2 and s3");
  assert.equal(activity.uniqueUsers, 3, "account, anonymous id and device fallback each count once");
  assert.equal(activity.knownAccounts, 1, "only the signed-in identity is a known account");

  assert.equal(summarizer.completedJobs, null, "billable job counts do not apply to a credit-free tool");
  assert.equal(summarizer.telemetry.actionsCompleted, 1, "observed completions live under telemetry");
  assert.equal(summarizer.creditsConsumed, 0, "a tool with no credit system consumes no credits");

  assert.ok(
    !response.body.items.some((row) => row.key === "unknown"),
    "events with no tool attribution must not become a tool"
  );
  assert.equal(response.body.summary.telemetryOnlyTools, 1);
});

test("plugin UI surfaces do not become tools", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";

  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());
  const now = new Date();

  // The plugin stamps a tool on every event, defaulting to the current surface.
  // Session, heartbeat and analytics-plumbing traffic all carry "dashboard",
  // which would otherwise outrank every real tool on the page.
  const surfaceEvent = (eventId, tool) => ({
    eventId, schemaVersion: 2, isSemantic: true, eventType: "plugin_session_started",
    sessionId: "s1", deviceId: "d1", source: "main", tool,
    eventAt: new Date(now.getTime() - 60 * 1000), receivedAt: now, payload: {},
    user: { isAuthenticated: false, userId: null, anonymousId: "a1", email: null },
  });

  await client.db("analytics").collection("plugin_analytics_events").insertMany([
    surfaceEvent("s-dash1", "dashboard"),
    surfaceEvent("s-dash2", "dashboard"),
    surfaceEvent("s-collapsed", "collapsed-dashboard"),
    surfaceEvent("s-profile", "profile"),
    surfaceEvent("s-game", "wayfall-game"),
    surfaceEvent("s-glass", "liquid-glass"),
    surfaceEvent("s-real", "comment-summarizer"),
  ]);

  const tools = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const keys = tools.body.items.map((row) => row.key);
  for (const surface of ["dashboard", "collapsed-dashboard", "profile", "wayfall-game", "liquid-glass"]) {
    assert.ok(!keys.includes(surface), `${surface} is a UI surface, not a tool`);
  }
  assert.ok(keys.includes("comment-summarizer"), "a real tool still appears");
  assert.equal(tools.body.summary.telemetryOnlyTools, 1);
  assert.equal(tools.body.summary.observedToolEvents, 1, "surface noise is excluded from tool totals");
});

test("one product reported under two codes is merged, not double counted", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";

  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());
  const now = new Date();
  const user = new ObjectId();

  await client.db("waysorted").collection("users").insertOne({ _id: user, email: "e@example.com", createdAt: now });
  // Reservations record "frame_gallery"; the plugin emits "frame-gallery".
  await client.db("waysorted").collection("usagereservations").insertOne({
    _id: new ObjectId(), user, toolCode: "frame_gallery", featureCode: "export",
    status: "committed", creditsReserved: 3, createdAt: now, committedAt: now, updatedAt: now,
  });
  await client.db("analytics").collection("plugin_analytics_events").insertOne({
    eventId: "fg1", schemaVersion: 2, isSemantic: true, eventType: "tool_opened",
    sessionId: "s1", deviceId: "d1", source: "main", tool: "frame-gallery",
    eventAt: now, receivedAt: now, payload: {},
    user: { isAuthenticated: true, userId: String(user), anonymousId: null, email: null },
  });

  const tools = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const frames = tools.body.items.filter((row) => row.key === "frames-to-pdf" || row.key === "frame-gallery");
  assert.equal(frames.length, 1, "one product, one row");
  assert.equal(frames[0].key, "frames-to-pdf");
  assert.equal(frames[0].coverage, "measured", "the credit-backed row absorbs the telemetry");
  assert.equal(frames[0].uniqueUsers, 1, "the same user is not counted twice");
  assert.ok(frames[0].telemetry, "telemetry is attached to the merged row");
  assert.equal(frames[0].telemetry.opens, 1);
});

test("telemetry does not displace credit-derived tool metrics", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";

  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());

  const backend = client.db("waysorted");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({ _id: user, email: "bob@example.com", createdAt: now });
  await backend.collection("usagereservations").insertOne({
    _id: new ObjectId(), user, toolCode: "palettable", featureCode: "export_palette",
    status: "committed", creditsReserved: 5, createdAt: now, committedAt: now, updatedAt: now,
  });

  const response = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const palettable = response.body.items.find((row) => row.key === "palettable");
  assert.ok(palettable, "credit-backed tools remain measured with no telemetry present");
  assert.equal(palettable.coverage, "measured");
  assert.equal(palettable.creditsConsumed, 5, "reservation-derived credits are unchanged");
  assert.equal(palettable.telemetry, null, "absent telemetry is reported as absent, not as zero activity");
});

test("one person spanning the legacy and modern identity formats counts once", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";

  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());
  const now = new Date();
  const account = new ObjectId();

  await client.db("waysorted").collection("users").insertOne({
    _id: account, email: "same@example.com", createdAt: now,
  });

  const base = {
    schemaVersion: 2, isSemantic: true, eventType: "feature_used",
    sessionId: "s1", deviceId: "d1", source: "main", tool: "comment-summarizer",
    eventAt: new Date(now.getTime() - 60 * 1000), receivedAt: now, payload: {},
  };

  await client.db("analytics").collection("plugin_analytics_events").insertMany([
    // Legacy: an email sat in user.userId, with no email field.
    { ...base, eventId: "old", user: { isAuthenticated: true, userId: "Same@Example.com", email: null, anonymousId: null } },
    // Modern: a real account id, with the email carried alongside.
    { ...base, eventId: "new", sessionId: "s2", user: { isAuthenticated: true, userId: String(account), email: "same@example.com", anonymousId: null } },
  ]);

  const tools = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const row = tools.body.items.find((item) => item.key === "comment-summarizer");
  assert.equal(row.telemetry.uniqueUsers, 1, "one human, not one per identity format");
  assert.equal(row.telemetry.sessions, 2, "both sessions still counted");
  assert.equal(row.telemetry.events, 2);
});
