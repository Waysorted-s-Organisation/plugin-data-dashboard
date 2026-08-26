import assert from "node:assert/strict";
import test from "node:test";

import { MongoClient, ObjectId } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");
const DAY = 24 * 60 * 60 * 1000;

/**
 * The users table's job column used to count committed usage reservations and
 * nothing else, so anyone whose work never produced one — the run was not
 * billed, the hold never committed, the reservation lost its user link, the
 * person was signed out and could not hold a reservation at all — read as "0
 * jobs" beside a tool they demonstrably used.
 *
 * These tests pin the replacement: reservations stay authoritative when they
 * exist, telemetry answers when they do not, the two are never added together,
 * and the lifetime columns stop disagreeing with each other about the period.
 */
async function harness(t) {
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
  return { client, analytics: client.db("analytics"), backend: client.db("waysorted") };
}

const telemetry = (over) => ({
  schemaVersion: 2,
  isSemantic: true,
  sessionId: "s1",
  deviceId: "device-1",
  source: "main",
  tool: "comment-summarizer",
  receivedAt: new Date(),
  payload: {},
  user: { isAuthenticated: false, userId: null, anonymousId: null, email: null },
  ...over,
});

test("tool jobs are counted from telemetry when no reservation records them", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const observed = new ObjectId();
  const credited = new ObjectId();

  await backend.collection("users").insertMany([
    { _id: observed, email: "observed@example.com", createdAt: new Date(now.getTime() - 5 * DAY) },
    { _id: credited, email: "credited@example.com", createdAt: new Date(now.getTime() - 5 * DAY) },
  ]);

  // The credited user's four runs each produce BOTH a committed reservation and
  // a completed tool action, which is what the plugin emits once a tool charges
  // credits. The two records describe the same four jobs.
  await backend.collection("usagereservations").insertMany(
    [1, 2, 3, 4].map(() => ({
      _id: new ObjectId(), user: credited, toolCode: "palettable", status: "committed",
      creditsReserved: 2, createdAt: now, committedAt: now, updatedAt: now,
    }))
  );

  await analytics.collection("plugin_analytics_events").insertMany([
    // The user opens the tool, then finishes three runs whose reservations
    // never reached "committed".
    telemetry({
      eventId: "obs-open", eventType: "tool_opened", eventAt: new Date(now.getTime() - 60000),
      user: { isAuthenticated: true, userId: String(observed), anonymousId: null, email: null },
    }),
    ...[1, 2, 3].map((n) => telemetry({
      eventId: `obs${n}`, eventType: "tool_action_completed",
      eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: true, userId: String(observed), anonymousId: null, email: null },
    })),
    // A start is not finished work and must not inflate the count.
    telemetry({
      eventId: "obs-start", eventType: "tool_action_started", eventAt: now,
      user: { isAuthenticated: true, userId: String(observed), anonymousId: null, email: null },
    }),
    ...[1, 2, 3, 4].map((n) => telemetry({
      eventId: `cred${n}`, eventType: "tool_action_completed", tool: "palettable",
      eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: true, userId: String(credited), anonymousId: null, email: null },
    })),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const observedRow = users.body.items.find((row) => row.email === "observed@example.com");
  assert.equal(observedRow.creditedJobs, 0, "nothing billed");
  assert.equal(observedRow.observedJobs, 3, "three finished runs are visible in telemetry");
  assert.equal(observedRow.completedJobs, 3, "so the column reports three, not zero");
  assert.equal(observedRow.completedJobsSource, "observed", "and says where the number came from");

  const creditedRow = users.body.items.find((row) => row.email === "credited@example.com");
  assert.equal(creditedRow.creditedJobs, 4);
  assert.equal(creditedRow.observedJobs, 4, "the same four jobs are also in telemetry");
  assert.equal(creditedRow.completedJobs, 4, "reported once, never summed to eight");
  assert.equal(creditedRow.completedJobsSource, "credited", "the billing record stays authoritative");
});

test("only finished runs count as jobs; invocations do not stand in for them", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "feature@example.com", createdAt: now });

  await analytics.collection("plugin_analytics_events").insertMany([
    // Invocations and starts. Neither says the work finished, and a single run
    // can emit several, so counting them would be a different number wearing
    // the word "jobs".
    ...[1, 2].map((n) => telemetry({
      eventId: `f${n}`, eventType: "feature_used", tool: "icon-library",
      eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: true, userId: String(user), anonymousId: null, email: null },
    })),
    telemetry({
      eventId: "st1", eventType: "tool_action_started", tool: "icon-library", eventAt: now,
      user: { isAuthenticated: true, userId: String(user), anonymousId: null, email: null },
    }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "feature@example.com");
  assert.equal(row.observedJobs, 0, "nothing here is evidence that a run finished");
  assert.ok(row.topTool, "but the tool they used is still known");

  // The tool is named as under-instrumented rather than reported as idle.
  const tools = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.deepEqual(tools.body.summary.toolsWithoutCompletions, ["Icon Library"]);
});

test("an account id that does not join is rescued by the email on the same event", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({
    _id: user, email: "real.person@example.com", createdAt: new Date(now.getTime() - 10 * DAY),
  });

  // The plugin sends its own notion of the account — an auth subject, not the
  // backend users._id — but does send the email alongside it.
  await analytics.collection("plugin_analytics_events").insertMany(
    [1, 2, 3].map((n) => telemetry({
      eventId: `x${n}`, eventType: "tool_action_completed", tool: "palettable",
      eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: true, userId: "auth0|9f3c7b21", anonymousId: null, email: "real.person@example.com" },
    }))
  );

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "real.person@example.com");
  assert.equal(row.observedJobs, 3, "keyed on the email, which resolves, not the id, which does not");
  assert.equal(row.topTool.label, "Palettable");
  assert.equal(users.body.items.filter((item) => item.anonymous).length, 0);
});

test("a lifetime job count is not paired with a window-scoped latest tool", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({
    _id: user, email: "lapsed@example.com", createdAt: new Date(now.getTime() - 90 * DAY),
  });

  // Their only tool use was twenty days ago — outside a seven-day window, but
  // squarely inside the lifetime lookback the job column is measured over.
  await analytics.collection("plugin_analytics_events").insertMany(
    [1, 2].map((n) => telemetry({
      eventId: `old${n}`, eventType: "tool_action_completed", tool: "palettable",
      eventAt: new Date(now.getTime() - 20 * DAY - n * 1000),
      user: { isAuthenticated: true, userId: String(user), anonymousId: null, email: null },
    }))
  );

  const users = await request(app)
    .get("/api/operations/users?days=7")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "lapsed@example.com");
  assert.equal(row.observedJobs, 2, "the lifetime count sees the work");
  assert.equal(row.observedJobsInRange, 0, "the in-range count correctly does not");
  assert.ok(row.topTool, "and the latest-tool column is answered from the same lifetime span");
  assert.equal(row.topTool.label, "Palettable");
});

test("the ingest's unknown-device placeholder is never treated as an identity", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const account = new ObjectId();
  await backend.collection("users").insertOne({ _id: account, email: "real@example.com", createdAt: now });

  await analytics.collection("plugin_analytics_events").insertMany([
    // Two different people whose builds sent no device id. The ingest writes
    // the same literal placeholder for both.
    telemetry({ eventId: "u1", eventType: "tool_action_completed", deviceId: "unknown-device", sessionId: "sa", eventAt: now }),
    telemetry({ eventId: "u2", eventType: "tool_action_completed", deviceId: "unknown-device", sessionId: "sb", eventAt: now }),
    // A sign-in whose link event also carries the placeholder. Honouring it
    // would hand every unattributed device's activity to this one account.
    telemetry({
      eventId: "link1", eventType: "identity_linked", deviceId: "unknown-device", eventAt: now,
      payload: { anonymousId: "unknown-device", userId: String(account) },
      user: { isAuthenticated: true, userId: String(account), anonymousId: null, email: null },
    }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(
    users.body.items.filter((row) => row.anonymous).length,
    0,
    "the placeholder does not become a visitor row"
  );
  const row = users.body.items.find((item) => item.email === "real@example.com");
  assert.equal(row.observedJobs, 0, "and its activity is not dumped onto a real account");
});

test("signed-out visitors report the jobs they completed", async (t) => {
  const { analytics } = await harness(t);
  const now = new Date();

  await analytics.collection("plugin_analytics_events").insertMany(
    [1, 2, 3].map((n) => telemetry({
      eventId: `a${n}`, eventType: "tool_action_completed", tool: "unit-converter",
      deviceId: "device-visitor", eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: false, userId: null, anonymousId: "anon-1", email: null, name: "Visitor" },
    }))
  );

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const visitor = users.body.items.find((row) => row.anonymous);
  assert.ok(visitor, "the visitor is listed");
  assert.equal(visitor.creditedJobs, 0, "they hold no wallet and so no reservation");
  assert.equal(visitor.completedJobs, 3, "but they finished three jobs");
  assert.equal(visitor.completedJobsSource, "observed");
});

test("the summary counts jobs the same way the users table does", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const credited = new ObjectId();
  const observed = new ObjectId();

  await backend.collection("users").insertMany([
    { _id: credited, email: "billed@example.com", createdAt: new Date(now.getTime() - 40 * DAY) },
    { _id: observed, email: "unbilled@example.com", createdAt: new Date(now.getTime() - 40 * DAY) },
  ]);
  // Two credited runs, each with its telemetry twin.
  await backend.collection("usagereservations").insertMany(
    [1, 2].map(() => ({
      _id: new ObjectId(), user: credited, toolCode: "palettable", status: "committed",
      creditsReserved: 3, createdAt: now, committedAt: now, updatedAt: now,
    }))
  );
  await analytics.collection("plugin_analytics_events").insertMany([
    ...[1, 2].map((n) => telemetry({
      eventId: `sc${n}`, eventType: "tool_action_completed", tool: "palettable",
      eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: true, userId: String(credited), anonymousId: null, email: null },
    })),
    // Five runs with no reservation behind them at all.
    ...[1, 2, 3, 4, 5].map((n) => telemetry({
      eventId: `so${n}`, eventType: "tool_action_completed",
      eventAt: new Date(now.getTime() - n * 1000),
      user: { isAuthenticated: true, userId: String(observed), anonymousId: null, email: null },
    })),
  ]);

  const summary = await request(app)
    .get("/api/operations/summary?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  // 2 credited (not 4 — the telemetry twins are the same two runs) plus 5
  // observed-only. Committed-reservations-only would have reported 2.
  assert.equal(summary.body.metrics.completedJobs.value, 7);
  assert.equal(
    summary.body.metrics.activatedUsers.value,
    2,
    "activation counts any tool use, matching the users table"
  );
});
