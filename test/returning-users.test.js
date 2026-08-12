import assert from "node:assert/strict";
import test from "node:test";

import { MongoClient, ObjectId } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");
const DAY_MS = 24 * 60 * 60 * 1000;

async function boot(t, timezone) {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  if (timezone) process.env.REPORTING_TIMEZONE = timezone;
  else delete process.env.REPORTING_TIMEZONE;
  const client = new MongoClient(mongod.getUri());
  await client.connect();
  t.after(() => client.close());
  return client;
}

const pluginEvent = (over) => ({
  eventId: over.eventId,
  schemaVersion: 2,
  isSemantic: true,
  eventType: "plugin_session_started",
  eventAt: over.eventAt,
  source: "main",
  tool: "dashboard",
  sessionId: over.sessionId || "s",
  deviceId: "d1",
  payload: {},
  user: over.user,
  ...over,
});

test("plugin activity makes a user count as returning without extra logins", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "dana@example.com", name: "Dana", createdAt: new Date(now.getTime() - 20 * DAY_MS),
  });
  // Exactly one backend login. The plugin authenticates from a stored token on
  // later launches, so no further session documents are ever created.
  await backend.collection("sessions").insertOne({
    user, source: "google", completed: true,
    completedAt: new Date(now.getTime() - 10 * DAY_MS),
    createdAt: new Date(now.getTime() - 10 * DAY_MS),
  });

  await analytics.collection("plugin_analytics_events").insertMany([
    pluginEvent({ eventId: "p1", eventAt: new Date(now.getTime() - 5 * DAY_MS), user: { isAuthenticated: true, userId: String(user), email: "dana@example.com", anonymousId: null } }),
    pluginEvent({ eventId: "p2", eventAt: new Date(now.getTime() - 3 * DAY_MS), user: { isAuthenticated: true, userId: String(user), email: "dana@example.com", anonymousId: null } }),
    pluginEvent({ eventId: "p3", eventAt: new Date(now.getTime() - 1 * DAY_MS), user: { isAuthenticated: true, userId: String(user), email: "dana@example.com", anonymousId: null } }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const dana = users.body.items.find((row) => row.email === "dana@example.com");
  assert.ok(dana, "user is listed");
  assert.equal(dana.successfulLogins, 1, "still only one backend login");
  assert.equal(dana.pluginActiveDays, 3, "three distinct plugin days are counted");
  assert.ok(dana.activeDaysInRange >= 4, "login day plus three plugin days");
  assert.ok(dana.segments.includes("returning"), "a user who came back via the plugin is returning");
  assert.equal(dana.lifecycleStage, "returning");

  const summary = await request(app)
    .get("/api/operations/summary?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.equal(summary.body.metrics.returningUsers.value, 1, "the summary counts the return too");
});

test("legacy events that stored an email in userId still resolve to the account", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "Legacy@Example.com", name: "Legacy", createdAt: new Date(now.getTime() - 30 * DAY_MS),
  });

  // Written before the identity fix: userId holds an email address. These rows
  // must resolve on read rather than being rewritten in place.
  await analytics.collection("plugin_analytics_events").insertMany([
    pluginEvent({ eventId: "l1", eventAt: new Date(now.getTime() - 6 * DAY_MS), user: { isAuthenticated: true, userId: "legacy@example.com", email: null, anonymousId: null } }),
    pluginEvent({ eventId: "l2", eventAt: new Date(now.getTime() - 2 * DAY_MS), user: { isAuthenticated: true, userId: "legacy@example.com", email: null, anonymousId: null } }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const legacy = users.body.items.find((row) => row.email === "Legacy@Example.com");
  assert.equal(legacy.pluginActiveDays, 2, "email-keyed history is matched case-insensitively");
  assert.ok(legacy.segments.includes("returning"), "historical activity restores the return");
});

test("day bucketing follows the configured reporting timezone", async (t) => {
  const client = await boot(t, "Asia/Kolkata");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "tz@example.com", createdAt: new Date("2026-08-01T00:00:00Z"),
  });

  // 17:30Z is 23:00 IST on the 5th; 20:30Z is 02:00 IST on the 6th. In UTC
  // these are one day; in IST they are two, and the user did return.
  await analytics.collection("plugin_analytics_events").insertMany([
    pluginEvent({ eventId: "tz1", eventAt: new Date("2026-08-05T17:30:00Z"), user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null } }),
    pluginEvent({ eventId: "tz2", eventAt: new Date("2026-08-05T20:30:00Z"), user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null } }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=3650")
    .set("Authorization", basicAuth)
    .expect(200);

  const row = users.body.items.find((item) => item.email === "tz@example.com");
  assert.equal(row.pluginActiveDays, 2, "two local days, not one UTC day");
  assert.ok(row.segments.includes("returning"));
});

test("a user with no plugin activity is unchanged and reported as such", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "quiet@example.com", createdAt: new Date(now.getTime() - 5 * DAY_MS),
  });
  await backend.collection("sessions").insertOne({
    user, source: "google", completed: true, completedAt: now, createdAt: now,
  });

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const row = users.body.items.find((item) => item.email === "quiet@example.com");
  assert.equal(row.pluginActiveDays, 0, "absence of activity is reported as zero, not inferred");
  assert.equal(row.activeDaysInRange, 1);
  assert.ok(!row.segments.includes("returning"), "one day is not a return");
});

test("the boundary day belongs to exactly one period", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "edge@example.com", createdAt: new Date(now.getTime() - 90 * DAY_MS),
  });

  // The previous window ends exactly where the current one begins. One event
  // sits genuinely in the previous window; the other is an hour INTO the
  // current window but shares a calendar day with the boundary. Counting the
  // second in both periods would manufacture a second previous-period day and
  // flip the user to "returning" in a window where they were active once.
  const currentStart = new Date(now.getTime() - 30 * DAY_MS);
  await analytics.collection("plugin_analytics_events").insertMany([
    pluginEvent({ eventId: "edge-prev", eventAt: new Date(now.getTime() - 31 * DAY_MS), user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null } }),
    pluginEvent({ eventId: "edge-cur", eventAt: new Date(currentStart.getTime() + 60 * 60 * 1000), user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null } }),
  ]);

  const summary = await request(app)
    .get("/api/operations/summary?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(
    summary.body.metrics.returningUsers.previous, 0,
    "one day in the previous window is not a return"
  );
  assert.equal(summary.body.metrics.returningUsers.value, 0, "one day in the current window is not a return either");
});

test("activity just before the window boundary stays in the previous period only", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();
  const currentStart = new Date(now.getTime() - 30 * DAY_MS);

  await backend.collection("users").insertOne({
    _id: user, email: "cusp@example.com", createdAt: new Date(now.getTime() - 90 * DAY_MS),
  });

  // Two consecutive plugin days, BOTH entirely inside the previous window, the
  // later one a second before the boundary instant. Windowing by calendar-day
  // string used to hand the later day to the current period, leaving each
  // period with one day so the user was "returning" in neither — while also
  // inventing a current-period active user who did nothing in it.
  await analytics.collection("plugin_analytics_events").insertMany([
    pluginEvent({ eventId: "cusp1", eventAt: new Date(currentStart.getTime() - DAY_MS - 1000), user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null } }),
    pluginEvent({ eventId: "cusp2", eventAt: new Date(currentStart.getTime() - 1000), user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null } }),
  ]);

  const summary = await request(app)
    .get("/api/operations/summary?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(summary.body.metrics.activeUsers.value, 0, "no activity in the current window");
  assert.equal(summary.body.metrics.activeUsers.previous, 1, "the user was active in the previous window");
  assert.equal(summary.body.metrics.returningUsers.previous, 1, "two days in the previous window is a return");
});

test("cohort retention and the funnel agree about plugin returns", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();
  const created = new Date(now.getTime() - 40 * DAY_MS);

  await backend.collection("users").insertOne({
    _id: user, email: "cohort@example.com", createdAt: created,
  });
  // One login on signup day, then plugin activity on later days. The funnel
  // counted this as a return; the cohort table, reading logins only, did not.
  await backend.collection("sessions").insertOne({
    user, source: "google", completed: true, completedAt: created, createdAt: created,
  });
  await analytics.collection("plugin_analytics_events").insertMany(
    [3, 5, 20].map((offset) => pluginEvent({
      eventId: `co${offset}`,
      eventAt: new Date(created.getTime() + offset * DAY_MS),
      user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null },
    }))
  );

  const lifecycle = await request(app)
    .get("/api/operations/lifecycle?days=90")
    .set("Authorization", basicAuth)
    .expect(200);

  const returnedStage = lifecycle.body.stages.find((s) => s.key === "returned");
  assert.equal(returnedStage.users, 1, "the funnel counts the plugin return");

  const cohort = lifecycle.body.cohorts.find((row) => row.signedUp > 0);
  assert.ok(cohort, "a cohort row exists");
  assert.equal(cohort.returned7d, 1, "returned within 7 days via the plugin");
  assert.equal(cohort.returned30d, 1, "and within 30 days");
});

test("an idle plugin left open does not count as activity", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const idle = new ObjectId();
  const busy = new ObjectId();

  await backend.collection("users").insertMany([
    { _id: idle, email: "idle@example.com", createdAt: new Date(now.getTime() - 20 * DAY_MS) },
    { _id: busy, email: "busy@example.com", createdAt: new Date(now.getTime() - 20 * DAY_MS) },
  ]);

  // The heartbeat fires every 30s for as long as the plugin is open, so a
  // plugin parked in a background tab would otherwise look like a daily
  // returning user without the person ever touching it.
  await analytics.collection("plugin_analytics_events").insertMany([
    ...[2, 4, 6].map((d) => pluginEvent({
      eventId: `hb${d}`, eventType: "session_heartbeat",
      eventAt: new Date(now.getTime() - d * DAY_MS),
      user: { isAuthenticated: true, userId: String(idle), email: null, anonymousId: null },
    })),
    ...[2, 4].map((d) => pluginEvent({
      eventId: `use${d}`, eventType: "tool_opened",
      eventAt: new Date(now.getTime() - d * DAY_MS),
      user: { isAuthenticated: true, userId: String(busy), email: null, anonymousId: null },
    })),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const idleRow = users.body.items.find((r) => r.email === "idle@example.com");
  const busyRow = users.body.items.find((r) => r.email === "busy@example.com");

  assert.equal(idleRow.pluginActiveDays, 0, "heartbeats are not activity");
  assert.ok(!idleRow.segments.includes("returning"), "an idle plugin is not a returning user");
  assert.equal(busyRow.pluginActiveDays, 2, "real tool use is activity");
  assert.ok(busyRow.segments.includes("returning"));
});

test("a daily plugin user is not labelled dormant", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "daily@example.com", createdAt: new Date(now.getTime() - 60 * DAY_MS),
  });
  // Last login 45 days ago; dormancy measured from logins alone would call this
  // user dormant even though they used the plugin yesterday.
  await backend.collection("sessions").insertOne({
    user, source: "google", completed: true,
    completedAt: new Date(now.getTime() - 45 * DAY_MS), createdAt: new Date(now.getTime() - 45 * DAY_MS),
  });
  await analytics.collection("plugin_analytics_events").insertMany(
    [1, 2, 3].map((d) => pluginEvent({
      eventId: `d${d}`, eventType: "feature_used",
      eventAt: new Date(now.getTime() - d * DAY_MS),
      user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null },
    }))
  );

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const row = users.body.items.find((r) => r.email === "daily@example.com");
  assert.ok(!row.segments.includes("dormant"), "recent plugin use means not dormant");
  assert.ok(!row.segments.includes("at_risk"), "nor at risk");
});

test("a D+1 plugin return counts the same as a D+1 login return", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();

  // Signup at 09:00, return the next calendar day at 15:00 — 30 hours later.
  const created = new Date(now.getTime() - 40 * DAY_MS);
  created.setUTCHours(9, 0, 0, 0);
  const nextDayAfternoon = new Date(created.getTime() + DAY_MS + 6 * 60 * 60 * 1000);

  const viaLogin = new ObjectId();
  const viaPlugin = new ObjectId();
  await backend.collection("users").insertMany([
    { _id: viaLogin, email: "login@example.com", createdAt: created },
    { _id: viaPlugin, email: "plugin@example.com", createdAt: created },
  ]);
  await backend.collection("sessions").insertMany([
    { user: viaLogin, source: "google", completed: true, completedAt: created, createdAt: created },
    { user: viaLogin, source: "google", completed: true, completedAt: nextDayAfternoon, createdAt: nextDayAfternoon },
    { user: viaPlugin, source: "google", completed: true, completedAt: created, createdAt: created },
  ]);
  // The plugin user's only return is telemetry — no second session document,
  // which is the normal case since the plugin reuses a stored token.
  await analytics.collection("plugin_analytics_events").insertOne(
    pluginEvent({
      eventId: "d1", eventType: "feature_used", eventAt: nextDayAfternoon,
      user: { isAuthenticated: true, userId: String(viaPlugin), email: null, anonymousId: null },
    })
  );

  const lifecycle = await request(app)
    .get("/api/operations/lifecycle?days=90")
    .set("Authorization", basicAuth)
    .expect(200);

  const returnedStage = lifecycle.body.stages.find((s) => s.key === "returned");
  assert.equal(returnedStage.users, 2, "the funnel counts both returns");

  const cohort = lifecycle.body.cohorts.find((row) => row.signedUp === 2);
  assert.ok(cohort, "both users share a signup week");
  assert.equal(cohort.returned7d, 2, "the plugin-evidenced return is not dropped");
  assert.equal(cohort.returned30d, 2);
  assert.equal(cohort.return7dRate, 100, "cohort table agrees with the funnel");
});

test("narrowing the range does not relabel a plugin-active user as dormant", async (t) => {
  const client = await boot(t, "UTC");
  const backend = client.db("waysorted");
  const analytics = client.db("analytics");
  const now = new Date();
  const user = new ObjectId();

  await backend.collection("users").insertOne({
    _id: user, email: "ranged@example.com", createdAt: new Date(now.getTime() - 300 * DAY_MS),
  });
  await backend.collection("sessions").insertOne({
    user, source: "google", completed: true,
    completedAt: new Date(now.getTime() - 200 * DAY_MS), createdAt: new Date(now.getTime() - 200 * DAY_MS),
  });
  // Plugin activity 8-10 days ago: inside a 30-day window, outside a 7-day one.
  await analytics.collection("plugin_analytics_events").insertMany(
    [8, 9, 10].map((d) => pluginEvent({
      eventId: `r${d}`, eventType: "feature_used",
      eventAt: new Date(now.getTime() - d * DAY_MS),
      user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null },
    }))
  );

  for (const days of [7, 30, 90]) {
    const users = await request(app)
      .get(`/api/operations/users?days=${days}`)
      .set("Authorization", basicAuth)
      .expect(200);
    const row = users.body.items.find((r) => r.email === "ranged@example.com");
    assert.ok(!row.segments.includes("dormant"), `not dormant at days=${days}`);
    assert.ok(!row.segments.includes("at_risk"), `not at risk at days=${days}`);
  }
});

test("signed-out visitors are measured by their pseudonymous id", async (t) => {
  const client = await boot(t, "UTC");
  const analytics = client.db("analytics");
  const now = new Date();

  // No user records at all — nobody here has an account. The plugin still
  // gives each visitor a stable pseudonymous id, so their behaviour is
  // measurable even though who they are is not.
  const anon = (eventId, anonymousId, deviceId, dayOffset) => pluginEvent({
    eventId, eventType: "tool_opened", deviceId,
    eventAt: new Date(now.getTime() - dayOffset * DAY_MS),
    user: { isAuthenticated: false, userId: null, email: null, anonymousId },
  });

  await analytics.collection("plugin_analytics_events").insertMany([
    // A returning visitor: two distinct days under one pseudonymous id.
    anon("a1", "figma_abc", "d1", 3),
    anon("a2", "figma_abc", "d1", 1),
    // A one-visit visitor.
    anon("b1", "figma_xyz", "d2", 2),
    // No anonymousId at all: falls back to the device so they still count once.
    pluginEvent({
      eventId: "c1", eventType: "tool_opened", deviceId: "d3",
      eventAt: new Date(now.getTime() - DAY_MS),
      user: { isAuthenticated: false, userId: null, email: null, anonymousId: null },
    }),
  ]);

  const summary = await request(app)
    .get("/api/operations/summary?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const metrics = summary.body.metrics;
  assert.equal(metrics.anonymousVisitors.value, 3, "three distinct signed-out visitors");
  assert.equal(metrics.returningAnonymousVisitors.value, 1, "one of them came back");
  assert.equal(metrics.activeUsers.value, 0, "they are not counted as known accounts");
});

test("an unreadable analytics store is reported as unknown, never as inactivity", async (t) => {
  // Two servers so the analytics store can be stopped while the backend stays
  // up — the shape of a real outage (network blip, Atlas failover).
  const backendMongo = await MongoMemoryServer.create();
  const analyticsMongo = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await backendMongo.stop(); await analyticsMongo.stop().catch(() => {}); });

  process.env.MONGODB_URI = analyticsMongo.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = backendMongo.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  delete process.env.REPORTING_TIMEZONE;

  const backendClient = new MongoClient(process.env.BACKEND_MONGODB_URI);
  await backendClient.connect();
  t.after(() => backendClient.close());
  const analyticsClient = new MongoClient(process.env.MONGODB_URI);
  await analyticsClient.connect();

  const now = new Date();
  const user = new ObjectId();
  await backendClient.db("waysorted").collection("users").insertOne({
    _id: user, email: "outage@example.com", createdAt: new Date(now.getTime() - 300 * DAY_MS),
  });
  // Last login is old enough that login evidence alone would read as dormant.
  await backendClient.db("waysorted").collection("sessions").insertOne({
    user, source: "google", completed: true,
    completedAt: new Date(now.getTime() - 35 * DAY_MS), createdAt: new Date(now.getTime() - 35 * DAY_MS),
  });
  await analyticsClient.db("analytics").collection("plugin_analytics_events").insertMany(
    [1, 2, 3].map((d) => pluginEvent({
      eventId: `o${d}`, eventType: "feature_used",
      eventAt: new Date(now.getTime() - d * DAY_MS),
      user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null },
    }))
  );

  const healthy = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const healthyRow = healthy.body.items.find((r) => r.email === "outage@example.com");
  assert.equal(healthyRow.pluginActivityAvailable, true);
  assert.equal(healthyRow.pluginActiveDays, 3);
  assert.ok(healthyRow.segments.includes("returning"), "readable activity proves the return");
  assert.ok(!healthyRow.segments.includes("dormant"));

  // Take the analytics store away mid-flight.
  await analyticsClient.close();
  await closeDb();
  await analyticsMongo.stop();

  const broken = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const brokenRow = broken.body.items.find((r) => r.email === "outage@example.com");

  assert.equal(brokenRow.pluginActivityAvailable, false, "the read failure is reported");
  assert.equal(brokenRow.pluginActiveDays, null, "not zero — zero would be a measurement");
  assert.equal(brokenRow.activeDaysInRange, null, "likewise unknown, not zero");
  assert.ok(
    !brokenRow.segments.includes("dormant"),
    "an outage must not convert an active user into a churn-risk entry"
  );
  assert.ok(!brokenRow.segments.includes("at_risk"), "nor into at_risk");
});
