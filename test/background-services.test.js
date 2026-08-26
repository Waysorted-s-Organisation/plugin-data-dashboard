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
 * Opening the plugin starts a fleet of background services — sync, credit
 * refresh, license checks, prefetch — and they announce finished work the same
 * way a user's run does. Counting those made the job column measure how often
 * someone opened the plugin rather than how much they got done.
 *
 * The rule is not a list of background action names, which are the plugin's
 * internal vocabulary and change without notice. It is that nobody opened the
 * tool: a person must open a tool to run it, a background service need not. A
 * completion counts only when the same session shows a tool_opened for that
 * same tool at or before it.
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
  return { analytics: client.db("analytics"), backend: client.db("waysorted") };
}

const at = (base, seconds) => new Date(base.getTime() + seconds * 1000);

const event = (over) => ({
  schemaVersion: 2,
  isSemantic: true,
  source: "main",
  tool: "palettable",
  eventType: "tool_action_completed",
  sessionId: "launch-1",
  deviceId: "device-1",
  receivedAt: new Date(),
  payload: {},
  user: { isAuthenticated: true, userId: null, anonymousId: null, email: null },
  ...over,
});

test("services that fire at launch are not counted as finished jobs", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const launch = new Date(now.getTime() - 10 * 60000);
  const user = new ObjectId();
  await backend.collection("users").insertOne({
    _id: user, email: "worker@example.com", createdAt: new Date(now.getTime() - 30 * DAY),
  });
  const asUser = { isAuthenticated: true, userId: String(user), anonymousId: null, email: "worker@example.com" };

  await analytics.collection("plugin_analytics_events").insertMany([
    // The launch burst. Four services report finished work on two real tools,
    // seconds after the plugin opened, with nobody having opened either tool.
    event({ eventId: "bg1", tool: "palettable", eventAt: at(launch, 1), user: asUser }),
    event({ eventId: "bg2", tool: "palettable", eventAt: at(launch, 2), user: asUser }),
    event({ eventId: "bg3", tool: "icon-library", eventAt: at(launch, 2), user: asUser }),
    event({ eventId: "bg4", tool: "icon-library", eventAt: at(launch, 3), user: asUser }),
    // Two minutes later the person opens a tool and runs it twice.
    event({ eventId: "open1", eventType: "tool_opened", tool: "palettable", eventAt: at(launch, 120), user: asUser }),
    event({ eventId: "real1", tool: "palettable", eventAt: at(launch, 140), user: asUser }),
    event({ eventId: "real2", tool: "palettable", eventAt: at(launch, 160), user: asUser }),
    // icon-library is opened in a later session, which proves the tool does
    // report opens — so its launch-burst completions are judged, not excused.
    event({ eventId: "open2", eventType: "tool_opened", tool: "icon-library", sessionId: "launch-2", eventAt: at(launch, 400), user: asUser }),
    event({ eventId: "real3", tool: "icon-library", sessionId: "launch-2", eventAt: at(launch, 420), user: asUser }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "worker@example.com");
  assert.equal(row.observedJobs, 3, "two palettable runs and one icon-library run; the four launch events are not jobs");

  const tools = await request(app)
    .get("/api/operations/tools?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.equal(tools.body.summary.observedJobs, 3, "the tools page applies the identical rule");
  assert.equal(tools.body.summary.backgroundCompletions, 4, "and says how much it set aside");
});

test("a completion before the tool was opened is background, after it is work", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const launch = new Date(now.getTime() - 10 * 60000);
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "boundary@example.com", createdAt: now });
  const asUser = { isAuthenticated: true, userId: String(user), anonymousId: null, email: "boundary@example.com" };

  await analytics.collection("plugin_analytics_events").insertMany([
    event({ eventId: "before", eventAt: at(launch, 5), user: asUser }),
    event({ eventId: "open", eventType: "tool_opened", eventAt: at(launch, 10), user: asUser }),
    // Exactly on the open counts: the open and the first action of a fast run
    // can share a second.
    event({ eventId: "same", eventAt: at(launch, 10), user: asUser }),
    event({ eventId: "after", eventAt: at(launch, 15), user: asUser }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "boundary@example.com");
  assert.equal(row.observedJobs, 2, "the one before the open is not work the user asked for");
});

test("a tool that never reports an open cannot be judged, so its jobs are kept", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "noopens@example.com", createdAt: now });
  const asUser = { isAuthenticated: true, userId: String(user), anonymousId: null, email: "noopens@example.com" };

  // This build never emits tool_opened at all. Applying the rule would delete
  // every job it has, so the rule stands down and the tool is named instead.
  await analytics.collection("plugin_analytics_events").insertMany(
    [1, 2, 3].map((n) => event({
      eventId: `u${n}`, tool: "comment-summarizer", eventAt: new Date(now.getTime() - n * 60000), user: asUser,
    }))
  );

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "noopens@example.com");
  assert.equal(row.observedJobs, 3, "over-reporting beats deleting real work");

  const health = await request(app)
    .get("/api/operations/data-health")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.deepEqual(health.body.coverage.toolJobs.ungatedTools, ["comment-summarizer"]);
  assert.equal(health.body.coverage.toolJobs.backgroundCompletionsExcluded, 0);
});

test("a session straddling the window boundary keeps the open that justifies it", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "straddle@example.com", createdAt: new Date(now.getTime() - 30 * DAY) });
  const asUser = { isAuthenticated: true, userId: String(user), anonymousId: null, email: "straddle@example.com" };

  // A 7-day window starts between the open and the completion. Looking opens up
  // only inside the window would have called this finished run background.
  await analytics.collection("plugin_analytics_events").insertMany([
    event({ eventId: "o", eventType: "tool_opened", eventAt: new Date(now.getTime() - 7 * DAY - 60000), user: asUser }),
    event({ eventId: "c", eventAt: new Date(now.getTime() - 7 * DAY + 60000), user: asUser }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=7")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "straddle@example.com");
  assert.equal(row.observedJobsInRange, 1, "the open sits just outside the window but still counts");
});
