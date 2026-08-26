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
 * One dataset, every page, one set of answers.
 *
 * Each page grew its own definition of the same words. "Activated" meant any
 * tool use on the users table and a first credited reservation on the summary;
 * "jobs" meant committed reservations in one place and completed tool actions
 * in another; the tools page counted only billable work. This seeds a single
 * realistic world and asserts the pages agree about it, so a future change to
 * one of them cannot quietly drift from the rest.
 */
test("every page reports the same world", async (t) => {
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
  const ago = (ms) => new Date(now.getTime() - ms);
  const paid = new ObjectId();      // credited work, wallet, purchase
  const comped = new ObjectId();    // backend-granted subscription, no wallet
  const free = new ObjectId();      // telemetry-only work
  const idle = new ObjectId();      // signed up, never did anything

  await backend.collection("users").insertMany([
    { _id: paid, email: "paid@example.com", name: "Paid", createdAt: ago(20 * DAY) },
    { _id: comped, email: "comped@example.com", name: "Comped", createdAt: ago(15 * DAY) },
    { _id: free, email: "free@example.com", name: "Free", createdAt: ago(10 * DAY) },
    { _id: idle, email: "idle@example.com", name: "Idle", createdAt: ago(5 * DAY) },
  ]);
  await backend.collection("userbillings").insertOne({
    _id: new ObjectId(), user: paid, availableCredits: 40, heldCredits: 0,
    lifetimePurchasedCredits: 100, lifetimeSpentCredits: 60, subscriptionStatus: "active",
    subscriptionPlanCode: "pro", updatedAt: now,
  });
  await backend.collection("subscriptions").insertOne({
    _id: new ObjectId(), user: comped, planCode: "pro_monthly", status: "active",
    currentPeriodStart: ago(2 * DAY), currentPeriodEnd: new Date(now.getTime() + 28 * DAY),
    createdAt: ago(2 * DAY), updatedAt: now,
  });
  await backend.collection("sessions").insertMany(
    [paid, comped, free, idle].map((user) => ({
      _id: new ObjectId(), user, source: "figma", completed: true,
      completedAt: ago(9 * DAY), createdAt: ago(9 * DAY),
    }))
  );
  // Three billed runs for `paid`, all inside the window.
  await backend.collection("usagereservations").insertMany(
    [1, 2, 3].map((n) => ({
      _id: new ObjectId(), user: paid, toolCode: "palettable", featureCode: "export",
      status: "committed", creditsReserved: 5,
      createdAt: ago(n * DAY), committedAt: ago(n * DAY), updatedAt: ago(n * DAY),
    }))
  );

  const telemetry = (over) => ({
    schemaVersion: 2, isSemantic: true, source: "main", deviceId: "device-a",
    receivedAt: now, payload: {},
    user: { isAuthenticated: true, userId: null, anonymousId: null, email: null },
    ...over,
  });
  const as = (user, email) => ({ isAuthenticated: true, userId: String(user), anonymousId: null, email });

  await analytics.collection("plugin_analytics_events").insertMany([
    // `paid`: opens the tool, three runs — the same three the reservations
    // record. Plus two launch-burst completions nobody asked for.
    telemetry({ eventId: "p-bg1", eventType: "tool_action_completed", tool: "palettable", sessionId: "s-paid", eventAt: ago(3 * DAY + 5000), user: as(paid, "paid@example.com") }),
    telemetry({ eventId: "p-bg2", eventType: "tool_action_completed", tool: "icon-library", sessionId: "s-paid", eventAt: ago(3 * DAY + 4000), user: as(paid, "paid@example.com") }),
    telemetry({ eventId: "p-open", eventType: "tool_opened", tool: "palettable", sessionId: "s-paid", eventAt: ago(3 * DAY + 3000), user: as(paid, "paid@example.com") }),
    ...[1, 2, 3].map((n) => telemetry({
      eventId: `p-run${n}`, eventType: "tool_action_completed", tool: "palettable",
      sessionId: "s-paid", eventAt: ago(3 * DAY - n * 1000), user: as(paid, "paid@example.com"),
    })),
    // `free`: two runs in a tool that charges nothing, on two separate days.
    telemetry({ eventId: "f-open1", eventType: "tool_opened", tool: "icon-library", sessionId: "s-free-1", eventAt: ago(4 * DAY), user: as(free, "free@example.com") }),
    telemetry({ eventId: "f-run1", eventType: "tool_action_completed", tool: "icon-library", sessionId: "s-free-1", eventAt: ago(4 * DAY - 1000), user: as(free, "free@example.com") }),
    telemetry({ eventId: "f-open2", eventType: "tool_opened", tool: "icon-library", sessionId: "s-free-2", eventAt: ago(2 * DAY), user: as(free, "free@example.com") }),
    telemetry({ eventId: "f-run2", eventType: "tool_action_completed", tool: "icon-library", sessionId: "s-free-2", eventAt: ago(2 * DAY - 1000), user: as(free, "free@example.com") }),
    // `comped`: one run, and its session opens with unidentified events — the
    // pre-authentication head of the launch.
    telemetry({ eventId: "c-boot", eventType: "tool_opened", tool: "palettable", sessionId: "s-comped", deviceId: "device_1787730142122_z", eventAt: ago(DAY), user: { isAuthenticated: false, userId: null, anonymousId: null, email: null } }),
    telemetry({ eventId: "c-run", eventType: "tool_action_completed", tool: "palettable", sessionId: "s-comped", deviceId: "device_1787730142122_z", eventAt: ago(DAY - 1000), user: as(comped, "comped@example.com") }),
    // A genuine signed-out visitor who never signs in.
    telemetry({ eventId: "v-open", eventType: "tool_opened", tool: "unit-converter", sessionId: "s-visitor", deviceId: "device_1787726317271_q", eventAt: ago(6 * 3600 * 1000), user: { isAuthenticated: false, userId: null, anonymousId: null, email: null, name: "Visitor" } }),
    telemetry({ eventId: "v-run", eventType: "tool_action_completed", tool: "unit-converter", sessionId: "s-visitor", deviceId: "device_1787726317271_q", eventAt: ago(6 * 3600 * 1000 - 1000), user: { isAuthenticated: false, userId: null, anonymousId: null, email: null, name: "Visitor" } }),
  ]);

  const get = async (path) => (await request(app).get(path).set("Authorization", basicAuth).expect(200)).body;
  const [summary, users, tools, lifecycle, health] = await Promise.all([
    get("/api/operations/summary?days=30"),
    get("/api/operations/users?days=30&pageSize=100"),
    get("/api/operations/tools?days=30"),
    get("/api/operations/lifecycle?days=30"),
    get("/api/operations/data-health"),
  ]);

  // --- identity ----------------------------------------------------------
  assert.equal(
    users.items.filter((row) => row.anonymous).length,
    1,
    "only the visitor who never signed in stays anonymous; the pre-auth head of comped's launch does not"
  );
  const byEmail = Object.fromEntries(users.items.filter((row) => row.email).map((row) => [row.email, row]));

  // --- jobs, per user ----------------------------------------------------
  assert.equal(byEmail["paid@example.com"].creditedJobs, 3);
  assert.equal(byEmail["paid@example.com"].observedJobs, 3, "the two launch-burst completions are not jobs");
  assert.equal(byEmail["paid@example.com"].completedJobs, 3, "and the same three runs are not counted twice");
  assert.equal(byEmail["free@example.com"].completedJobs, 2);
  assert.equal(byEmail["free@example.com"].completedJobsSource, "observed");
  assert.equal(byEmail["comped@example.com"].completedJobs, 1);
  assert.equal(byEmail["idle@example.com"].completedJobs, 0);

  // --- jobs, page to page ------------------------------------------------
  //
  // Every reservation here has a user, so nothing is unattributable and the
  // summary must equal the sum of the rows exactly.
  const rowJobs = users.items.reduce((sum, row) => sum + row.jobsInRange, 0);
  assert.equal(summary.metrics.completedJobs.value, rowJobs, "summary equals the sum of the rows");
  assert.equal(summary.metrics.completedJobs.value, 7, "3 paid + 2 free + 1 comped + 1 visitor");
  assert.equal(tools.summary.observedJobs, 7, "the tools page counts the same finished runs");
  assert.equal(tools.summary.backgroundCompletions, 2, "and sets aside the same launch burst");
  assert.equal(tools.summary.completedJobs, 3, "credited work is reported separately and unchanged");

  // --- lifecycle ---------------------------------------------------------
  assert.equal(byEmail["comped@example.com"].lifecycleStage, "customer", "a backend-granted subscription is commercial standing");
  assert.equal(byEmail["comped@example.com"].walletStatus, "missing", "and the missing wallet is still reported");
  assert.equal(byEmail["paid@example.com"].lifecycleStage, "customer");
  assert.ok(byEmail["free@example.com"].segments.includes("returning"), "two days of tool use is a return");
  assert.ok(byEmail["idle@example.com"].segments.includes("not_activated"));

  // --- activation means one thing everywhere -----------------------------
  const activatedRows = users.items.filter((row) => !row.anonymous && row.segments.includes("activated")).length;
  assert.equal(activatedRows, 3, "paid, free and comped all used a tool");
  const usedATool = lifecycle.stages.find((stage) => stage.key === "activated");
  assert.equal(
    usedATool.users,
    users.items.filter((row) => !row.anonymous && row.segments.includes("activated") && row.segments.includes("new")).length,
    "the funnel counts the signup cohort by the same rule the table uses"
  );

  // --- coverage is stated, not implied -----------------------------------
  // Most sessions here share one persistent device id, which is what a healthy
  // plugin build looks like — so the detector must NOT cry per-launch.
  assert.equal(health.components.identity.status, "stable");
  assert.equal(health.coverage.toolJobs.backgroundCompletionsExcluded, 2);
  assert.deepEqual(health.coverage.toolJobs.ungatedTools, [], "every tool here reports opens");
});
