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
 * The conversion tracker is co-owned: the dashboard knows when someone signed
 * up, what they used and what they pay; a person knows where they came from and
 * what was said to them. The export must carry the first set and never claim
 * the second.
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

const telemetry = (over) => ({
  schemaVersion: 2, isSemantic: true, source: "main", deviceId: "device-a",
  receivedAt: new Date(), payload: {},
  user: { isAuthenticated: true, userId: null, anonymousId: null, email: null },
  ...over,
});

test("the export carries what the dashboard knows and claims nothing else", async (t) => {
  const { analytics, backend } = await harness(t);
  const now = new Date();
  const ago = (ms) => new Date(now.getTime() - ms);
  const premium = new ObjectId();
  const comped = new ObjectId();
  const free = new ObjectId();
  const lapsed = new ObjectId();

  await backend.collection("users").insertMany([
    { _id: premium, email: "Premium@Example.com", name: "Paying Pat", createdAt: ago(60 * DAY) },
    { _id: comped, email: "comped@example.com", name: "Comped Chris", createdAt: ago(40 * DAY) },
    { _id: free, email: "free@example.com", name: "Free Fran", createdAt: ago(3 * DAY) },
    { _id: lapsed, email: "lapsed@example.com", name: "Lapsed Lee", createdAt: ago(120 * DAY) },
  ]);
  await backend.collection("userbillings").insertOne({
    _id: new ObjectId(), user: premium, availableCredits: 50,
    lifetimePurchasedCredits: 200, subscriptionStatus: "active", updatedAt: now,
  });
  await backend.collection("subscriptions").insertOne({
    _id: new ObjectId(), user: comped, planCode: "pro_monthly", status: "active",
    currentPeriodStart: ago(2 * DAY), currentPeriodEnd: new Date(now.getTime() + 28 * DAY),
    createdAt: ago(2 * DAY), updatedAt: now,
  });
  await backend.collection("sessions").insertMany(
    [premium, comped, free, lapsed].map((user) => ({
      _id: new ObjectId(), user, source: "figma", completed: true,
      completedAt: ago(100 * DAY), createdAt: ago(100 * DAY),
    }))
  );

  const as = (user, email) => ({ isAuthenticated: true, userId: String(user), anonymousId: null, email });
  await analytics.collection("plugin_analytics_events").insertMany([
    telemetry({ eventId: "o1", eventType: "tool_opened", tool: "palettable", sessionId: "s1", eventAt: ago(2 * DAY), user: as(premium, "premium@example.com") }),
    telemetry({ eventId: "c1", eventType: "tool_action_completed", tool: "palettable", sessionId: "s1", eventAt: ago(2 * DAY - 1000), user: as(premium, "premium@example.com") }),
    telemetry({ eventId: "o2", eventType: "tool_opened", tool: "frames", sessionId: "s2", eventAt: ago(DAY), user: as(comped, "comped@example.com") }),
    telemetry({ eventId: "c2", eventType: "tool_action_completed", tool: "frames", sessionId: "s2", eventAt: ago(DAY - 1000), user: as(comped, "comped@example.com") }),
    telemetry({ eventId: "o3", eventType: "tool_opened", tool: "comment-summarizer", sessionId: "s3", eventAt: ago(2 * DAY), user: as(free, "free@example.com") }),
    telemetry({ eventId: "c3", eventType: "tool_action_completed", tool: "comment-summarizer", sessionId: "s3", eventAt: ago(2 * DAY - 1000), user: as(free, "free@example.com") }),
  ]);

  const response = await request(app)
    .get("/api/exports/users-sheet?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const byEmail = Object.fromEntries(response.body.rows.map((row) => [row.email.toLowerCase(), row]));
  assert.equal(response.body.rows.length, 4);

  // Source (E) is the acquisition channel. The dashboard knows the login source
  // — a different fact — and must not put one where the other goes.
  assert.deepEqual(response.body.humanOwnedColumns, ["E"]);
  assert.ok(
    !response.body.columns.some((column) => column.column === "E"),
    "no exported column targets E"
  );
  for (const row of response.body.rows) {
    assert.ok(!("source" in row), "the payload does not even carry a source field to be written by mistake");
  }

  assert.equal(byEmail["premium@example.com"].plan, "Premium", "a purchase is Premium");
  assert.equal(byEmail["comped@example.com"].plan, "Premium", "so is a subscription granted from the backend, wallet or no wallet");
  assert.equal(byEmail["free@example.com"].plan, "Free");

  assert.equal(byEmail["free@example.com"].activeStatus, "New", "signed up inside the window");
  assert.equal(byEmail["lapsed@example.com"].activeStatus, "Churned", "no sign of life for 100 days");
  assert.equal(byEmail["premium@example.com"].activeStatus, "Active");

  // The tool vocabulary matches the sheet's own dropdown, including the alias:
  // the event says "frames", the sheet says "Frames to PDF".
  assert.equal(byEmail["comped@example.com"].likedFeature, "Frames to PDF");
  assert.equal(byEmail["premium@example.com"].likedFeature, "Palettable");
  assert.equal(byEmail["lapsed@example.com"].likedFeature, "", "no tool use is blank, not a guess");

  assert.equal(byEmail["free@example.com"].signupDate, new Date(now.getTime() - 3 * DAY).toISOString().slice(0, 10));
  assert.match(byEmail["premium@example.com"].lastActiveDate, /^\d{4}-\d{2}-\d{2}$/);

  // Sorted oldest signup first, so a backfill appends in a sensible order.
  const dates = response.body.rows.map((row) => row.signupDate);
  assert.deepEqual(dates, [...dates].sort(), "rows arrive in signup order");
});

test("status is left blank rather than asserting churn from data that was not read", async (t) => {
  const { backend } = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({
    _id: user, email: "unknown@example.com", name: "Unknown Ursula",
    createdAt: new Date(now.getTime() - 120 * DAY),
  });
  await backend.collection("sessions").insertOne({
    _id: new ObjectId(), user, source: "figma", completed: true,
    completedAt: new Date(now.getTime() - 119 * DAY), createdAt: new Date(now.getTime() - 119 * DAY),
  });
  // The analytics store is pointed somewhere unreachable, so plugin activity —
  // the only evidence that would disprove "this person stopped" — cannot be
  // read. Writing "Churned" into a spreadsheet a human then acts on would be
  // asserting a conclusion from data that was never retrieved.
  process.env.MONGODB_URI = "mongodb://127.0.0.1:1/analytics";

  const response = await request(app)
    .get("/api/exports/users-sheet?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const [row] = response.body.rows;
  assert.equal(row.activeStatus, "", "unknown is blank, not churned");
  assert.equal(response.body.coverage.statusUnavailable, 1);
  assert.match(response.body.coverage.message, /could not be read/);
});

test("the export is behind the same gate as the rest of the dashboard", async (t) => {
  await harness(t);
  await request(app).get("/api/exports/users-sheet").expect(401);
});
