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
 * The credits page is built from the credit ledger while the tools and users
 * pages are built from usage reservations. Different sources for the same work
 * is defensible — the ledger is the billing record — but the two must at least
 * agree on what a tool is called and on what counts as a use, and they did not.
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
  return client.db("waysorted");
}

const held = (user, reservation, toolCode, credits, at) =>
  ({ user, reservation, reason: "reservation_hold", deltaCredits: -credits, toolCode, createdAt: at });
const committed = (user, reservation, toolCode, at) =>
  ({ user, reservation, reason: "reservation_commit", deltaCredits: 0, toolCode, createdAt: at });

test("one product is one row, whichever alias the ledger recorded", async (t) => {
  const backend = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "a@example.com", createdAt: now });
  await backend.collection("userbillings").insertOne({ _id: new ObjectId(), user, availableCredits: 10, updatedAt: now });

  // The same product, written three ways across the ledger's history.
  const rows = [];
  for (const [n, code] of [[1, "frame_gallery"], [2, "frame-gallery"], [3, "frames"]]) {
    const reservation = new ObjectId();
    rows.push(held(user, reservation, code, n, new Date(now.getTime() - 60000)), committed(user, reservation, code, now));
  }
  await backend.collection("creditledgers").insertMany(rows);

  const overview = await request(app)
    .get("/api/operations/credits/overview?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(overview.body.tools.length, 1, "three aliases are one product, not three");
  const [tool] = overview.body.tools;
  assert.equal(tool.toolLabel, "Frames to PDF", "named the way every other page names it");
  assert.equal(tool.completedUses, 3, "and its uses are added together rather than split");
  assert.equal(tool.creditsSpent, 6);
  assert.equal(tool.userCount, 1, "one person, counted once");
});

test("a refunded charge is not a use, on the bars as well as in the total", async (t) => {
  const backend = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "b@example.com", createdAt: now });

  const real = new ObjectId();
  const refunded = new ObjectId();
  await backend.collection("creditledgers").insertMany([
    held(user, real, "palettable", 5, new Date(now.getTime() - 60000)),
    committed(user, real, "palettable", now),
    held(user, refunded, "palettable", 4, new Date(now.getTime() - 60000)),
    committed(user, refunded, "palettable", now),
    { user, reservation: refunded, reason: "compensation_credit", deltaCredits: 4, toolCode: "palettable", createdAt: now },
  ]);

  const overview = await request(app)
    .get("/api/operations/credits/overview?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const [tool] = overview.body.tools;
  assert.equal(overview.body.summary.completedUsesInRange, 1, "the charge that was reversed is not a use");
  assert.equal(tool.completedUses, 1, "and the bar agrees with the total printed above it");
  assert.equal(tool.compensatedUses, 1, "the reversal is still visible rather than hidden");
  assert.equal(tool.creditsSpent, 5, "only the charge that stuck");
});

test("the commercial panel answers for the period it is showing", async (t) => {
  const backend = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "c@example.com", createdAt: now });
  await backend.collection("subscriptions").insertMany([
    { _id: new ObjectId(), user, planCode: "pro", status: "active", createdAt: new Date(now.getTime() - 2 * DAY), updatedAt: new Date(now.getTime() - 2 * DAY) },
    { _id: new ObjectId(), user, planCode: "pro", status: "cancelled", createdAt: new Date(now.getTime() - 200 * DAY), updatedAt: new Date(now.getTime() - 200 * DAY) },
  ]);
  await backend.collection("startergrants").insertMany([
    { _id: new ObjectId(), user, grantedCredits: 20, status: "granted", grantedAt: new Date(now.getTime() - 2 * DAY), createdAt: new Date(now.getTime() - 2 * DAY) },
    { _id: new ObjectId(), user, grantedCredits: 0, status: "blocked", blockedAt: new Date(now.getTime() - 200 * DAY), createdAt: new Date(now.getTime() - 200 * DAY) },
  ]);

  const recent = await request(app)
    .get("/api/operations/commercial?days=7")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.deepEqual(recent.body.subscriptions, { active: 1 }, "the cancellation was 200 days ago");
  assert.deepEqual(recent.body.starterGrants, { granted: 1 });
  assert.equal(recent.body.period.days, 7, "and the payload says which period it answered for");

  const all = await request(app)
    .get("/api/operations/commercial?days=365")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.equal(all.body.subscriptions.cancelled, 1, "widening the range brings the older rows in");
  assert.equal(all.body.starterGrants.blocked, 1);
});

test("feedback is grouped by the product it is about, not by the URL it came from", async (t) => {
  const backend = await harness(t);
  const now = new Date();
  const user = new ObjectId();
  await backend.collection("users").insertOne({ _id: user, email: "d@example.com", createdAt: now });
  // Legacy rows carry a tool id; current rows carry the page path.
  await backend.collection("feedback").insertOne({
    _id: new ObjectId(), userId: user, feedbackType: "nps", score: 8, toolId: "frame_gallery", createdAt: now,
  });
  await backend.collection("feedbacks").insertMany([
    { _id: new ObjectId(), userId: user, rating: 4, comment: "good", path: "/tools/frames", createdAt: now },
    { _id: new ObjectId(), userId: user, rating: 3, comment: "hmm", path: "/settings/billing", createdAt: now },
  ]);

  const feedback = await request(app)
    .get("/api/operations/feedback?days=90")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(
    feedback.body.feedbackByTool["Frames to PDF"],
    2,
    "a tool id and a path pointing at the same tool are the same tool"
  );
  assert.equal(
    feedback.body.feedbackByTool["/settings/billing"],
    1,
    "a path that is not a tool keeps its own name rather than being dressed up as a product"
  );
});
