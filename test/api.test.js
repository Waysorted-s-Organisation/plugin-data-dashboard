import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";

import { MongoClient, ObjectId } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");

test("credits-first operations APIs", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await closeDb(); await mongod.stop(); });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";

  const client = new MongoClient(process.env.BACKEND_MONGODB_URI);
  await client.connect();
  t.after(() => client.close());
  const db = client.db("waysorted");
  const [u19, u20, u21, noWallet] = [new ObjectId(), new ObjectId(), new ObjectId(), new ObjectId()];
  const [rCommitted, rReleased, rCompensated, rFeature, rUnattributed] = [new ObjectId(), new ObjectId(), new ObjectId(), new ObjectId(), new ObjectId()];
  const now = new Date();
  const hourAgo = new Date(now.getTime() - 60 * 60 * 1000);

  await db.collection("users").insertMany([
    { _id: u19, email: "alice@example.com", name: "Alice", creditsRemaining: 999, createdAt: now },
    { _id: u20, email: "bob@example.com", name: "Bob", creditsRemaining: 999, createdAt: now },
    { _id: u21, email: "carol@example.com", name: "Carol", creditsRemaining: 999, createdAt: now },
    { _id: noWallet, email: "missing@example.com", name: "Missing Wallet", creditsRemaining: 50, createdAt: now },
  ]);
  await db.collection("userbillings").insertMany([
    { user: u19, availableCredits: 19, heldCredits: 2, lifetimeSpentCredits: 5, lifetimePurchasedCredits: 20, subscriptionStatus: "active", subscriptionPlanCode: "pro", updatedAt: now },
    { user: u20, availableCredits: 20, heldCredits: 0, lifetimeSpentCredits: 4, lifetimePurchasedCredits: 20, subscriptionStatus: "inactive", updatedAt: now },
    { user: u21, availableCredits: 21, heldCredits: 0, lifetimeSpentCredits: 6, lifetimePurchasedCredits: 30, subscriptionStatus: "active", updatedAt: now },
  ]);
  await db.collection("creditledgers").insertMany([
    { user: u19, reservation: rCommitted, reason: "reservation_hold", deltaCredits: -5, toolCode: "palette", idempotencyKey: "hold-1", createdAt: hourAgo },
    { user: u19, reservation: rCommitted, reason: "reservation_commit", deltaCredits: 0, toolCode: "palette", idempotencyKey: "commit-1", createdAt: now },
    { user: u19, reservation: rReleased, reason: "reservation_hold", deltaCredits: -3, toolCode: "pdf", idempotencyKey: "hold-2", createdAt: hourAgo },
    { user: u19, reservation: rReleased, reason: "reservation_release", deltaCredits: 3, toolCode: "pdf", idempotencyKey: "release-2", createdAt: now },
    { user: u20, reservation: rCompensated, reason: "reservation_hold", deltaCredits: -4, toolCode: "export", idempotencyKey: "hold-3", createdAt: hourAgo },
    { user: u20, reservation: rCompensated, reason: "reservation_commit", deltaCredits: 0, toolCode: "export", idempotencyKey: "commit-3", createdAt: hourAgo },
    { user: u20, reservation: rCompensated, reason: "compensation_credit", deltaCredits: 4, toolCode: "export", idempotencyKey: "compensate-3", createdAt: now },
    { user: u21, reservation: rFeature, reason: "reservation_hold", deltaCredits: -2, featureCode: "image-cleanup", idempotencyKey: "hold-4", createdAt: hourAgo },
    { user: u21, reservation: rFeature, reason: "reservation_commit", deltaCredits: 0, featureCode: "image-cleanup", idempotencyKey: "commit-4", createdAt: now },
    { user: u21, reservation: rUnattributed, reason: "reservation_hold", deltaCredits: -1, idempotencyKey: "hold-5", createdAt: hourAgo },
    { user: u21, reservation: rUnattributed, reason: "reservation_commit", deltaCredits: 0, idempotencyKey: "commit-5", createdAt: now },
  ]);
  await db.collection("sessions").insertMany([
    { sessionId: "oauth", user: u19, source: "web", completed: true, completedAt: now, createdAt: hourAgo },
    { sessionId: "otp", user: u20, createdAt: now },
    { sessionId: "pending", source: "web", createdAt: now },
  ]);

  await t.test("requires owner authentication", async () => {
    const response = await request(app).get("/api/operations/credits/overview");
    assert.equal(response.status, 401);
  });

  await t.test("uses billing wallets and committed reservation lifecycles", async () => {
    const response = await request(app).get("/api/operations/credits/overview?days=30").set("Authorization", basicAuth);
    assert.equal(response.status, 200);
    assert.equal(response.body.summary.wallets, 3);
    assert.equal(response.body.summary.walletsMissing, 1);
    assert.equal(response.body.summary.totalAvailableCredits, 60);
    assert.equal(response.body.summary.totalHeldCredits, 2);
    assert.equal(response.body.summary.lowCreditUsers, 2);
    assert.equal(response.body.summary.completedUsesInRange, 4);
    assert.equal(response.body.summary.creditsSpentInRange, 8);
    assert.equal(response.body.tools.find((row) => row.tool === "palette").creditsSpent, 5);
    assert.equal(response.body.tools.some((row) => row.tool === "pdf"), false);
    assert.equal(response.body.tools.find((row) => row.tool === "export").creditsSpent, 0);
    assert.equal(response.body.tools.find((row) => row.tool === "image-cleanup").creditsSpent, 2);
    assert.equal(response.body.dataQuality.unattributedUses, 1);
  });

  await t.test("searches email and preserves missing-wallet state", async () => {
    const alice = await request(app).get("/api/operations/credits/users?search=ALICE&pageSize=25").set("Authorization", basicAuth);
    assert.equal(alice.status, 200);
    assert.equal(alice.body.items.length, 1);
    assert.equal(alice.body.items[0].availableCredits, 19);
    assert.equal(alice.body.items[0].legacyCreditsRemaining, 999);

    const missing = await request(app).get("/api/operations/credits/users?search=missing%40example.com").set("Authorization", basicAuth);
    assert.equal(missing.body.items[0].walletStatus, "missing");
    assert.equal(missing.body.items[0].availableCredits, null);
    assert.equal(missing.body.pagination.pageSize, 25);
  });

  await t.test("filters exact low-credit boundary", async () => {
    const response = await request(app).get("/api/operations/credits/users?lowCredit=true&pageSize=25").set("Authorization", basicAuth);
    assert.deepEqual(response.body.items.map((row) => row.availableCredits).sort((a, b) => a - b), [19, 20]);
  });

  await t.test("returns detailed wallet, tools, and raw ledger", async () => {
    const response = await request(app).get(`/api/operations/credits/users/${u19}?days=30`).set("Authorization", basicAuth);
    assert.equal(response.status, 200);
    assert.equal(response.body.user.email, "alice@example.com");
    assert.equal(response.body.tools[0].tool, "palette");
    assert.equal(response.body.ledger.length, 4);
  });

  await t.test("groups successful OAuth and OTP sessions", async () => {
    const response = await request(app).get("/api/operations/activity/recent-users?days=7&pageSize=25").set("Authorization", basicAuth);
    assert.equal(response.status, 200);
    assert.equal(response.body.summary.users, 2);
    assert.equal(response.body.summary.logins, 2);
    assert.equal(response.body.items.find((row) => row.email === "alice@example.com").latestSource, "web");
    assert.equal(response.body.items.find((row) => row.email === "bob@example.com").latestSource, "otp");
  });

  await t.test("health is sanitized", async () => {
    const response = await request(app).get("/api/operations/health").set("Authorization", basicAuth);
    assert.equal(response.status, 200);
    assert.equal(response.body.backendDatabase.users, 4);
    assert.equal(JSON.stringify(response.body).includes(mongod.getUri()), false);
  });

  await t.test("newsletter customer uses billing and credit ledger", async (customerTest) => {
    const upstream = createServer((req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        subscriber: { id: 42, email: "alice@example.com", name: "Alice", status: "active" },
        notification_profile: { id: 9, external_user_id: String(u19), email: "alice@example.com" },
        preferences: [], suppressions: [], enrollments: [], deliveries: [], broadcast_history: [],
      }));
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    customerTest.after(() => upstream.close());
    process.env.NEWSLETTER_API_URL = `http://127.0.0.1:${upstream.address().port}`;
    process.env.NEWSLETTER_MANAGEMENT_TOKEN = "server-only-token";
    const response = await request(app).get("/api/newsletter/customers/42").set("Authorization", basicAuth);
    assert.equal(response.status, 200);
    assert.equal(response.body.billing.availableCredits, 19);
    assert.equal(response.body.credit_activity.tools[0].tool, "palette");
    assert.equal(response.body.data_coverage.billing_wallet, true);
    assert.equal("product_activity" in response.body, false);
    assert.equal(JSON.stringify(response.body).includes("server-only-token"), false);
  });

  await t.test("retains the raw plugin ingest compatibility route", async () => {
    const response = await request(app).post("/api/plugin-analytics/ingest").send({
      source: "figma-plugin-main",
      sessionId: "ingest-session",
      deviceId: "ingest-device",
      user: { isAuthenticated: true, userId: String(u19), email: "alice@example.com" },
      events: [{ eventType: "tool_used", tool: "palette", payload: { action: "export" } }],
    });
    assert.equal(response.status, 202);
    assert.equal(response.body.inserted, 1);
    const analyticsClient = new MongoClient(process.env.MONGODB_URI);
    await analyticsClient.connect();
    const stored = await analyticsClient.db("analytics").collection("plugin_analytics_events").findOne({ sessionId: "ingest-session" });
    await analyticsClient.close();
    assert.equal(stored.tool, "palette");
    assert.equal(stored.user.userId, String(u19));
  });

  await t.test("old analytics APIs and pages are removed", async () => {
    for (const path of ["/api/plugin-analytics/dashboard", "/api/plugin-analytics/features", "/api/plugin-analytics/heatmap", "/api/plugin-analytics/stats", "/features.html", "/heatmap.html"]) {
      const response = await request(app).get(path).set("Authorization", basicAuth);
      assert.equal(response.status, 404, path);
    }
    const root = await request(app).get("/").set("Authorization", basicAuth);
    assert.equal(root.status, 200);
    assert.match(root.text, /<h1>Credits<\/h1>/);
  });
});

test("operations APIs fail closed without backend configuration", async () => {
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  delete process.env.BACKEND_MONGODB_URI;
  const response = await request(app).get("/api/operations/credits/overview").set("Authorization", basicAuth);
  assert.equal(response.status, 503);
  assert.equal(response.body.code, "BACKEND_DATABASE_NOT_CONFIGURED");
});
