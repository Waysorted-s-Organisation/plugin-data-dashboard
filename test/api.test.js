import test from "node:test";
import assert from "node:assert";
import request from "supertest";
import app from "../src/server.js";
import { MongoMemoryServer } from "mongodb-memory-server";
import { closeDatabases, getDb } from "../src/db.js";

// Mock auth middleware by overriding env
process.env.DASHBOARD_BASIC_AUTH_USER = "test";
process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
const authString = "Basic " + Buffer.from("test:test").toString("base64");

let mongod;

test("Dashboard APIs", async (t) => {
  mongod = await MongoMemoryServer.create();
  process.env.MONGODB_URI = mongod.getUri();
  
  const db = await getDb();
  const eventsColl = db.collection("plugin_analytics_events");
  const engageColl = db.collection("plugin_engagement");
  const usersColl = db.collection("users");
  const userBillingColl = db.collection("userbillings");
  const creditLedgerColl = db.collection("creditledgers");

  // Setup mock data
  await eventsColl.deleteMany({});
  await engageColl.deleteMany({});
  await usersColl.deleteMany({});
  await userBillingColl.deleteMany({});
  await creditLedgerColl.deleteMany({});

  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  await usersColl.insertMany([
    {
      _id: "user1",
      email: "alice@example.com",
      name: "Alice",
      creditsRemaining: 45,
      createdAt: now,
      updatedAt: now,
    },
    {
      _id: "user-ingest",
      email: "ingest@example.com",
      name: "Ingest User",
      creditsRemaining: 17,
      createdAt: now,
      updatedAt: now,
    },
    {
      _id: "backend-only-user",
      email: "backend@example.com",
      name: "Backend Only",
      creditsRemaining: 82,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await userBillingColl.insertMany([
    {
      _id: "billing-user1",
      user: "user1",
      availableCredits: 45,
      heldCredits: 0,
      lifetimeSpentCredits: 9,
      subscriptionStatus: "active",
    },
    {
      _id: "billing-user-ingest",
      user: "user-ingest",
      availableCredits: 17,
      heldCredits: 0,
      lifetimeSpentCredits: 3,
      subscriptionStatus: "inactive",
    },
    {
      _id: "billing-backend-only-user",
      user: "backend-only-user",
      availableCredits: 82,
      heldCredits: 0,
      lifetimeSpentCredits: 0,
      subscriptionStatus: "inactive",
    },
  ]);

  await creditLedgerColl.insertMany([
    {
      _id: "ledger-user1-1",
      user: "user1",
      deltaCredits: -5,
      reason: "reservation_hold",
      toolCode: "frame_gallery",
      reservation: "reservation-1",
      createdAt: now,
      updatedAt: now,
    },
    {
      _id: "ledger-user1-2",
      user: "user1",
      deltaCredits: 2,
      reason: "reservation_release",
      toolCode: "frame_gallery",
      reservation: "reservation-1",
      createdAt: now,
      updatedAt: now,
    },
    {
      _id: "ledger-user1-3",
      user: "user1",
      deltaCredits: 0,
      reason: "reservation_commit",
      toolCode: "frame_gallery",
      reservation: "reservation-1",
      createdAt: now,
      updatedAt: now,
    },
    {
      _id: "ledger-user-ingest-1",
      user: "user-ingest",
      deltaCredits: -4,
      reason: "reservation_hold",
      toolCode: "unit_converter",
      reservation: "reservation-2",
      createdAt: now,
      updatedAt: now,
    },
    {
      _id: "ledger-user-ingest-2",
      user: "user-ingest",
      deltaCredits: 0,
      reason: "reservation_commit",
      toolCode: "unit_converter",
      reservation: "reservation-2",
      createdAt: now,
      updatedAt: now,
    },
  ]);

  await eventsColl.insertMany([
    {
      eventType: "tool_action",
      eventAt: yesterday,
      sessionId: "session-yesterday-user1",
      tool: "frame-gallery",
      user: {
        isAuthenticated: true,
        userId: "user1",
        name: "Alice",
        email: "alice@example.com",
      },
    },
    {
      eventType: "tool_action",
      eventAt: yesterday,
      sessionId: "session-yesterday-anon",
      tool: "import-tool",
      user: {
        isAuthenticated: false,
        anonymousId: "anon-1",
      },
    },
    {
      eventType: "tool_favorite_changed",
      eventAt: now,
      sessionId: "session-like",
      tool: "import-tool",
      payload: { isFavorited: true },
      user: {
        isAuthenticated: true,
        userId: "user1",
        name: "Alice",
      }
    },
    {
      eventType: "tool_action",
      eventAt: now,
      sessionId: "session-save",
      tool: "unit-converter",
      payload: { action: "save-preset" },
      user: {
        isAuthenticated: true,
        userId: "user-ingest",
        name: "Ingest User",
      }
    },
    {
      eventType: "tool_action",
      eventAt: now,
      sessionId: "session-follow",
      tool: "profile",
      payload: { action: "emailEntered" },
      user: {
        isAuthenticated: true,
        userId: "user-ingest",
        name: "Ingest User",
      }
    },
    {
      eventType: "credit_consumed",
      eventAt: now,
      tool: "import-tool",
      user: {
        isAuthenticated: true,
        userId: "user1",
        name: "Alice",
        creditsRemaining: 45
      }
    },
    {
      eventType: "tool_time_spent",
      eventAt: now,
      tool: "import-tool",
      payload: { durationMs: 10000 },
      user: {
        isAuthenticated: true,
        userId: "user1",
        name: "Alice"
      }
    }
  ]);

  await t.test("GET /api/plugin-analytics/mau", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/mau")
      .set("Authorization", authString);
      
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.mau, 3);
    assert.strictEqual(res.body.authenticated, 2);
  });

  await t.test("GET /api/plugin-analytics/credit-consumption", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/credit-consumption")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.users.length, 3);
    assert.strictEqual(res.body.users[0]._id, "user1");
    assert.strictEqual(res.body.users[0].email, "alice@example.com");
    assert.strictEqual(res.body.users[0].totalConsumed, 9);
    assert.strictEqual(res.body.users[0].creditsRemaining, 45);
  });

  await t.test("POST /api/plugin-analytics/ingest preserves creditsRemaining", async () => {
    const ingestRes = await request(app)
      .post("/api/plugin-analytics/ingest")
      .send({
        source: "test-suite",
        sessionId: "session-credit-ingest",
        deviceId: "device-credit-ingest",
        sentAt: now.toISOString(),
        events: [
          {
            eventType: "credit_consumed",
            eventAt: now.toISOString(),
            tool: "unit-converter",
            payload: { amount: 3, featureCode: "preset_customizable" },
            user: {
              isAuthenticated: true,
              userId: "user-ingest",
              name: "Ingest User",
              email: "ingest@example.com",
              creditsRemaining: 17,
            },
          },
        ],
      });

    assert.strictEqual(ingestRes.status, 202);

    const creditRes = await request(app)
      .get("/api/plugin-analytics/credit-consumption")
      .set("Authorization", authString);

    const ingestedUser = creditRes.body.users.find((user) => user._id === "user-ingest");
    assert.ok(ingestedUser);
    assert.strictEqual(ingestedUser.creditsRemaining, 17);
    assert.strictEqual(ingestedUser.totalConsumed, 4);
  });

  await t.test("GET /api/plugin-analytics/user-top-tools", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/user-top-tools")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.users.length, 1);
    assert.strictEqual(res.body.users[0]._id, "user1");
    assert.strictEqual(res.body.users[0].topTools[0].tool, "import-tool");
    assert.strictEqual(res.body.users[0].topTools[0].timeSpentMs, 10000);
  });

  await t.test("GET /api/plugin-analytics/credit-intelligence", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/credit-intelligence")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.summary);
    assert.ok(Array.isArray(res.body.users));
    assert.ok(Array.isArray(res.body.toolBreakdown));
    assert.ok(res.body.newsletter);
    assert.strictEqual(res.body.summary.trackedUsers, 3);
    assert.strictEqual(res.body.summary.lowCreditUsers, 2);
    assert.strictEqual(res.body.summary.creditsConsumedTotal, 13);
    assert.ok(
      res.body.users.some(
        (user) =>
          user._id === "backend-only-user" &&
          user.email === "backend@example.com" &&
          user.creditsRemaining === 82 &&
          user.riskLevel === "healthy"
      )
    );
    assert.ok(res.body.newsletter.subject.includes("Waysorted credit digest"));
  });

  await t.test("GET /api/plugin-analytics/newsletter/runout-preview", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/newsletter/runout-preview")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(typeof res.body.subject === "string");
    assert.ok(typeof res.body.text === "string");
    assert.ok(typeof res.body.html === "string");
  });

  await t.test("GET /api/plugin-analytics/stats", async () => {
    const res = await request(app).get("/api/plugin-analytics/stats");
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.likes, 1);
    assert.strictEqual(res.body.saves, 1);
    assert.strictEqual(res.body.follows, 1);
  });

  await t.test("POST /api/plugin-analytics/stats/like", async () => {
    const res = await request(app).post("/api/plugin-analytics/stats/like");
    assert.strictEqual(res.status, 200);
    
    const getRes = await request(app).get("/api/plugin-analytics/stats");
    assert.strictEqual(getRes.body.likes, 1);
  });

  await t.test("GET /api/plugin-analytics/stats/history", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/stats/history?range=7d")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.length >= 2);
    const yesterdayStr = yesterday.toISOString().slice(0, 10);
    const todayStr = now.toISOString().slice(0, 10);
    assert.strictEqual(res.body[0]._id, yesterdayStr);
    assert.strictEqual(res.body[1]._id, todayStr);
    assert.strictEqual(res.body[1].mau, 3);
    assert.strictEqual(res.body[1].authenticatedUsers, 2);
    assert.strictEqual(res.body[1].likes, 1);
    assert.strictEqual(res.body[1].saves, 1);
    assert.strictEqual(res.body[1].follows, 1);
    assert.strictEqual(res.body[1].reuses, 2);
  });

  await t.test("GET /api/plugin-analytics/stats/deltas", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/stats/deltas")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.today);
    assert.ok(res.body.yesterday);
    assert.strictEqual(res.body.today.mau, 3);
    assert.strictEqual(res.body.yesterday.mau, 2);
    assert.strictEqual(res.body.changes.daily.mau, 1);
    assert.strictEqual(res.body.changes.daily.likes, 1);
  });

  await t.test("GET /api/plugin-analytics/credit-consumption/by-tool", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/credit-consumption/by-tool")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.tools);
    assert.ok(res.body.tools.length >= 2);
    const frameGallery = res.body.tools.find((tool) => tool.tool === "frame-gallery");
    const unitConverter = res.body.tools.find((tool) => tool.tool === "unit-converter");
    assert.ok(frameGallery);
    assert.ok(unitConverter);
    assert.strictEqual(frameGallery.totalAmount, 3);
    assert.strictEqual(unitConverter.totalAmount, 4);
  });

  await t.test("GET /api/plugin-analytics/retention", async () => {
    // Add retention events
    const firstDate = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000); // 15 days ago
    const day1Date = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000); // 14 days ago (Day 1)
    
    await eventsColl.insertMany([
      {
        eventType: "session_start",
        eventAt: firstDate,
        sessionId: "session-2",
        user: {
          isAuthenticated: true,
          userId: "user2",
          name: "Bob"
        }
      },
      {
        eventType: "interaction",
        eventAt: day1Date,
        sessionId: "session-3",
        user: {
          isAuthenticated: true,
          userId: "user2",
          name: "Bob"
        }
      }
    ]);

    const res = await request(app)
      .get("/api/plugin-analytics/retention")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.cohorts);
    const firstDateStr = firstDate.toISOString().slice(0, 10);
    const matchingCohort = res.body.cohorts.find(c => c.firstSeenDate === firstDateStr);
    assert.ok(matchingCohort);
    assert.strictEqual(matchingCohort.totalUsers, 1);
    assert.strictEqual(matchingCohort.day1, 1.0);
  });

  // Cleanup
  const snapshotsColl = db.collection("stats_snapshots");
  await eventsColl.deleteMany({});
  await engageColl.deleteMany({});
  await usersColl.deleteMany({});
  await userBillingColl.deleteMany({});
  await creditLedgerColl.deleteMany({});
  await snapshotsColl.deleteMany({});
  await closeDatabases();
  await mongod.stop();
});
