import test from "node:test";
import assert from "node:assert";
import request from "supertest";
import app from "../src/server.js";
import { MongoMemoryServer } from "mongodb-memory-server";
import { MongoClient } from "mongodb";
import { getDb } from "../src/db.js";

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

  // Setup mock data
  await eventsColl.deleteMany({});
  await engageColl.deleteMany({});

  const now = new Date();
  await eventsColl.insertMany([
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
    assert.strictEqual(res.body.mau, 1);
    assert.strictEqual(res.body.authenticated, 1);
  });

  await t.test("GET /api/plugin-analytics/credit-consumption", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/credit-consumption")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.users.length, 1);
    assert.strictEqual(res.body.users[0]._id, "user1");
    assert.strictEqual(res.body.users[0].totalConsumed, 1);
    assert.strictEqual(res.body.users[0].creditsRemaining, 45);
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

  await t.test("GET /api/plugin-analytics/stats", async () => {
    const res = await request(app).get("/api/plugin-analytics/stats");
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.likes, 0);
  });

  await t.test("POST /api/plugin-analytics/stats/like", async () => {
    const res = await request(app).post("/api/plugin-analytics/stats/like");
    assert.strictEqual(res.status, 200);
    
    const getRes = await request(app).get("/api/plugin-analytics/stats");
    assert.strictEqual(getRes.body.likes, 1);
  });

  await t.test("GET /api/plugin-analytics/stats/history", async () => {
    const snapshotsColl = db.collection("stats_snapshots");
    await snapshotsColl.deleteMany({});

    const todayStr = new Date().toISOString().slice(0, 10);
    const yesterdayStr = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    
    await snapshotsColl.insertMany([
      {
        _id: todayStr,
        date: new Date(todayStr),
        mau: 10,
        authenticatedUsers: 5,
        likes: 12,
        saves: 8,
        follows: 4,
        reuses: 20,
        creditsConsumed: 50,
        activeSessions: 3,
        recordedAt: new Date()
      },
      {
        _id: yesterdayStr,
        date: new Date(yesterdayStr),
        mau: 8,
        authenticatedUsers: 4,
        likes: 10,
        saves: 7,
        follows: 3,
        reuses: 18,
        creditsConsumed: 45,
        activeSessions: 2,
        recordedAt: new Date()
      }
    ]);

    const res = await request(app)
      .get("/api/plugin-analytics/stats/history?range=7d")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.length, 2);
    assert.strictEqual(res.body[0]._id, yesterdayStr);
    assert.strictEqual(res.body[1]._id, todayStr);
  });

  await t.test("GET /api/plugin-analytics/stats/deltas", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/stats/deltas")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.today);
    assert.ok(res.body.yesterday);
    assert.strictEqual(res.body.today.mau, 10);
    assert.strictEqual(res.body.yesterday.mau, 8);
    assert.strictEqual(res.body.changes.daily.mau, 2); // 10 - 8
    assert.strictEqual(res.body.changes.daily.likes, 2); // 12 - 10
  });

  await t.test("GET /api/plugin-analytics/credit-consumption/by-tool", async () => {
    const res = await request(app)
      .get("/api/plugin-analytics/credit-consumption/by-tool")
      .set("Authorization", authString);

    assert.strictEqual(res.status, 200);
    assert.ok(res.body.tools);
    assert.strictEqual(res.body.tools.length, 1);
    assert.strictEqual(res.body.tools[0].tool, "import-tool");
    assert.strictEqual(res.body.tools[0].count, 1);
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
  await snapshotsColl.deleteMany({});
  await mongod.stop();
});
