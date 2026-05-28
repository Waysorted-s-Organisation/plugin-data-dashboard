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

  // Cleanup
  await eventsColl.deleteMany({});
  await engageColl.deleteMany({});
  await mongod.stop();
});
