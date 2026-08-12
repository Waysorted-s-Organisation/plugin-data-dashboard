import assert from "node:assert/strict";
import test from "node:test";

import { MongoClient, ObjectId } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");

/**
 * Every served operations endpoint must return 200 with data present.
 *
 * A reference to a variable that exists in one function but not another —
 * exactly the kind of thing a copy-paste edit introduces — throws only at
 * request time, and the sidebar renders it as a red "Data unavailable" with no
 * indication of which endpoint died. Nothing else in this suite calls
 * data-health, commercial or feedback, so that break shipped unnoticed.
 */
test("every operations endpoint responds with data", async (t) => {
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

  const now = new Date();
  const user = new ObjectId();
  await client.db("waysorted").collection("users").insertOne({
    _id: user, email: "smoke@example.com", createdAt: now,
  });
  await client.db("waysorted").collection("sessions").insertOne({
    user, source: "figma", completed: true, completedAt: now, createdAt: now,
  });
  // A purchase old enough to be a stalled checkout, which is what tripped the
  // data-health endpoint when its guard was pasted into the wrong function.
  await client.db("waysorted").collection("purchases").insertOne({
    user, productCode: "sub_month_1", kind: "subscription", status: "pending",
    amountPaise: 14900, currency: "INR", creditsGranted: 150,
    createdAt: new Date(now.getTime() - 3 * 60 * 60 * 1000), updatedAt: now,
  });
  await client.db("analytics").collection("plugin_analytics_events").insertOne({
    eventId: "smoke1", schemaVersion: 2, isSemantic: true, eventType: "tool_opened",
    sessionId: "s1", deviceId: "d1", source: "main", tool: "comment-summarizer",
    eventAt: now, receivedAt: now, payload: {},
    user: { isAuthenticated: true, userId: String(user), email: null, anonymousId: null },
  });

  const endpoints = [
    "/api/operations/data-health",
    "/api/operations/summary?days=30",
    "/api/operations/users?days=30",
    "/api/operations/tools?days=30",
    "/api/operations/lifecycle?days=90",
    "/api/operations/commercial?days=30",
    "/api/operations/feedback?days=90",
  ];

  for (const path of endpoints) {
    const response = await request(app).get(path).set("Authorization", basicAuth);
    assert.equal(response.status, 200, `${path} returned ${response.status}`);
    assert.ok(response.body && typeof response.body === "object", `${path} returned no body`);
    assert.ok(!response.body.error, `${path} reported: ${response.body.error}`);
  }

  // The sidebar reads this shape to decide between "live" and the red
  // "Data unavailable" dot, so its absence is what the user actually sees.
  const health = await request(app)
    .get("/api/operations/data-health")
    .set("Authorization", basicAuth)
    .expect(200);
  assert.equal(health.body.components?.backendDatabase?.status, "healthy");
});
