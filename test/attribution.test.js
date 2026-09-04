import assert from "node:assert/strict";
import test from "node:test";

import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");
const emailAuth = "Basic " + Buffer.from("anshbhatt140@gmail.com:test").toString("base64");

test("owner attribution campaign management", async (t) => {
  const previousEnvironment = Object.fromEntries(
    [
      "MONGODB_URI",
      "MONGODB_DB",
      "DASHBOARD_BASIC_AUTH_USER",
      "DASHBOARD_BASIC_AUTH_PASS",
      "DASHBOARD_ADMIN_EMAILS",
      "WAYSORTED_PUBLIC_URL",
    ].map((name) => [name, process.env[name]])
  );
  const mongod = await MongoMemoryServer.create();
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  process.env.DASHBOARD_ADMIN_EMAILS = "anshbhatt140@gmail.com";
  process.env.WAYSORTED_PUBLIC_URL = "https://www.waysorted.com";
  t.after(async () => {
    await closeDb();
    await mongod.stop();
    for (const [name, value] of Object.entries(previousEnvironment)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  await t.test("requires dashboard authentication", async () => {
    const response = await request(app).get("/api/operations/attribution/campaigns");
    assert.equal(response.status, 401);
  });

  await t.test("accepts the configured admin email", async () => {
    const response = await request(app)
      .get("/api/operations/attribution/campaigns")
      .set("Authorization", emailAuth);
    assert.equal(response.status, 200);
  });

  await t.test("creates and lists a Madhura checkout campaign", async () => {
    const create = await request(app).post("/api/operations/attribution/campaigns").set("Authorization", basicAuth).send({ name: "Madhura", utmSource: "madhura", utmMedium: "referral", utmCampaign: "checkout", destinationPath: "/payment" });
    assert.equal(create.status, 201);
    assert.equal(create.body.campaign.checkoutUrl, "https://www.waysorted.com/payment?utm_source=madhura&utm_medium=referral&utm_campaign=checkout");
    const list = await request(app).get("/api/operations/attribution/campaigns").set("Authorization", basicAuth);
    assert.equal(list.status, 200);
    assert.equal(list.body.items.length, 1);
    assert.equal(list.body.items[0].utmSource, "madhura");
  });

  await t.test("rejects duplicates and external destinations", async () => {
    const duplicate = await request(app).post("/api/operations/attribution/campaigns").set("Authorization", basicAuth).send({ name: "Madhura again", utmSource: "madhura", utmCampaign: "checkout" });
    assert.equal(duplicate.status, 409);
    const external = await request(app).post("/api/operations/attribution/campaigns").set("Authorization", basicAuth).send({ name: "Unsafe", utmSource: "unsafe", destinationPath: "https://example.com/payment" });
    assert.equal(external.status, 400);
  });

  await t.test("blocks cross-origin campaign creation", async () => {
    const response = await request(app).post("/api/operations/attribution/campaigns").set("Authorization", basicAuth).set("Origin", "https://evil.example").send({ name: "Blocked", utmSource: "blocked" });
    assert.equal(response.status, 403);
  });
});
