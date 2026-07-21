import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";

import request from "supertest";

import app from "../src/server.js";


const basicAuth = "Basic " + Buffer.from("test:test").toString("base64");


test("newsletter proxy is authenticated, allowlisted, and server-side", async (t) => {
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  process.env.NEWSLETTER_MANAGEMENT_TOKEN = "private-newsletter-token";

  const received = [];
  const upstream = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      received.push({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization,
        body: body ? JSON.parse(body) : null,
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, upstream: req.url }));
    });
  });
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  t.after(() => upstream.close());
  const address = upstream.address();
  process.env.NEWSLETTER_API_URL = `http://127.0.0.1:${address.port}`;

  const unauthorized = await request(app).get("/api/newsletter/overview");
  assert.equal(unauthorized.status, 401);

  const overview = await request(app)
    .get("/api/newsletter/overview?range=30")
    .set("Authorization", basicAuth);
  assert.equal(overview.status, 200);
  assert.equal(overview.body.success, true);
  assert.equal(
    received[0].url,
    "/api/management/overview?range=30"
  );
  assert.equal(
    received[0].authorization,
    "Bearer private-newsletter-token"
  );

  const create = await request(app)
    .post("/api/newsletter/campaigns")
    .set("Authorization", basicAuth)
    .send({ name: "Unified Campaign" });
  assert.equal(create.status, 200);
  assert.deepEqual(received[1].body, { name: "Unified Campaign" });

  const blocked = await request(app)
    .post("/api/newsletter/campaigns")
    .set("Authorization", basicAuth)
    .set("Origin", "https://attacker.example")
    .send({ name: "Blocked" });
  assert.equal(blocked.status, 403);
  assert.equal(received.length, 2);

  const automations = await request(app)
    .get("/api/newsletter/automations/n1_onboarding_activation")
    .set("Authorization", basicAuth);
  const templates = await request(app)
    .get("/api/newsletter/content/templates?kind=all")
    .set("Authorization", basicAuth);
  const version = await request(app)
    .put("/api/newsletter/templates/7")
    .set("Authorization", basicAuth)
    .send({ name: "Version 2" });
  assert.equal(automations.status, 200);
  assert.equal(templates.status, 200);
  assert.equal(version.status, 200);
  assert.equal(received[4].method, "PUT");
  assert.deepEqual(received[4].body, { name: "Version 2" });

  const unsupported = await request(app)
    .get("/api/newsletter/internal/secrets")
    .set("Authorization", basicAuth);
  assert.equal(unsupported.status, 404);
  assert.equal(received.length, 5);
});


test("newsletter proxy fails closed when integration is not configured", async () => {
  process.env.DASHBOARD_BASIC_AUTH_USER = "test";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "test";
  delete process.env.NEWSLETTER_API_URL;
  delete process.env.NEWSLETTER_MANAGEMENT_TOKEN;

  const response = await request(app)
    .get("/api/newsletter/overview")
    .set("Authorization", basicAuth);

  assert.equal(response.status, 503);
  assert.equal(
    response.body.error,
    "Newsletter integration is not configured"
  );
});
