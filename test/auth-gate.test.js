import assert from "node:assert/strict";
import test from "node:test";

import { MongoMemoryServer } from "mongodb-memory-server";
import request from "supertest";

import app from "../src/server.js";
import { closeDb } from "../src/db.js";

/**
 * The gate used to call next() when either credential was empty, and
 * `.env.example` ships both empty — so a deployment following the README's
 * copy-the-example setup served the customer database and a proxy holding a
 * privileged mass-send token to anyone with the URL, silently. Every existing
 * test set the credentials first, so that branch had no coverage at all and the
 * suite was green on a wide-open dashboard.
 *
 * These run it. They also pin the two things the fix must not break: the
 * plugin's own endpoints and the public health check, which are registered
 * ahead of the gate precisely so a refusal here cannot interrupt telemetry.
 */
async function harness(t) {
  const mongod = await MongoMemoryServer.create();
  const previousUser = process.env.DASHBOARD_BASIC_AUTH_USER;
  const previousPass = process.env.DASHBOARD_BASIC_AUTH_PASS;
  const previousAllow = process.env.ALLOW_UNAUTHENTICATED;
  t.after(async () => {
    await closeDb();
    await mongod.stop();
    // Restored so clearing them here cannot leak into another test file.
    process.env.DASHBOARD_BASIC_AUTH_USER = previousUser ?? "";
    process.env.DASHBOARD_BASIC_AUTH_PASS = previousPass ?? "";
    if (previousAllow === undefined) delete process.env.ALLOW_UNAUTHENTICATED;
    else process.env.ALLOW_UNAUTHENTICATED = previousAllow;
  });
  process.env.MONGODB_URI = mongod.getUri("analytics");
  process.env.MONGODB_DB = "analytics";
  process.env.BACKEND_MONGODB_URI = mongod.getUri("waysorted");
  process.env.BACKEND_MONGODB_DB = "waysorted";
  process.env.ANALYTICS_SIGNING_SECRET = "test-secret";
}

const GATED = [
  "/api/operations/summary",
  "/api/operations/users",
  "/api/operations/credits/users",
  "/api/operations/tools",
  "/api/operations/feedback",
  "/api/operations/data-health",
  "/api/operations/health",
  "/api/newsletter/overview",
  "/summary.html",
  "/users.html",
  "/",
];

test("with no credentials configured, nothing behind the gate is served", async (t) => {
  await harness(t);
  process.env.DASHBOARD_BASIC_AUTH_USER = "";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "";
  delete process.env.ALLOW_UNAUTHENTICATED;

  for (const path of GATED) {
    const response = await request(app).get(path);
    assert.equal(response.status, 503, `${path} must refuse, not serve`);
    assert.equal(response.body.error, "Dashboard authentication is not configured");
  }
});

test("a half-configured gate is an unconfigured gate", async (t) => {
  await harness(t);
  delete process.env.ALLOW_UNAUTHENTICATED;

  for (const [user, pass] of [["someone", ""], ["", "secret"], ["   ", "secret"]]) {
    process.env.DASHBOARD_BASIC_AUTH_USER = user;
    process.env.DASHBOARD_BASIC_AUTH_PASS = pass;
    const response = await request(app).get("/api/operations/users");
    assert.equal(response.status, 503, `user=${JSON.stringify(user)} pass=${JSON.stringify(pass)}`);
  }
});

test("refusing to serve the dashboard never interrupts the plugin", async (t) => {
  await harness(t);
  process.env.DASHBOARD_BASIC_AUTH_USER = "";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "";
  delete process.env.ALLOW_UNAUTHENTICATED;

  // The public health check stays public: it is what deployment platforms
  // probe, and a 503 here would report a healthy deployment as broken.
  const health = await request(app).get("/health").expect(200);
  assert.equal(health.body.ok, true);

  // Telemetry keeps flowing. These routes are registered ahead of the gate.
  const ingest = await request(app)
    .post("/api/plugin-analytics/ingest")
    .send({
      source: "main",
      sessionId: "s-1",
      deviceId: "d-1",
      events: [{ eventId: "e1", eventType: "tool_action_completed", tool: "palettable", eventAt: new Date().toISOString() }],
    });
  assert.equal(ingest.status, 202, "the plugin must still be able to send events");
  assert.equal(ingest.body.stored, 1);

  // The session exchange answers on its own terms. It reports its own
  // configuration here — WAYSORTED_API_URL is unset in this harness — and the
  // point is that the answer never comes from the dashboard gate.
  const session = await request(app).post("/api/plugin-analytics/session").send({});
  assert.notEqual(
    session.body.error,
    "Dashboard authentication is not configured",
    "the plugin's sign-in exchange is not behind the dashboard gate"
  );
});

test("the local escape hatch opens the gate and says so", async (t) => {
  await harness(t);
  process.env.DASHBOARD_BASIC_AUTH_USER = "";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "";
  process.env.ALLOW_UNAUTHENTICATED = "true";

  const response = await request(app).get("/api/operations/users?days=30");
  assert.equal(response.status, 200, "local development still works without credentials");

  // Only the exact string opts in. A stray "1" or "yes" must not open the door.
  for (const value of ["1", "yes", "TRUE ", "false"]) {
    process.env.ALLOW_UNAUTHENTICATED = value;
    const guarded = await request(app).get("/api/operations/users");
    const expected = value.trim().toLowerCase() === "true" ? 200 : 503;
    assert.equal(guarded.status, expected, `ALLOW_UNAUTHENTICATED=${JSON.stringify(value)}`);
  }
});

test("with credentials configured, the gate behaves exactly as before", async (t) => {
  await harness(t);
  process.env.DASHBOARD_BASIC_AUTH_USER = "operator";
  process.env.DASHBOARD_BASIC_AUTH_PASS = "correct-horse";
  delete process.env.ALLOW_UNAUTHENTICATED;

  const anonymous = await request(app).get("/api/operations/users");
  assert.equal(anonymous.status, 401);
  assert.equal(
    anonymous.headers["www-authenticate"],
    'Basic realm="Waysorted Operations"',
    "the realm must not change: browsers cache Basic credentials per realm, so renaming it signs every operator out"
  );

  const wrong = await request(app)
    .get("/api/operations/users")
    .set("Authorization", "Basic " + Buffer.from("operator:wrong").toString("base64"));
  assert.equal(wrong.status, 403);

  const wrongUser = await request(app)
    .get("/api/operations/users")
    .set("Authorization", "Basic " + Buffer.from("intruder:correct-horse").toString("base64"));
  assert.equal(wrongUser.status, 403);

  // A password containing a colon survives the split.
  process.env.DASHBOARD_BASIC_AUTH_PASS = "a:b:c";
  const colon = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", "Basic " + Buffer.from("operator:a:b:c").toString("base64"));
  assert.equal(colon.status, 200);

  process.env.DASHBOARD_BASIC_AUTH_PASS = "correct-horse";
  const right = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", "Basic " + Buffer.from("operator:correct-horse").toString("base64"));
  assert.equal(right.status, 200);
});
