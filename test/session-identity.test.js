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
 * The plugin cannot be used without logging in, yet the users table filled with
 * "signed-out visitor" rows doing real work in real tools. Two production facts
 * explain it, and these tests pin both fixes.
 *
 * First, events start flowing at plugin boot, before the stored token has been
 * validated, so the opening events of every launch carry no identity. Nothing
 * stitched them to the account, so each launch minted a phantom visitor.
 * identity_linked cannot help: it fires when a signed-out session signs in,
 * which under a hard login gate never happens.
 *
 * Second, the device id is not stable. Production ids look like
 * `device_<epoch-ms>_<suffix>` and the timestamp is the launch that made them,
 * so the same person on the same machine is a new "device" every time they open
 * the plugin.
 *
 * The session id survives both: it is on every event, scoped to one launch, and
 * whatever account appears anywhere in it owns all of it.
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

const event = (over) => ({
  schemaVersion: 2,
  isSemantic: true,
  source: "main",
  tool: "icon-library",
  eventType: "tool_action_completed",
  receivedAt: new Date(),
  payload: {},
  user: { isAuthenticated: false, userId: null, anonymousId: null, email: null },
  ...over,
});

const signedOut = { isAuthenticated: false, userId: null, anonymousId: null, email: null };
const signedIn = (user) => ({ isAuthenticated: true, userId: String(user), anonymousId: null, email: null });

test("the pre-authentication head of a launch belongs to whoever the launch belonged to", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const account = new ObjectId();
  await backend.collection("users").insertOne({
    _id: account, email: "gated@example.com", name: "Gated", createdAt: new Date(now.getTime() - 30 * DAY),
  });

  await analytics.collection("plugin_analytics_events").insertMany([
    // Boot. The token has not been validated yet, so no identity is attached.
    event({ eventId: "boot1", sessionId: "launch-1", deviceId: "device_1787730142122_8", eventType: "tool_opened", eventAt: new Date(now.getTime() - 5 * 60000) }),
    event({ eventId: "boot2", sessionId: "launch-1", deviceId: "device_1787730142122_8", eventAt: new Date(now.getTime() - 4 * 60000) }),
    // Auth resolves mid-launch. Same session, now identified.
    event({ eventId: "auth1", sessionId: "launch-1", deviceId: "device_1787730142122_8", eventAt: new Date(now.getTime() - 3 * 60000), user: signedIn(account) }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(
    users.body.items.filter((row) => row.anonymous).length,
    0,
    "a launch that authenticated leaves no signed-out visitor behind"
  );
  const row = users.body.items.find((item) => item.email === "gated@example.com");
  // boot1 is a tool_opened, which is not finished work. The other two are, and
  // one of them happened before the plugin knew who the user was.
  assert.equal(row.observedJobs, 2, "the pre-auth job counts for the account too");
  assert.equal(row.topTool.label, "Icon Library");
});

test("a device id minted fresh at each launch still resolves to one person", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const account = new ObjectId();
  await backend.collection("users").insertOne({
    _id: account, email: "daily@example.com", createdAt: new Date(now.getTime() - 30 * DAY),
  });

  // Two launches on two days. The plugin mints a different device id each time,
  // exactly as production does, so device-keyed attribution would see two
  // separate strangers and never register a return.
  await analytics.collection("plugin_analytics_events").insertMany([
    event({ eventId: "d1a", sessionId: "launch-a", deviceId: "device_1787715258581_s", eventAt: new Date(now.getTime() - 2 * DAY) }),
    event({ eventId: "d1b", sessionId: "launch-a", deviceId: "device_1787715258581_s", eventAt: new Date(now.getTime() - 2 * DAY + 60000), user: signedIn(account) }),
    event({ eventId: "d2a", sessionId: "launch-b", deviceId: "device_1787730142122_8", eventAt: new Date(now.getTime() - 1 * DAY) }),
    event({ eventId: "d2b", sessionId: "launch-b", deviceId: "device_1787730142122_8", eventAt: new Date(now.getTime() - 1 * DAY + 60000), user: signedIn(account) }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(users.body.items.filter((row) => row.anonymous).length, 0, "no phantom visitors per launch");
  const row = users.body.items.find((item) => item.email === "daily@example.com");
  assert.equal(row.observedJobs, 4, "both launches count toward the same person");
  assert.equal(row.pluginActiveDays, 2, "two launches on two days");
  assert.ok(row.segments.includes("returning"), "which is a return");
});

test("a launch that never authenticates stays a signed-out visitor", async (t) => {
  const { analytics } = await harness(t);
  const now = new Date();
  await analytics.collection("plugin_analytics_events").insertMany([
    event({ eventId: "n1", sessionId: "launch-never", deviceId: "device_1787726317271_j", eventAt: now, user: { ...signedOut, name: "Curious Visitor" } }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const visitor = users.body.items.find((row) => row.anonymous);
  assert.ok(visitor, "someone who opened the plugin and never signed in is still real");
  assert.equal(visitor.name, "Curious Visitor");
});

test("a launch containing two accounts is not attributed to either", async (t) => {
  const { backend, analytics } = await harness(t);
  const now = new Date();
  const first = new ObjectId();
  const second = new ObjectId();
  await backend.collection("users").insertMany([
    { _id: first, email: "first@example.com", createdAt: now },
    { _id: second, email: "second@example.com", createdAt: now },
  ]);

  await analytics.collection("plugin_analytics_events").insertMany([
    event({ eventId: "s1", sessionId: "shared", deviceId: "device_x", eventAt: now }),
    event({ eventId: "s2", sessionId: "shared", deviceId: "device_x", eventAt: now, user: signedIn(first) }),
    event({ eventId: "s3", sessionId: "shared", deviceId: "device_x", eventAt: now, user: signedIn(second) }),
  ]);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const firstRow = users.body.items.find((row) => row.email === "first@example.com");
  const secondRow = users.body.items.find((row) => row.email === "second@example.com");
  assert.equal(firstRow.observedJobs, 1, "each account keeps only its own identified event");
  assert.equal(secondRow.observedJobs, 1);
  assert.ok(
    users.body.items.some((row) => row.anonymous),
    "the ambiguous pre-auth event is left unattributed rather than guessed at"
  );
});

test("data health reports a device id that is regenerated on every launch", async (t) => {
  const { analytics } = await harness(t);
  const now = new Date();
  await analytics.collection("plugin_analytics_events").insertMany(
    [1, 2, 3, 4].map((n) => event({
      eventId: `h${n}`, sessionId: `session-${n}`, deviceId: `device_178773014212${n}_x`,
      eventAt: new Date(now.getTime() - n * 60000),
    }))
  );

  const health = await request(app)
    .get("/api/operations/data-health")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(health.body.components.identity.status, "per_launch");
  assert.equal(health.body.components.identity.devicesPerSession, 1);
  assert.match(health.body.components.identity.message, /figma\.clientStorage/);
});

test("an id the plugin sent is kept even when its auth flag says otherwise", async (t) => {
  const { backend } = await harness(t);
  const now = new Date();
  const account = new ObjectId();
  await backend.collection("users").insertOne({
    _id: account, email: "lagging@example.com", createdAt: new Date(now.getTime() - 10 * DAY),
  });

  // The plugin's auth state machine lags the token it already holds, so the
  // opening events of a launch arrive flagged signed-out while carrying a real
  // account id. Blanking the id on the strength of the flag is what turned a
  // logged-in person into a signed-out visitor inside their own session.
  await request(app)
    .post("/api/plugin-analytics/ingest")
    .send({
      source: "main",
      sessionId: "boot-session",
      deviceId: "device_1787730142122_9",
      events: [
        {
          eventId: "lag1",
          eventType: "tool_action_completed",
          tool: "palettable",
          eventAt: now.toISOString(),
          user: { isAuthenticated: false, userId: String(account), email: "lagging@example.com" },
        },
      ],
    })
    .expect(202);

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  assert.equal(users.body.items.filter((row) => row.anonymous).length, 0, "no phantom visitor");
  const row = users.body.items.find((item) => item.email === "lagging@example.com");
  assert.equal(row.observedJobs, 1, "the job lands on the real account");
});

test("a subscription granted from the backend makes someone a customer without a wallet", async (t) => {
  const { backend } = await harness(t);
  const now = new Date();
  const comped = new ObjectId();
  const plain = new ObjectId();
  await backend.collection("users").insertMany([
    { _id: comped, email: "comped@example.com", createdAt: new Date(now.getTime() - 10 * DAY) },
    { _id: plain, email: "plain@example.com", createdAt: new Date(now.getTime() - 10 * DAY) },
  ]);
  // Granted straight into the subscriptions collection for one month. No
  // userbillings document was created, which is exactly the case that used to
  // render identically to a user who had never opened a checkout.
  await backend.collection("subscriptions").insertOne({
    _id: new ObjectId(), user: comped, planCode: "pro_monthly", status: "active",
    currentPeriodStart: now, currentPeriodEnd: new Date(now.getTime() + 30 * DAY),
    createdAt: now, updatedAt: now,
  });

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);

  const row = users.body.items.find((item) => item.email === "comped@example.com");
  assert.equal(row.lifecycleStage, "customer", "the subscription record is commercial standing");
  assert.equal(row.subscriptionStatus, "active");
  assert.equal(row.subscriptionPlan, "pro_monthly");
  assert.equal(row.subscriptionSource, "subscription_record", "and it says the wallet is not the source");
  assert.equal(row.walletStatus, "missing", "the missing wallet is still reported, not hidden");

  const untouched = users.body.items.find((item) => item.email === "plain@example.com");
  assert.notEqual(untouched.lifecycleStage, "customer");
  assert.equal(untouched.subscriptionSource, null);
});

test("an expired backend subscription does not make someone a customer", async (t) => {
  const { backend } = await harness(t);
  const now = new Date();
  const lapsed = new ObjectId();
  await backend.collection("users").insertOne({
    _id: lapsed, email: "lapsed-sub@example.com", createdAt: new Date(now.getTime() - 90 * DAY),
  });
  await backend.collection("subscriptions").insertOne({
    _id: new ObjectId(), user: lapsed, planCode: "pro_monthly", status: "expired",
    currentPeriodStart: new Date(now.getTime() - 60 * DAY),
    currentPeriodEnd: new Date(now.getTime() - 30 * DAY),
    createdAt: now, updatedAt: now,
  });

  const users = await request(app)
    .get("/api/operations/users?days=30")
    .set("Authorization", basicAuth)
    .expect(200);
  const row = users.body.items.find((item) => item.email === "lapsed-sub@example.com");
  assert.notEqual(row.lifecycleStage, "customer", "a finished month is not an active plan");
});
