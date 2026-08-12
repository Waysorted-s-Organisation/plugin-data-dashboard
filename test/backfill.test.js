import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";

import { MongoClient } from "mongodb";
import { MongoMemoryServer } from "mongodb-memory-server";

const run = promisify(execFile);

/**
 * The backfill script is run as a real subprocess against a real database so
 * the test covers what an operator would actually execute, including the
 * dry-run default that must never write.
 */
async function runBackfill(uri, args = []) {
  const { stdout } = await run("node", ["scripts/backfill-analytics.js", ...args], {
    env: { ...process.env, MONGODB_URI: uri, MONGODB_DB: "analytics" },
  });
  return stdout;
}

test("backfill inspects, reports, and writes only when told to", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await mongod.stop(); });
  const uri = mongod.getUri("analytics");

  const client = new MongoClient(uri);
  await client.connect();
  t.after(() => client.close());
  const events = client.db("analytics").collection("plugin_analytics_events");

  const base = {
    sessionId: "s1", deviceId: "d1", source: "main", tool: "palettable",
    eventAt: new Date("2026-08-01T10:00:00Z"), payload: {},
    user: { isAuthenticated: false, userId: null, anonymousId: "a1", email: null },
  };
  await events.insertMany([
    // Pre-existing documents with no isSemantic field.
    { ...base, eventId: "b1", eventType: "tool_opened" },
    { ...base, eventId: "b2", eventType: "backend_operation" },
    // A legacy identity and a misattributed html-to-design event.
    { ...base, eventId: "b3", eventType: "feature_used",
      user: { isAuthenticated: true, userId: "person@example.com", anonymousId: null, email: null } },
    { ...base, eventId: "b4", eventType: "feature_used", tool: "import-tool",
      payload: { action: "import-html-design-svg" } },
    // Already flagged; must be left alone.
    { ...base, eventId: "b5", eventType: "tool_closed", isSemantic: true },
  ]);

  const report = await runBackfill(uri);
  assert.match(report, /total events\s+5/, "reports the true total");
  assert.match(report, /documents missing the flag\s+4/);
  assert.match(report, /shaped like an email \(legacy\)\s+1/, "detects the legacy identity");
  assert.match(report, /html-to-design events filed as import-tool\s+1/, "detects misattribution");
  assert.match(report, /Dry run/, "defaults to reporting only");

  const untouched = await events.countDocuments({ isSemantic: { $exists: false } });
  assert.equal(untouched, 4, "a dry run writes nothing");

  const applied = await runBackfill(uri, ["--apply"]);
  // tool_opened and both feature_used rows are curated types; only
  // backend_operation falls outside the semantic vocabulary.
  assert.match(applied, /isSemantic=true\s+set on 3 document/);
  assert.match(applied, /isSemantic=false set on 1 document/);

  assert.equal(await events.countDocuments({ isSemantic: { $exists: false } }), 0);
  assert.equal((await events.findOne({ eventId: "b1" })).isSemantic, true, "curated type flagged semantic");
  assert.equal((await events.findOne({ eventId: "b2" })).isSemantic, false, "backend_operation is non-semantic");

  // Nothing but the new field may change.
  const legacy = await events.findOne({ eventId: "b3" });
  assert.equal(legacy.user.userId, "person@example.com", "the legacy identity is preserved, not rewritten");
  const misattributed = await events.findOne({ eventId: "b4" });
  assert.equal(misattributed.tool, "import-tool", "the original tool value is preserved");
  assert.equal(misattributed.payload.action, "import-html-design-svg");

  const second = await runBackfill(uri, ["--apply"]);
  assert.match(second, /Nothing to write/, "re-running is a no-op");
});

test("backfill handles an empty collection without error", async (t) => {
  const mongod = await MongoMemoryServer.create();
  t.after(async () => { await mongod.stop(); });
  const stdout = await runBackfill(mongod.getUri("analytics"), ["--apply"]);
  assert.match(stdout, /No analytics events are stored/);
});
