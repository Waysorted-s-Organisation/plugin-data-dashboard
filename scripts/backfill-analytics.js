#!/usr/bin/env node
/**
 * Analytics backfill: inspect first, write only what is provably derivable.
 *
 * Default mode is a read-only report. Nothing is written unless --apply is
 * passed, and even then the only write is the additive `isSemantic` flag on
 * documents that predate it. No existing field is ever overwritten and no value
 * is invented — anything that cannot be derived from what is already stored is
 * reported as unrecoverable rather than guessed.
 *
 *   node scripts/backfill-analytics.js              # report only
 *   node scripts/backfill-analytics.js --apply      # also set isSemantic where missing
 *
 * Identity and tool re-attribution are deliberately NOT written. Both are
 * resolved at read time in product-intelligence.js, which leaves the original
 * documents untouched and keeps the correction reversible.
 */
import dotenv from "dotenv";

import { closeDb, getEventsCollection } from "../src/db.js";
import { SEMANTIC_EVENT_TYPES } from "../src/product-intelligence.js";

dotenv.config();

const APPLY = process.argv.includes("--apply");
const SEMANTIC = new Set(SEMANTIC_EVENT_TYPES);
const looksLikeEmail = (value) => typeof value === "string" && value.includes("@");
const looksLikeObjectId = (value) => typeof value === "string" && /^[a-f0-9]{24}$/i.test(value);

function line(label, value) {
  console.log(`  ${String(label).padEnd(42)} ${value}`);
}

async function main() {
  const events = await getEventsCollection();

  const total = await events.countDocuments({});
  if (!total) {
    console.log("\nNo analytics events are stored. Nothing to inspect or backfill.\n");
    return;
  }

  const [oldest, newest] = await Promise.all([
    events.findOne({}, { sort: { eventAt: 1 }, projection: { eventAt: 1 } }),
    events.findOne({}, { sort: { eventAt: -1 }, projection: { eventAt: 1 } }),
  ]);

  console.log("\n=== Stored analytics events ===");
  line("total events", total);
  line("oldest event", oldest?.eventAt?.toISOString?.() || "unknown");
  line("newest event", newest?.eventAt?.toISOString?.() || "unknown");

  // --- Event type coverage -------------------------------------------------
  const byType = await events
    .aggregate([{ $group: { _id: "$eventType", count: { $sum: 1 } } }, { $sort: { count: -1 } }])
    .toArray();
  console.log("\n=== Event types present ===");
  for (const row of byType) {
    const kind = SEMANTIC.has(row._id) ? "semantic" : "non-semantic";
    line(`${row._id} (${kind})`, row.count);
  }

  const missingFlag = await events.countDocuments({ isSemantic: { $exists: false } });
  console.log("\n=== Backfillable: isSemantic ===");
  line("documents missing the flag", missingFlag);
  line("derivable from eventType", missingFlag ? "yes — purely additive, no data replaced" : "n/a");

  // --- Tool attribution ----------------------------------------------------
  const byTool = await events
    .aggregate([{ $group: { _id: "$tool", count: { $sum: 1 } } }, { $sort: { count: -1 } }])
    .toArray();
  const unknownTool = byTool.find((row) => row._id === "unknown")?.count || 0;
  console.log("\n=== Tool attribution ===");
  for (const row of byTool) line(row._id ?? "(null)", row.count);

  // Historical events misattributed by the old "import" substring rule can be
  // recognised by the action recorded in their own payload. Reported only —
  // the read path resolves this without touching stored documents.
  const recoverableHtmlToDesign = await events.countDocuments({
    tool: "import-tool",
    "payload.action": { $regex: "^import-html-design-" },
  });
  console.log("\n=== Backfillable: tool re-attribution ===");
  line("events with tool 'unknown'", unknownTool);
  line("html-to-design events filed as import-tool", recoverableHtmlToDesign);
  line(
    "recoverable from payload.action",
    recoverableHtmlToDesign ? "yes — resolved at read time, documents left intact" : "none found"
  );

  // --- Identity ------------------------------------------------------------
  const identitySample = await events
    .aggregate([
      {
        $group: {
          _id: null,
          withUserId: { $sum: { $cond: [{ $ifNull: ["$user.userId", false] }, 1, 0] } },
          withEmail: { $sum: { $cond: [{ $ifNull: ["$user.email", false] }, 1, 0] } },
          withAnonymous: { $sum: { $cond: [{ $ifNull: ["$user.anonymousId", false] }, 1, 0] } },
          noIdentity: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $not: [{ $ifNull: ["$user.userId", false] }] },
                    { $not: [{ $ifNull: ["$user.email", false] }] },
                    { $not: [{ $ifNull: ["$user.anonymousId", false] }] },
                  ],
                },
                1,
                0,
              ],
            },
          },
        },
      },
    ])
    .toArray();

  const distinctUserIds = await events.distinct("user.userId", { "user.userId": { $ne: null } });
  const emailShaped = distinctUserIds.filter(looksLikeEmail).length;
  const objectIdShaped = distinctUserIds.filter(looksLikeObjectId).length;
  const otherShaped = distinctUserIds.length - emailShaped - objectIdShaped;

  const idStats = identitySample[0] || {};
  console.log("\n=== Identity ===");
  line("events carrying a userId", idStats.withUserId || 0);
  line("events carrying an email", idStats.withEmail || 0);
  line("events carrying only an anonymousId", idStats.withAnonymous || 0);
  line("events with no identity at all", idStats.noIdentity || 0);
  line("distinct userId values", distinctUserIds.length);
  line("  ...shaped like an ObjectId", objectIdShaped);
  line("  ...shaped like an email (legacy)", emailShaped);
  line("  ...neither", otherShaped);
  line(
    "email-shaped ids",
    emailShaped ? "resolved to accounts at read time; documents left intact" : "none — identity is clean"
  );

  // --- Sessions ------------------------------------------------------------
  const sessions = await events.distinct("sessionId");
  const endedSessions = await events.distinct("sessionId", { eventType: "plugin_session_ended" });
  console.log("\n=== Session completeness ===");
  line("distinct sessions", sessions.length);
  line("sessions with a recorded end", endedSessions.length);
  line(
    "sessions with no end",
    `${sessions.length - endedSessions.length} — not recoverable, the close event was never sent`
  );

  // --- Write ---------------------------------------------------------------
  console.log("\n=== Action ===");
  if (!missingFlag) {
    console.log("  Nothing to write: every document already carries isSemantic.\n");
    return;
  }
  if (!APPLY) {
    console.log(`  Dry run. Re-run with --apply to set isSemantic on ${missingFlag} document(s).`);
    console.log("  No other field would be written or modified.\n");
    return;
  }

  const semanticResult = await events.updateMany(
    { isSemantic: { $exists: false }, eventType: { $in: [...SEMANTIC] } },
    { $set: { isSemantic: true } }
  );
  const nonSemanticResult = await events.updateMany(
    { isSemantic: { $exists: false }, eventType: { $nin: [...SEMANTIC] } },
    { $set: { isSemantic: false } }
  );
  console.log(`  isSemantic=true  set on ${semanticResult.modifiedCount} document(s)`);
  console.log(`  isSemantic=false set on ${nonSemanticResult.modifiedCount} document(s)`);
  console.log("  No existing field was overwritten.\n");
}

main()
  .catch((error) => {
    console.error("Backfill inspection failed:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDb();
  });
