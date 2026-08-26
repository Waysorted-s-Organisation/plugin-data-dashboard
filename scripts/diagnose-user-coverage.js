#!/usr/bin/env node
/**
 * Explains the gap between users visible in the dashboard and identities
 * present in the database.
 *
 * Read-only. Nothing is written. Run it against production to find out exactly
 * why a person you can see in `sessions` does not appear on the users page.
 *
 *   node scripts/diagnose-user-coverage.js
 *
 * The dashboard builds its user list from the `users` collection, and counts a
 * login only when the session is completed AND linked to a user. So an identity
 * can be present in the database yet absent from the dashboard for several
 * distinct reasons, which this separates.
 */
import dotenv from "dotenv";

import {
  closeDb,
  getBackendCreditLedgerCollection,
  getBackendSessionsCollection,
  getBackendUsageReservationsCollection,
  getBackendUsersCollection,
  getBackendUserBillingCollection,
  getEventsCollection,
} from "../src/db.js";

dotenv.config();

const line = (label, value) => console.log(`  ${String(label).padEnd(52)} ${value}`);

async function main() {
  const [users, sessions, billings] = await Promise.all([
    (await getBackendUsersCollection()).find({}, { projection: { email: 1, createdAt: 1 } }).toArray(),
    (await getBackendSessionsCollection())
      .find({}, { projection: { user: 1, completed: 1, completedAt: 1, createdAt: 1, source: 1 } })
      .toArray(),
    (await getBackendUserBillingCollection()).find({}, { projection: { user: 1 } }).toArray(),
  ]);

  const userIds = new Set(users.map((row) => String(row._id)));

  console.log("\n=== Collections ===");
  line("users", users.length);
  line("sessions", sessions.length);
  line("userbillings (wallets)", billings.length);

  // --- Why a session may not produce a visible login ----------------------
  const noUser = sessions.filter((row) => !row.user);
  const notCompleted = sessions.filter((row) => row.user && row.completed !== true);
  const completed = sessions.filter((row) => row.user && row.completed === true);
  const noTimestamp = completed.filter((row) => !row.completedAt && !row.createdAt);

  console.log("\n=== Sessions the dashboard does not count as a login ===");
  line("no user linked (auth started, never finished)", noUser.length);
  line("linked but completed !== true", notCompleted.length);
  line("completed but no usable timestamp", noTimestamp.length);
  line("counted as a successful login", completed.length - noTimestamp.length);

  // --- Identities present in sessions but absent from users --------------
  const sessionUserIds = new Set(sessions.filter((row) => row.user).map((row) => String(row.user)));
  const orphaned = [...sessionUserIds].filter((idValue) => !userIds.has(idValue));

  console.log("\n=== Identity coverage ===");
  line("distinct users referenced by sessions", sessionUserIds.size);
  line("...that have a users document", sessionUserIds.size - orphaned.length);
  line("...ORPHANED (no users document)", orphaned.length);
  if (orphaned.length) {
    console.log("\n  Orphaned session user ids — these can never appear on the");
    console.log("  dashboard, because the users page is built from the users");
    console.log("  collection. Their sessions exist but the account does not:");
    for (const idValue of orphaned.slice(0, 20)) console.log(`    ${idValue}`);
    if (orphaned.length > 20) console.log(`    ...and ${orphaned.length - 20} more`);
  }

  const usersWithNoSession = users.filter((row) => !sessionUserIds.has(String(row._id)));
  line("users with no session at all", usersWithNoSession.length);
  const walletUserIds = new Set(billings.map((row) => String(row.user)));
  line("users with no wallet", users.filter((row) => !walletUserIds.has(String(row._id))).length);

  // --- Anonymous plugin visitors -----------------------------------------
  try {
    const events = await getEventsCollection();
    const anonIds = await events.distinct("user.anonymousId", { "user.anonymousId": { $ne: null } });
    const accountIds = await events.distinct("user.userId", { "user.userId": { $ne: null } });
    console.log("\n=== Plugin telemetry identities ===");
    line("distinct anonymous (signed-out) visitors", anonIds.length);
    line("distinct account ids in telemetry", accountIds.length);
    const unknownAccounts = accountIds.filter((idValue) => idValue && !idValue.includes("@") && !userIds.has(String(idValue)));
    line("...telemetry account ids with no users document", unknownAccounts.length);
  } catch (error) {
    console.log("\n=== Plugin telemetry identities ===");
    line("unavailable", error?.message || String(error));
  }

  // --- Why a person's job count reads zero -------------------------------
  //
  // The users table reports committed usage reservations first and falls back
  // to completed tool actions seen in telemetry. A zero therefore has several
  // distinct causes, and only one of them means "this person did nothing".
  console.log("\n=== Why job counts read zero ===");
  try {
    const [reservations, ledgerReasons] = await Promise.all([
      (await getBackendUsageReservationsCollection())
        .find({}, { projection: { user: 1, status: 1, toolCode: 1 } })
        .toArray(),
      (await getBackendCreditLedgerCollection())
        .aggregate([{ $group: { _id: "$reason", count: { $sum: 1 } } }, { $sort: { count: -1 } }])
        .toArray(),
    ]);

    const statuses = new Map();
    for (const row of reservations) {
      const status = row.status || "(missing)";
      statuses.set(status, (statuses.get(status) || 0) + 1);
    }
    console.log("\n  Reservation status distribution — only \"committed\" is counted as a job:");
    for (const [status, count] of [...statuses].sort((a, b) => b[1] - a[1])) line(`    ${status}`, count);

    const noReservationUser = reservations.filter((row) => !row.user).length;
    const orphanReservations = reservations.filter((row) => row.user && !userIds.has(String(row.user))).length;
    line("reservations with no user link", noReservationUser);
    line("reservations whose user has no users document", orphanReservations);

    const committedUsers = new Set(
      reservations.filter((row) => row.status === "committed" && row.user).map((row) => String(row.user))
    );
    line("users with at least one committed reservation", committedUsers.size);

    // A credit path that writes a ledger reason the dashboard does not
    // recognise is invisible to every credited metric. These are the reasons
    // actually present.
    console.log("\n  Credit ledger reasons present:");
    for (const row of ledgerReasons) line(`    ${row._id ?? "(null)"}`, row.count);

    // Telemetry-side evidence of finished work, keyed the way the dashboard
    // keys it.
    const events = await getEventsCollection();
    const completions = await events
      .aggregate([
        { $match: { eventType: { $in: ["tool_action_completed", "feature_used"] } } },
        {
          $group: {
            _id: { $ifNull: ["$user.userId", "$user.email"] },
            jobs: { $sum: 1 },
          },
        },
      ])
      .toArray();
    const telemetryAccounts = new Set(
      completions.filter((row) => row._id && userIds.has(String(row._id))).map((row) => String(row._id))
    );
    const telemetryUnmatched = completions.filter((row) => row._id && !userIds.has(String(row._id)));
    const telemetryAnonymous = completions.filter((row) => !row._id).reduce((sum, row) => sum + row.jobs, 0);

    console.log("\n  Completed tool jobs seen in telemetry:");
    line("identities with completed tool jobs", completions.length);
    line("...matching a users document", telemetryAccounts.size);
    line("...NOT matching any users document", telemetryUnmatched.length);
    line("jobs from signed-out / unkeyed identities", telemetryAnonymous);
    if (telemetryUnmatched.length) {
      console.log("\n  Telemetry identities the dashboard cannot join to an account.");
      console.log("  If these look like account ids, the plugin is sending an id");
      console.log("  that is not the users._id the dashboard joins on, and every one");
      console.log("  of these people reads as zero jobs:");
      for (const row of telemetryUnmatched.slice(0, 20)) console.log(`    ${row._id} (${row.jobs} jobs)`);
      if (telemetryUnmatched.length > 20) console.log(`    ...and ${telemetryUnmatched.length - 20} more`);
    }

    const rescued = [...telemetryAccounts].filter((idValue) => !committedUsers.has(idValue)).length;
    const genuinelyIdle = users.filter(
      (row) => !committedUsers.has(String(row._id)) && !telemetryAccounts.has(String(row._id))
    ).length;
    console.log("\n  Resulting job column:");
    line("users showing a credited count", committedUsers.size);
    line("users showing an observed count instead", rescued);
    line("users genuinely showing zero", genuinelyIdle);
  } catch (error) {
    line("unavailable", error?.message || String(error));
  }

  // --- Does the plugin actually report finished runs? ---------------------
  //
  // A job is a run that FINISHED, which only tool_action_completed says. If a
  // tool emits opens, starts and feature_used but never a completion, its job
  // count is unmeasurable from telemetry — and that is an instrumentation gap
  // to fix in the plugin, not a number to substitute here.
  console.log("\n=== Finished-run reporting, per tool ===");
  try {
    const events = await getEventsCollection();
    const perTool = await events
      .aggregate([
        { $match: { tool: { $nin: [null, "", "unknown"] } } },
        {
          $group: {
            _id: "$tool",
            opened: { $sum: { $cond: [{ $eq: ["$eventType", "tool_opened"] }, 1, 0] } },
            started: { $sum: { $cond: [{ $eq: ["$eventType", "tool_action_started"] }, 1, 0] } },
            completed: { $sum: { $cond: [{ $eq: ["$eventType", "tool_action_completed"] }, 1, 0] } },
            failed: { $sum: { $cond: [{ $eq: ["$eventType", "tool_action_failed"] }, 1, 0] } },
            featureUsed: { $sum: { $cond: [{ $eq: ["$eventType", "feature_used"] }, 1, 0] } },
          },
        },
        { $sort: { completed: -1 } },
      ])
      .toArray();

    console.log(`  ${"tool".padEnd(26)}${"opened".padStart(9)}${"started".padStart(9)}${"COMPLETED".padStart(11)}${"failed".padStart(9)}${"feature".padStart(9)}`);
    for (const row of perTool) {
      console.log(
        `  ${String(row._id).padEnd(26)}${String(row.opened).padStart(9)}${String(row.started).padStart(9)}` +
        `${String(row.completed).padStart(11)}${String(row.failed).padStart(9)}${String(row.featureUsed).padStart(9)}`
      );
    }
    const silent = perTool.filter((row) => !row.completed && (row.started || row.featureUsed));
    if (silent.length) {
      console.log("\n  These tools show activity but NEVER report a finished run, so their");
      console.log("  job counts read zero no matter what the dashboard does. Emit");
      console.log("  tool_action_completed when the run succeeds:");
      for (const row of silent) console.log(`    ${row._id}`);
    } else if (perTool.length) {
      console.log("\n  Every active tool reports finished runs.");
    }
  } catch (error) {
    line("unavailable", error?.message || String(error));
  }

  // --- How much finished work did nobody ask for? -------------------------
  //
  // Opening the plugin starts background services that report completions.
  // A completion counts as a job only if the same session shows the user
  // opening that tool first. This shows how many are being set aside, and how
  // soon after launch they fire.
  console.log("\n=== Background completions ===");
  try {
    const events = await getEventsCollection();
    const opens = new Map();
    const toolsWithOpens = new Set();
    for (const row of await events
      .aggregate([
        { $match: { eventType: "tool_opened" } },
        { $group: { _id: { session: "$sessionId", tool: "$tool" }, firstOpenAt: { $min: "$eventAt" } } },
      ])
      .toArray()) {
      const tool = String(row._id?.tool || "").trim().toLowerCase();
      if (!tool) continue;
      toolsWithOpens.add(tool);
      opens.set(`${row._id?.session}|${tool}`, new Date(row.firstOpenAt));
    }

    const sessionStart = new Map();
    for (const row of await events
      .aggregate([{ $group: { _id: "$sessionId", firstAt: { $min: "$eventAt" } } }])
      .toArray()) {
      sessionStart.set(String(row._id), new Date(row.firstAt));
    }

    const completions = await events
      .find(
        { eventType: "tool_action_completed" },
        { projection: { tool: 1, sessionId: 1, eventAt: 1 } }
      )
      .toArray();

    const perTool = new Map();
    const lagBuckets = { "0-5s": 0, "5-30s": 0, "30s-2m": 0, "2m+": 0 };
    for (const row of completions) {
      const tool = String(row.tool || "").trim().toLowerCase();
      const stat = perTool.get(tool) || { counted: 0, background: 0, ungated: 0 };
      if (!toolsWithOpens.has(tool)) {
        stat.ungated += 1;
      } else {
        const openedAt = opens.get(`${row.sessionId}|${tool}`);
        if (openedAt && new Date(row.eventAt) >= openedAt) stat.counted += 1;
        else {
          stat.background += 1;
          const startedAt = sessionStart.get(String(row.sessionId));
          const lag = startedAt ? (new Date(row.eventAt) - startedAt) / 1000 : null;
          if (lag === null) continue;
          if (lag <= 5) lagBuckets["0-5s"] += 1;
          else if (lag <= 30) lagBuckets["5-30s"] += 1;
          else if (lag <= 120) lagBuckets["30s-2m"] += 1;
          else lagBuckets["2m+"] += 1;
        }
      }
      perTool.set(tool, stat);
    }

    line("total tool_action_completed events", completions.length);
    console.log(`\n  ${"tool".padEnd(26)}${"counted".padStart(10)}${"background".padStart(12)}${"ungated".padStart(10)}`);
    for (const [tool, stat] of [...perTool].sort((a, b) => b[1].background - a[1].background)) {
      console.log(`  ${tool.padEnd(26)}${String(stat.counted).padStart(10)}${String(stat.background).padStart(12)}${String(stat.ungated).padStart(10)}`);
    }
    console.log("\n  How long after the session's first event the background ones fire:");
    for (const [label, count] of Object.entries(lagBuckets)) line(`    ${label}`, count);
    console.log("\n  A pile-up in 0-5s is the launch burst — services starting with the");
    console.log("  plugin. 'ungated' means that tool never reports tool_opened, so the");
    console.log("  rule cannot judge it and all its completions are counted; emit");
    console.log("  tool_opened for it to make its numbers trustworthy.");
  } catch (error) {
    line("unavailable", error?.message || String(error));
  }

  // --- Are plugin identities stable enough to count people with? ---------
  console.log("\n=== Plugin identity stability ===");
  try {
    const events = await getEventsCollection();
    const [shape] = await events
      .aggregate([
        {
          $group: {
            _id: null,
            devices: { $addToSet: "$deviceId" },
            sessions: { $addToSet: "$sessionId" },
          },
        },
        { $project: { devices: { $size: "$devices" }, sessions: { $size: "$sessions" } } },
      ])
      .toArray();

    if (!shape) {
      line("no events", "nothing to assess");
    } else {
      line("distinct device ids", shape.devices);
      line("distinct session ids", shape.sessions);
      const ratio = shape.sessions ? Math.round((shape.devices / shape.sessions) * 100) / 100 : 0;
      line("device ids per session", ratio);
      console.log(
        ratio >= 0.75
          ? "\n  A ratio at or near 1 means the plugin mints a new device id on\n" +
              "  every launch instead of persisting one per install. A device id then\n" +
              "  identifies a LAUNCH, so a signed-out person is a brand-new visitor\n" +
              "  every time they open the plugin. Fix by persisting it in\n" +
              "  figma.clientStorage. Signed-in users are unaffected — the dashboard\n" +
              "  stitches their launches together through the session id."
          : "\n  Device ids persist across launches, so signed-out visitors are\n  counted once each."
      );

      // Sessions that produced events but never carried an account. Under a
      // hard login gate these should be the plugin's boot window only.
      const sessionAccounts = await events
        .aggregate([
          {
            $group: {
              _id: "$sessionId",
              accounts: { $addToSet: { $ifNull: ["$user.userId", "$user.email"] } },
              events: { $sum: 1 },
            },
          },
          {
            $project: {
              events: 1,
              named: { $size: { $filter: { input: "$accounts", cond: { $ne: ["$$this", null] } } } },
            },
          },
        ])
        .toArray();
      const neverAuthenticated = sessionAccounts.filter((row) => !row.named);
      const ambiguous = sessionAccounts.filter((row) => row.named > 1);
      console.log("");
      line("sessions that carried an account at some point", sessionAccounts.length - neverAuthenticated.length);
      line("sessions that NEVER carried an account", neverAuthenticated.length);
      line("...events inside them", neverAuthenticated.reduce((sum, row) => sum + row.events, 0));
      line("sessions carrying two or more accounts", ambiguous.length);
      console.log("\n  Sessions that carried an account are stitched whole to that account,");
      console.log("  including the pre-authentication events at the head of the launch.");
      console.log("  Only the never-authenticated ones remain as signed-out visitors.");
      console.log("  Sessions with two accounts are left unattributed on purpose.");
    }
  } catch (error) {
    line("unavailable", error?.message || String(error));
  }

  console.log("\n=== Reading this ===");
  console.log("  The users page lists the users collection, one row per account, 25");
  console.log("  per page by default. An identity is invisible there if it has no");
  console.log("  users document (orphaned above), or if you are past page 1, or if a");
  console.log("  segment/search filter is active.");
  console.log("  A user WITH a document always appears, even with zero sessions —");
  console.log("  so a non-zero 'orphaned' count is the explanation worth acting on.\n");
}

main()
  .catch((error) => {
    console.error("Diagnosis failed:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDb();
  });
