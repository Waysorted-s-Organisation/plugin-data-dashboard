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
  getBackendSessionsCollection,
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
