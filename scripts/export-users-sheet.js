#!/usr/bin/env node
/**
 * Dumps the Users tab as CSV or JSON, for a one-off backfill by paste-import
 * or for inspecting what the daily sync would write.
 *
 *   node scripts/export-users-sheet.js            # CSV to stdout
 *   node scripts/export-users-sheet.js --json     # full payload, with evidence
 *   node scripts/export-users-sheet.js --days=90
 *
 * Read-only. Nothing is written anywhere.
 */
import dotenv from "dotenv";

import { closeDb } from "../src/db.js";
import { SHEET_COLUMNS, usersSheetExport } from "../src/sheet-export.js";

dotenv.config();

const asJson = process.argv.includes("--json");
const daysArg = process.argv.find((value) => value.startsWith("--days="));
const days = daysArg ? Number(daysArg.split("=")[1]) : 30;

// Quoted per RFC 4180 so a name containing a comma or a quote survives the trip
// into a spreadsheet intact.
const csvCell = (value) => {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
};

async function main() {
  const payload = await usersSheetExport({ days });
  if (asJson) {
    console.log(JSON.stringify(payload, null, 2));
    return;
  }
  // Column E is skipped in SHEET_COLUMNS, so the CSV would silently shift every
  // column after it. An explicit empty Source column keeps the paste aligned.
  const headers = ["User ID", "Name", "Email", "Signup Date", "Source", "Liked Feature", "Active Status", "Last Active Date", "Plan"];
  const fields = ["userId", "name", "email", "signupDate", null, "likedFeature", "activeStatus", "lastActiveDate", "plan"];
  console.log(headers.map(csvCell).join(","));
  for (const row of payload.rows) {
    console.log(fields.map((field) => csvCell(field ? row[field] : "")).join(","));
  }
  console.error(`\n${payload.rows.length} accounts. ${payload.coverage.message}`);
  console.error(`Columns written: ${SHEET_COLUMNS.map((column) => column.column).join(", ")}. Source (E) is left for you to fill in.`);
}

main()
  .catch((error) => {
    console.error("Export failed:", error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDb();
  });
