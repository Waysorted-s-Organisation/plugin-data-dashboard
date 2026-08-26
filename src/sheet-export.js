import { productUsers } from "./product-intelligence.js";

/**
 * The Users tab of the conversion tracker, built from the dashboard's own
 * numbers.
 *
 * The sheet is co-owned. Signup, activity, tool use and plan are facts the
 * dashboard holds and a person should never have to retype; acquisition source
 * and everything on the Activity Log are facts only a person knows. So this
 * exports the machine-owned columns and nothing else, and the writer leaves the
 * rest of every row alone.
 *
 * Email is the row key. It is the only column both sides can agree on: the
 * sheet was populated by hand and has no account ids in it, and a person adding
 * a row types an email, not an ObjectId.
 */

/**
 * The values the sheet's own dropdowns accept.
 *
 * Exported so the setup instructions and the tests read from one list rather
 * than two that can drift.
 */
export const SHEET_ACTIVE_STATUSES = ["New", "Active", "Dormant", "Churned"];
export const SHEET_PLANS = ["Free", "Trial", "Premium"];

/**
 * Where the person is right now, in the sheet's four words.
 *
 * Deliberately silent when plugin activity could not be read. "Churned" is a
 * claim that someone stopped, and the evidence that would disprove it is the
 * activity index — asserting it from data that was never retrieved is the same
 * mistake the availability flag exists to prevent, except here it would be
 * written into a spreadsheet a human then acts on.
 */
export function activeStatus(row) {
  if (row.pluginActivityAvailable === false) return "";
  const segments = row.segments || [];
  if (segments.includes("dormant")) return "Churned";
  if (segments.includes("at_risk")) return "Dormant";
  if (segments.includes("new")) return "New";
  if (row.lastActiveAt || row.lastLoginAt) return "Active";
  return "";
}

/**
 * Free, Trial or Premium.
 *
 * Reads the lifecycle stage rather than the wallet alone, so a subscription
 * granted straight from the backend — which writes no wallet — still reads as
 * Premium rather than Free.
 */
export function plan(row) {
  const status = String(row.subscriptionStatus || "").toLowerCase();
  if (status === "trialing") return "Trial";
  if (row.lifecycleStage === "customer") return "Premium";
  return "Free";
}

const isoDay = (value) => {
  if (!value) return "";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString().slice(0, 10);
};

/**
 * @param days Window used for the "new" segment and the in-range figures. The
 *   status and tool columns themselves are lifetime facts.
 */
export async function usersSheetRows({ days = 30 } = {}) {
  const rows = [];
  let page = 1;
  let pages = 1;
  // Paged rather than read whole: productUsers is the same function the users
  // page calls, so the export cannot drift from what the dashboard shows.
  do {
    const payload = await productUsers({ days, page, pageSize: 100, identity: "accounts" });
    pages = payload.pagination.pages;
    for (const row of payload.items) {
      // Signed-out visitors are excluded by identity: "accounts". They have no
      // email, and a sheet keyed on email cannot hold them without inventing
      // one.
      if (!row.email) continue;
      rows.push({
        userId: row.id,
        name: row.name || "",
        email: row.email,
        signupDate: isoDay(row.joinedAt),
        likedFeature: row.topTool?.label || "",
        activeStatus: activeStatus(row),
        lastActiveDate: isoDay(row.lastActiveAt || row.lastLoginAt),
        plan: plan(row),
        // Not written to the sheet. Carried so a reviewer can see what the
        // status and tool columns were derived from without opening the
        // dashboard.
        evidence: {
          lifecycleStage: row.lifecycleStage,
          segments: row.segments,
          completedJobs: row.completedJobs,
          completedJobsSource: row.completedJobsSource,
          activityMeasured: row.pluginActivityAvailable !== false,
        },
      });
    }
    page += 1;
  } while (page <= pages);

  rows.sort((a, b) => String(a.signupDate).localeCompare(String(b.signupDate)) || a.email.localeCompare(b.email));
  return rows;
}

/**
 * Column order on the Users tab. Index 4 (Source, column E) is absent on
 * purpose: it is the acquisition channel, which the dashboard does not know and
 * must not overwrite with the authentication source it does know. They are
 * different facts that happen to sound alike.
 */
export const SHEET_COLUMNS = [
  { column: "A", header: "User ID", field: "userId" },
  { column: "B", header: "Name", field: "name" },
  { column: "C", header: "Email", field: "email" },
  { column: "D", header: "Signup Date", field: "signupDate" },
  { column: "F", header: "Liked Feature", field: "likedFeature" },
  { column: "G", header: "Active Status", field: "activeStatus" },
  { column: "H", header: "Last Active Date", field: "lastActiveDate" },
  { column: "I", header: "Plan", field: "plan" },
];

export async function usersSheetExport({ days = 30 } = {}) {
  const rows = await usersSheetRows({ days });
  const unmeasured = rows.filter((row) => !row.evidence.activityMeasured).length;
  return {
    asOf: new Date().toISOString(),
    period: { days },
    sheet: { tab: "Users", keyColumn: "C", keyField: "email", firstDataRow: 2 },
    columns: SHEET_COLUMNS,
    // Named so the writer cannot guess: anything not listed here is a human's
    // to fill in and must survive a sync untouched.
    humanOwnedColumns: ["E"],
    coverage: {
      users: rows.length,
      statusUnavailable: unmeasured,
      message: unmeasured
        ? `${unmeasured} rows have no Active Status because plugin activity could not be read. They are left blank rather than reported as churned.`
        : "Every row's status is backed by measured activity.",
    },
    rows,
  };
}
