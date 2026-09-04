import { ObjectId } from "mongodb";

import {
  getBackendCreditLedgerCollection,
  getBackendFeatureRequestsCollection,
  getBackendFeedbackCollection,
  getBackendFeedbacksCollection,
  getBackendPurchasesCollection,
  getBackendRefundsCollection,
  getBackendSessionsCollection,
  getBackendStarterGrantsCollection,
  getBackendSubscriptionsCollection,
  getBackendToolsCollection,
  getBackendUsageReservationsCollection,
  getBackendUserBillingCollection,
  getBackendUsersCollection,
  getEventsCollection,
  onDbClose,
} from "./db.js";
import {
  DAY_MS,
  NON_TOOL_SURFACES,
  asDate,
  asNumber,
  change,
  inRange,
  loginAt,
  median,
  normalizeToolCode,
  percent,
  period,
  reservationAt,
  successfulSession,
  terminalReservation,
} from "./product-metrics.js";

const id = (value) => value === null || value === undefined ? null : String(value);
const lowCreditThreshold = () => Math.max(0, asNumber(process.env.CREDIT_LOW_THRESHOLD, 20));

/**
 * Hard ceiling on any single telemetry aggregation.
 *
 * These run inside a serverless function with a fixed wall clock, and each of
 * them already has a defined behaviour for "could not read this": activity
 * reports `available: false` so rows say "Not measured" instead of asserting
 * zero, identity resolution degrades to fewer links, and the tools grid falls
 * back to the reservation-derived numbers. All of those are survivable. The
 * function running out of time is not — the page fails instead.
 *
 * So every aggregation is given a budget and allowed to lose. A timeout raises
 * inside the same try/catch that already handles an unreachable store.
 */
function aggregationOptions() {
  const configured = Number(String(process.env.ANALYTICS_AGGREGATION_TIMEOUT_MS || "").trim());
  const maxTimeMS = Number.isFinite(configured) && configured > 0 ? configured : 20000;
  return { maxTimeMS, allowDiskUse: true };
}

/**
 * Subscription states that mean money has changed hands, or is committed to.
 *
 * `past_due` and `grace` are included deliberately: the person paid before and
 * has not been cut off, which is a customer with a billing problem, not a
 * stranger.
 */
const COMMERCIAL_SUBSCRIPTION_STATUSES = new Set(["active", "cancel_scheduled", "trialing", "past_due", "grace"]);
const DEAD_SUBSCRIPTION_STATUSES = new Set(["cancelled", "canceled", "expired", "payment_pending", "failed", "refunded", "created"]);

/**
 * A subscription that currently entitles the user to something.
 *
 * The `subscriptions` collection was loaded, indexed and returned on the
 * profile, but nothing ever read it to decide commercial standing — that came
 * only from the wallet's own `subscriptionStatus`. A subscription granted
 * directly in the backend writes a subscription record without necessarily
 * initialising a wallet, so a real paying — or comped — subscriber rendered
 * byte-for-byte identically to someone who had never opened a checkout: no
 * wallet, no plan, stage "activated".
 *
 * An unexpired period is treated as substantive on its own, because a hand-made
 * record can carry a status string this dashboard has never seen.
 */
function activeSubscription(rows = [], now = new Date()) {
  return rows.find((row) => {
    const status = String(row.status || "").toLowerCase();
    if (COMMERCIAL_SUBSCRIPTION_STATUSES.has(status)) return true;
    if (DEAD_SUBSCRIPTION_STATUSES.has(status)) return false;
    const end = asDate(row.currentPeriodEnd);
    return Boolean(end && end > now);
  }) || null;
}
export const SEMANTIC_EVENT_TYPES = ["plugin_session_started", "plugin_session_ended", "tool_opened", "tool_closed", "tool_action_started", "tool_action_completed", "tool_action_failed", "feature_used", "active_tool_time", "favorite_changed", "billing_cta_viewed", "billing_cta_clicked", "user_facing_error_displayed", "feedback_submitted"];

/**
 * Timezone used to decide which calendar day an event belongs to.
 *
 * Day bucketing was previously always UTC, so for a user in UTC+5:30 any
 * activity before 05:30 local counted as the previous day — two genuine visits
 * on consecutive local days could collapse into one and not register as a
 * return. It defaults to UTC so existing numbers do not shift without an
 * explicit decision; set REPORTING_TIMEZONE (e.g. "Asia/Kolkata") to report in
 * the timezone the users actually live in.
 */
function reportingTimezone() {
  const configured = String(process.env.REPORTING_TIMEZONE || "").trim();
  if (!configured) return "UTC";
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone: configured });
    return configured;
  } catch {
    console.error(`Invalid REPORTING_TIMEZONE "${configured}"; falling back to UTC.`);
    return "UTC";
  }
}

function dayKeyFormatter(timezone) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit",
  });
}

const looksLikeEmail = (value) => typeof value === "string" && value.includes("@");

/**
 * Placeholders the ingest writes when the plugin sends no identifier.
 *
 * `src/server.js` substitutes the literal strings "unknown-device" and
 * "unknown-session" so a document always has the field. They are not
 * identities: treating them as one merges every device that failed to report
 * itself into a single "visitor", and — worse — an identity_linked event
 * carrying no device id would map the sentinel to a real account, dragging
 * every unattributed device's activity onto that one person.
 */
const ANONYMOUS_SENTINELS = new Set(["unknown-device", "unknown-session", "unknown", "null", "undefined"]);

const anonymousKey = (value) => {
  const text = String(value ?? "").trim();
  return text && !ANONYMOUS_SENTINELS.has(text.toLowerCase()) ? text : null;
};

/**
 * The account an unidentified event belongs to, if any.
 *
 * Session first, pseudonymous id second. The session is the stronger evidence
 * and the only one that works under a hard login gate, where the anonymous
 * events are the head of an authenticated launch rather than a separate
 * signed-out life. The anonymous id still answers the genuine case: someone who
 * used the plugin signed out and later created an account.
 */
function resolveAnonymous(sessionId, anonymous, sessionLinks, identityLinks) {
  const session = anonymousKey(sessionId);
  if (session && sessionLinks?.has(session)) return sessionLinks.get(session);
  return (anonymous && identityLinks?.get(anonymous)) || null;
}

/**
 * Events the plugin emits on its own, without the user doing anything.
 *
 * A heartbeat fires every 30 seconds for as long as the plugin is open, so
 * counting these as activity would make a plugin left open in a background tab
 * look like an engaged — and returning — user, and would let passive traffic
 * dominate the tools ranking. Activity must mean the person did something.
 */
export const PASSIVE_EVENT_TYPES = [
  "session_heartbeat",
  "backend_operation",
  "user_context_changed",
  // Emitted automatically when a signed-out session signs in. It is the record
  // that stitches their history to the account, so it must be stored and read —
  // but signing in is not tool use, and counting it would add a phantom active
  // day on the conversion date.
  "identity_linked",
  "analytics_transport_updated",
  "user_notification_shown",
  // Emitted by the sign-in poller's own timer once an attempt passes forty
  // seconds, not by anything the user did. It belongs in the auth funnel, but
  // counting it as activity would credit an active day to someone who clicked
  // once and then sat still.
  "auth_pending_slow",
];

/**
 * Events not worth persisting at all — a strict subset of the passive set.
 *
 * "Does not count as activity" and "is not worth storing" are different
 * questions, and conflating them loses real signal: backend_operation records
 * backend failures the user actually hit, user_context_changed records sign-in
 * transitions, and user_notification_shown records what was said to the user.
 * None of those should inflate an activity metric, but all of them answer
 * "what happened to this person". Only genuinely repetitive, information-free
 * traffic belongs here.
 */
export const NON_PERSISTED_EVENT_TYPES = [
  "session_heartbeat",
  "analytics_transport_updated",
];

/**
 * Distinct days on which each identity was active in the plugin.
 *
 * Returning users were previously derived only from backend login sessions.
 * The plugin authenticates from a stored token and so creates no new session
 * document on later launches — a user could open it every day for a month and
 * never register a single return. Plugin activity is the missing evidence.
 *
 * Events written before the identity fix may carry an email address in
 * user.userId. Rather than rewriting stored documents, those keys are detected
 * and routed to the email index here, so historical data resolves correctly on
 * read.
 */
/**
 * Maps a pseudonymous id to the account it turned out to belong to.
 *
 * The plugin emits identity_linked when a signed-out session signs in, and it
 * is the only record of that connection: both the plugin and the ingest blank
 * anonymousId once a user is authenticated, so after the fact there is nothing
 * left to join on. Without this, everything a person did before signing up is
 * stranded under their old pseudonymous id — they lose their activation, their
 * returns and their history at the exact moment they convert, and are
 * simultaneously still counted as a live anonymous visitor.
 *
 * Built over ALL time rather than the reporting window: a link made in March
 * must still resolve March activity when a July window is requested.
 */
async function identityLinksByAnonymousId() {
  const links = new Map();
  try {
    const collection = await getEventsCollection();
    const rows = await collection
      .aggregate([
        { $match: { eventType: "identity_linked" } },
        { $sort: { eventAt: 1 } },
        {
          $group: {
            _id: "$payload.anonymousId",
            userId: { $last: "$payload.userId" },
            email: { $last: "$payload.email" },
            // The device the link happened on. Anonymous activity is keyed by
            // device, so the link has to be findable by device too — the
            // pseudonymous id in the payload is only one of the two identities
            // a device produces. Both are launch-scoped in practice, which is
            // why accountsBySessionId exists alongside this.
            deviceIds: { $addToSet: "$deviceId" },
          },
        },
      ], aggregationOptions())
      .toArray();

    for (const row of rows) {
      const userId = row.userId ? String(row.userId) : null;
      const email = row.email ? String(row.email).toLowerCase() : null;
      // The account id is preferred; email is the fallback because the join on
      // the read side resolves it against a real user record.
      if (!userId && !email) continue;
      const account = userId || email;
      const anonymous = anonymousKey(row._id);
      if (anonymous) links.set(anonymous, account);
      for (const deviceId of row.deviceIds || []) {
        const device = anonymousKey(deviceId);
        if (device) links.set(device, account);
      }
    }
  } catch (error) {
    // Losing the links degrades attribution; it must not break the page.
    console.error("Identity link aggregation failed:", error?.message || error);
  }
  return links;
}

/**
 * The account that signed in during each plugin session.
 *
 * The plugin requires a login before any tool can be used, yet the users table
 * was full of "signed-out visitor" rows doing real work in real tools. They are
 * not signed-out people: they are the opening seconds of a signed-in person's
 * session. Events start flowing at plugin boot, before the stored token has
 * been validated and the user object populated, so the first events of every
 * launch carry no identity. Nothing stitched them to the account, so every
 * launch also produced a phantom visitor.
 *
 * identity_linked cannot cover this. It fires on a signed-out session SIGNING
 * IN, which under a hard login gate never happens — the person is already
 * authenticated, the events merely precede the plugin knowing it.
 *
 * The session id can cover it, and is the strongest join available: it is
 * written on every event, it is scoped to one launch on one machine, and if any
 * event in it carries an account then every event in it belongs to that
 * account. A session carrying two different accounts is left unresolved rather
 * than guessed at.
 *
 * Built over ALL time, like the anonymous-id links, so a July window still
 * resolves a March session.
 */
async function accountsBySessionId(start) {
  const links = new Map();
  try {
    const collection = await getEventsCollection();
    const rows = await collection
      .aggregate([
        {
          $match: {
            // Bounded to the same lookback every other read uses. A session
            // older than that can contribute to nothing, and an unbounded scan
            // of the whole event collection is the one query here big enough to
            // threaten the function's wall clock.
            eventAt: { $gte: start },
            $or: [
              { "user.userId": { $nin: [null, ""] } },
              { "user.email": { $nin: [null, ""] } },
              { eventType: "identity_linked" },
            ],
          },
        },
        {
          $group: {
            _id: "$sessionId",
            // identity_linked carries the account in its payload rather than in
            // user, so both places are read.
            userIds: { $addToSet: "$user.userId" },
            emails: { $addToSet: "$user.email" },
            linkedUserIds: { $addToSet: "$payload.userId" },
            linkedEmails: { $addToSet: "$payload.email" },
          },
        },
      ], aggregationOptions())
      .toArray();

    for (const row of rows) {
      const session = anonymousKey(row._id);
      if (!session) continue;
      const candidates = [...(row.userIds || []), ...(row.linkedUserIds || []), ...(row.emails || []), ...(row.linkedEmails || [])]
        .filter(Boolean)
        .map(String);
      // Events written before the identity fix store an email in user.userId,
      // so the two are separated by shape rather than by field.
      const accountIds = [...new Set(candidates.filter((value) => !looksLikeEmail(value)))];
      const emails = [...new Set(candidates.filter(looksLikeEmail).map((value) => value.toLowerCase()))];
      // Two accounts in one session means someone switched users mid-launch.
      // Attributing the whole session to either one would move the other's work
      // onto the wrong person, so it is left anonymous instead.
      if (accountIds.length > 1) continue;
      if (!accountIds.length && emails.length > 1) continue;
      const account = accountIds[0] || emails[0] || null;
      if (account) links.set(session, account);
    }
  } catch (error) {
    // Losing the links degrades attribution; it must not break the page.
    console.error("Session identity aggregation failed:", error?.message || error);
  }
  return links;
}

async function pluginActivityDaysByIdentity(start, end, identityLinks = new Map(), sessionLinks = new Map()) {
  const timezone = reportingTimezone();
  const byUserId = new Map();
  const byEmail = new Map();
  // Signed-out visitors. They have no account and no email, but the plugin
  // gives every session a stable pseudonymous id, so their behaviour is
  // measurable even though who they are is not. Grouping only by userId/email
  // discarded them entirely, leaving the platform's largest population
  // unmeasured purely because it had no name attached.
  const byAnonymous = new Map();
  const namesByAnonymous = new Map();
  let available = true;
  try {
    const collection = await getEventsCollection();
    const rows = await collection
      .aggregate([
        // Half-open, matching `inRange`. With an inclusive upper bound an event
        // landing exactly on a period boundary was counted in the previous
        // window AND the current one, biasing every change percentage measured
        // between them.
        { $match: { eventAt: { $gte: start, $lt: end }, eventType: { $nin: PASSIVE_EVENT_TYPES } } },
        {
          $group: {
            _id: {
              // Email first, account id second.
              //
              // Keying on the account id first stranded anyone whose id does
              // not join: the plugin's user.userId is whatever the plugin
              // believes the account is, and when that is not the backend
              // users._id the row was filed under a key nothing resolves — even
              // when the very same event carried a perfectly good email. Email
              // also converges the two eras of stored events, since rows
              // written before the identity fix hold an email IN user.userId.
              identity: { $ifNull: ["$user.email", "$user.userId"] },
              // Carried so a signed-out visitor can be shown by the name Figma
              // gives them, which is how they are recognisable to the owner.
              name: "$user.name",
              // Keys a visitor who never signs in. The device id is preferred
              // over the pseudonymous id because the plugin flips the latter
              // between a Figma-derived value and a device-derived one within a
              // single session, splitting one person into two.
              //
              // Neither is stable across launches: production device ids arrive
              // as `device_<epoch-ms>_<suffix>` whose timestamp is the launch
              // that minted them, so this key identifies a LAUNCH. That is
              // corrected for anyone who signs in — the session link above
              // moves their whole launch onto the account — and reported as a
              // measurement limit for those who never do. See identityHealth.
              anonymous: { $ifNull: ["$deviceId", "$user.anonymousId"] },
              // Carried so the opening, pre-authentication events of a launch
              // can be resolved to whoever the session turned out to belong to.
              session: "$sessionId",
              day: { $dateToString: { date: "$eventAt", format: "%Y-%m-%d", timezone } },
            },
          },
        },
        {
          $group: {
            _id: { identity: "$_id.identity", anonymous: "$_id.anonymous", session: "$_id.session" },
            days: { $addToSet: "$_id.day" },
            latestName: { $last: "$_id.name" },
          },
        },
      ], aggregationOptions())
      .toArray();

    for (const row of rows) {
      const identity = row._id?.identity;
      const anonymous = anonymousKey(row._id?.anonymous);
      // An unidentified row is routed to the account it belongs to. The session
      // is tried first: under a hard login gate almost every anonymous row is
      // the pre-authentication head of a signed-in session, and the session id
      // is the only key that survives a device id minted fresh at each launch.
      const linked = identity ? null : resolveAnonymous(row._id?.session, anonymous, sessionLinks, identityLinks);
      const resolved = identity || linked || null;
      const target = !resolved ? byAnonymous : looksLikeEmail(resolved) ? byEmail : byUserId;
      const rawKey = resolved || anonymous;
      if (!rawKey) continue;
      const key = looksLikeEmail(rawKey) ? String(rawKey).toLowerCase() : String(rawKey);
      const existing = target.get(key) || new Set();
      for (const day of row.days || []) existing.add(day);
      target.set(key, existing);
      if (target === byAnonymous && row.latestName) {
        namesByAnonymous.set(key, String(row.latestName));
      }
    }
  } catch (error) {
    // Login-derived days must still work if the analytics store is unreachable,
    // but the absence of plugin data is then unknown rather than known to be
    // zero. `available` lets callers say "we could not read this" instead of
    // asserting "this user did nothing".
    available = false;
    console.error("Plugin activity day aggregation failed:", error?.message || error);
  }
  // Built once here rather than per user: userFacts runs for every row in the
  // users table, and constructing an Intl.DateTimeFormat is not cheap.
  return { byUserId, byEmail, byAnonymous, namesByAnonymous, timezone, available, formatDay: dayKeyFormatter(timezone) };
}

/**
 * Sessions in which the user actually opened each tool.
 *
 * Opening the plugin starts a fleet of background services — sync, credit
 * refresh, license checks, prefetch — and they report themselves the same way a
 * user's run does, with a `tool_action_completed` carrying whatever surface the
 * plugin happens to be on. Counting those made every launch look like finished
 * work, so the job column measured how often people opened the plugin rather
 * than how much they got done.
 *
 * The distinguishing fact is not in the event's name — it is that nobody opened
 * the tool. A person has to open a tool to run it; a background service does
 * not. So a completion counts only when the same session shows a `tool_opened`
 * for that same tool at or before it.
 *
 * Deliberately not a list of background action names: those are the plugin's
 * internal vocabulary, they change without notice, and guessing them wrong
 * silently deletes real work. This rule reads only what is already recorded.
 *
 * Two escape hatches keep the rule from over-deleting:
 *   - A tool that emits no `tool_opened` ANYWHERE in the window cannot be
 *     judged by this rule, so its completions are all counted and the tool is
 *     named as ungated.
 *   - Opens are looked up from twelve hours before the window, so a session
 *     straddling the period boundary does not lose the open that justifies its
 *     completions.
 */
const SESSION_LOOKBEHIND_MS = 12 * 60 * 60 * 1000;

async function toolOpensBySessionTool(start, end) {
  const opens = new Map();
  const toolsWithOpens = new Set();
  try {
    const collection = await getEventsCollection();
    const rows = await collection
      .aggregate([
        {
          $match: {
            eventType: "tool_opened",
            eventAt: { $gte: new Date(start.getTime() - SESSION_LOOKBEHIND_MS), $lt: end },
          },
        },
        { $group: { _id: { session: "$sessionId", tool: "$tool" }, firstOpenAt: { $min: "$eventAt" } } },
      ], aggregationOptions())
      .toArray();
    for (const row of rows) {
      const tool = String(row._id?.tool || "").trim().toLowerCase();
      if (!tool) continue;
      toolsWithOpens.add(tool);
      const session = anonymousKey(row._id?.session);
      const openedAt = asDate(row.firstOpenAt);
      if (!session || !openedAt) continue;
      opens.set(`${session}|${tool}`, openedAt);
    }
  } catch (error) {
    // Without this index every completion is counted, which is the behaviour
    // that existed before the rule. Degrading to "count everything" is the
    // right failure: it over-reports rather than deleting real work.
    console.error("Tool-open index failed:", error?.message || error);
  }
  return { opens, toolsWithOpens };
}

/**
 * Completions in one session-and-tool group that a person actually asked for.
 *
 * Returns the counted total and the number set aside as background, so the
 * amount being filtered is reportable rather than invisible.
 */
function countUserInitiated(completions, sessionId, rawTool, openIndex) {
  const finished = completions.map(asDate).filter(Boolean);
  if (!finished.length) return { jobs: 0, background: 0, ungated: 0 };
  // Nothing to compare against for this tool, so the rule does not apply.
  if (!openIndex.toolsWithOpens.has(rawTool)) return { jobs: finished.length, background: 0, ungated: finished.length };
  const session = anonymousKey(sessionId);
  const openedAt = session ? openIndex.opens.get(`${session}|${rawTool}`) : null;
  if (!openedAt) return { jobs: 0, background: finished.length, ungated: 0 };
  const jobs = finished.filter((at) => at >= openedAt).length;
  return { jobs, background: finished.length - jobs, ungated: 0 };
}

/**
 * What each identity did with tools, derived from plugin activity.
 *
 * Two answers come out of one aggregation pass, because they need the same
 * grouping: which tool the identity touched most recently, and how many tool
 * jobs they actually finished.
 *
 * The users table's top-tool column was built purely from credit-charged
 * reservations, so a tool that charges nothing could never appear there and its
 * users read as having used nothing at all. Telemetry knows what they actually
 * opened, whether or not it billed them.
 *
 * The job count exists for the same reason one column to the left. "Credited
 * jobs" counts committed reservations, so a person whose run never produced one
 * — the tool did not bill, the hold is still open, the reservation lost its
 * user link — reads as zero jobs beside a tool they demonstrably used. Completed
 * tool actions give that row a number backed by evidence.
 *
 * The two counts are returned separately and must never be summed: a credited
 * run emits a committed reservation AND a tool_action_completed, so adding them
 * would count one job twice.
 */
async function pluginToolUseByIdentity(start, end, identityLinks = new Map(), sessionLinks = new Map()) {
  const top = { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map() };
  const counts = { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map() };
  let available = true;
  let backgroundCompletions = 0;
  const ungatedTools = new Set();
  try {
    const collection = await getEventsCollection();
    // Resolved per (session, tool) rather than inside the grouping below,
    // because the two are not aligned: the head of a launch is unidentified and
    // the tail is not, so an open and the completion it justifies can land in
    // different identity groups for the same session.
    const openIndex = await toolOpensBySessionTool(start, end);
    const rows = await collection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: start, $lt: end },
            eventType: { $nin: PASSIVE_EVENT_TYPES },
            tool: { $nin: [null, "", "unknown"] },
          },
        },
        {
          $group: {
            _id: {
              // Email first, account id second.
              //
              // Keying on the account id first stranded anyone whose id does
              // not join: the plugin's user.userId is whatever the plugin
              // believes the account is, and when that is not the backend
              // users._id the row was filed under a key nothing resolves — even
              // when the very same event carried a perfectly good email. Email
              // also converges the two eras of stored events, since rows
              // written before the identity fix hold an email IN user.userId.
              identity: { $ifNull: ["$user.email", "$user.userId"] },
              // Preferred over the pseudonymous id, which flips between a
              // Figma-derived and a device-derived value inside one session.
              // Not stable across launches either — see the note in
              // pluginActivityDaysByIdentity and identityHealth.
              anonymous: { $ifNull: ["$deviceId", "$user.anonymousId"] },
              // Carried so the opening, pre-authentication events of a launch
              // can be resolved to whoever the session turned out to belong to.
              session: "$sessionId",
              tool: "$tool",
            },
            events: { $sum: 1 },
            // A job is a run that FINISHED. tool_action_completed is the only
            // event that says so, so it is the only one counted.
            //
            // feature_used is deliberately not counted and not substituted: it
            // marks that a feature was invoked, not that the work it started
            // ever finished, and a tool can emit several per run. Tallied only
            // so a tool that reports invocations but never reports a completion
            // can be named as an instrumentation gap rather than silently
            // reporting zero jobs.
            // Timestamps rather than a count, because whether each one counts
            // depends on when the user opened the tool.
            completionsAt: { $push: { $cond: [{ $eq: ["$eventType", "tool_action_completed"] }, "$eventAt", "$$REMOVE"] } },
            featureUses: { $sum: { $cond: [{ $eq: ["$eventType", "feature_used"] }, 1, 0] } },
            lastEventAt: { $max: "$eventAt" },
          },
        },
        { $sort: { lastEventAt: -1 } },
      ], aggregationOptions())
      .toArray();

    for (const row of rows) {
      const normalized = normalizeToolCode(row._id?.tool);
      // Navigation chrome is not a tool the user "used".
      if (NON_TOOL_SURFACES.has(String(row._id?.tool || "").trim().toLowerCase())) continue;
      if (NON_TOOL_SURFACES.has(normalized.key)) continue;

      const identity = row._id?.identity;
      const anonymous = anonymousKey(row._id?.anonymous);
      // See pluginActivityDaysByIdentity: the session resolves the head of a
      // launch, the anonymous id resolves a genuine signed-out-then-signed-up.
      const linked = identity ? null : resolveAnonymous(row._id?.session, anonymous, sessionLinks, identityLinks);
      const resolved = identity || linked || null;
      const bucket = !resolved ? "byAnonymous" : looksLikeEmail(resolved) ? "byEmail" : "byUserId";
      const rawKey = resolved || anonymous;
      if (!rawKey) continue;
      const key = looksLikeEmail(rawKey) ? String(rawKey).toLowerCase() : String(rawKey);

      const existing = top[bucket].get(key);
      // Rows arrive newest-first, so the first one for an identity is the tool
      // they most recently used.
      const lastEventAt = asDate(row.lastEventAt);
      if (!existing || (lastEventAt && lastEventAt > existing.lastEventAt)) {
        top[bucket].set(key, {
          key: normalized.key,
          label: normalized.label,
          events: asNumber(row.events),
          credited: false,
          lastEventAt: asDate(row.lastEventAt),
        });
      }

      // Job evidence is accumulated across every tool the identity used, not
      // just the top one — the column answers "how much work did this person
      // get done", which is not a per-tool question.
      const rawTool = String(row._id?.tool || "").trim().toLowerCase();
      const initiated = countUserInitiated(row.completionsAt || [], row._id?.session, rawTool, openIndex);
      backgroundCompletions += initiated.background;
      if (initiated.ungated) ungatedTools.add(normalized.key);
      const tally = counts[bucket].get(key) || { completedActions: 0, featureUses: 0 };
      tally.completedActions += initiated.jobs;
      tally.featureUses += asNumber(row.featureUses);
      counts[bucket].set(key, tally);
    }
  } catch (error) {
    // A ReferenceError here previously looked identical to an unreachable
    // database: the map came back empty and every caller treated that as
    // "this user has used nothing".
    available = false;
    console.error(
      error instanceof ReferenceError || error instanceof TypeError
        ? `Plugin tool-use aggregation has a bug: ${error?.stack || error}`
        : `Plugin tool-use aggregation failed: ${error?.message || error}`
    );
  }

  const jobs = { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map() };
  for (const bucket of ["byUserId", "byEmail", "byAnonymous"]) {
    for (const [key, tally] of counts[bucket]) {
      jobs[bucket].set(key, tally.completedActions);
    }
  }
  return { top, jobs, available, backgroundCompletions, ungatedTools: [...ungatedTools] };
}

/**
 * Whether plugin identities are stable enough to count people with.
 *
 * The read path assumed the device id was minted once per install. Production
 * shows otherwise: ids arrive as `device_<epoch-ms>_<suffix>` whose embedded
 * timestamp matches the first event of the launch that produced them, so the
 * plugin mints a new one every time it opens and never persists it. A device id
 * therefore identifies a LAUNCH, not a machine, and every launch by the same
 * signed-out person becomes another "visitor".
 *
 * Session stitching removes that error for anyone who signs in, which under a
 * hard login gate is nearly everyone. What remains is a real measurement limit
 * on genuinely signed-out traffic, and it should be stated rather than quietly
 * inflating a headcount. The ratio of distinct devices to distinct sessions is
 * the test: at one device per install it sits far below one, and at one device
 * per launch it sits at one.
 */
async function identityHealth(start) {
  try {
    const collection = await getEventsCollection();
    const [row] = await collection
      .aggregate([
        { $match: { eventAt: { $gte: start } } },
        {
          $group: {
            _id: null,
            devices: { $addToSet: "$deviceId" },
            sessions: { $addToSet: "$sessionId" },
            accounts: { $addToSet: "$user.userId" },
          },
        },
        {
          $project: {
            devices: { $size: "$devices" },
            sessions: { $size: "$sessions" },
            accounts: { $size: { $filter: { input: "$accounts", cond: { $ne: ["$$this", null] } } } },
          },
        },
      ], aggregationOptions())
      .toArray();
    if (!row || !row.sessions) return { status: "unavailable", message: "No plugin events in range to assess identity stability." };
    const devicesPerSession = Math.round((row.devices / row.sessions) * 100) / 100;
    // A shared install produces many sessions per device. One device per
    // session means the id is regenerated at launch and cannot identify anyone
    // across launches.
    const stable = devicesPerSession < 0.75;
    return {
      status: stable ? "stable" : "per_launch",
      devices: row.devices,
      sessions: row.sessions,
      accounts: row.accounts,
      devicesPerSession,
      message: stable
        ? "Plugin device ids persist across launches, so signed-out visitors are counted once each."
        : "The plugin mints a new device id on every launch, so it identifies a launch rather than a machine. Signed-out visitors who never sign in are counted once per launch and cannot be de-duplicated. Signed-in users are unaffected: their activity is stitched to the account through the session id. Fix by persisting the device id in figma.clientStorage.",
    };
  } catch (error) {
    return { status: "unavailable", message: `Identity stability could not be assessed: ${error?.message || error}` };
  }
}

async function telemetryHealth(now = new Date()) {
  try {
    const collection = await getEventsCollection();
    const match = { schemaVersion: { $gte: 1 }, eventType: { $in: SEMANTIC_EVENT_TYPES } };
    const [latest, earliest, activeDays] = await Promise.all([
      collection.findOne(match, { sort: { eventAt: -1 }, projection: { eventAt: 1 } }),
      collection.findOne(match, { sort: { eventAt: 1 }, projection: { eventAt: 1 } }),
      collection.aggregate([{ $match: { ...match, eventAt: { $gte: new Date(now.getTime() - 8 * DAY_MS) } } }, { $group: { _id: { $dateToString: { date: "$eventAt", format: "%Y-%m-%d", timezone: "UTC" } } } }], aggregationOptions()).toArray(),
    ]);
    const latestAt = asDate(latest?.eventAt); const earliestAt = asDate(earliest?.eventAt);
    // These messages describe FRESHNESS only. Tool and user metrics are now
    // computed from telemetry regardless of this status, so the wording no
    // longer claims anything is hidden — it used to say metrics were withheld
    // while they were in fact being displayed.
    if (!latestAt) return { status: "unavailable", latestAt: null, healthyDays: 0, message: "No plugin events have been received yet." };
    if (now - latestAt > 2 * 60 * 60 * 1000) return { status: "stale", latestAt, earliestAt, healthyDays: activeDays.length, message: "Plugin telemetry is still shown but the most recent event is over two hours old, so recent activity may be incomplete." };
    if (!earliestAt || now - earliestAt < 7 * DAY_MS || activeDays.length < 7) return { status: "warming_up", latestAt, earliestAt, healthyDays: activeDays.length, message: `Plugin telemetry is current with ${activeDays.length} of 7 days of coverage. Metrics are shown; week-over-week comparisons will firm up as coverage builds.` };
    return { status: "healthy", latestAt, earliestAt, healthyDays: activeDays.length, message: "Semantic plugin telemetry has at least seven days of verified coverage." };
  } catch {
    return { status: "unavailable", latestAt: null, healthyDays: 0, message: "Plugin behavior telemetry is not configured." };
  }
}

function pageOptions(query = {}) {
  const page = Math.max(1, Math.floor(asNumber(query.page, 1)));
  const requested = Math.floor(asNumber(query.pageSize, 25));
  return { page, pageSize: [25, 50, 100].includes(requested) ? requested : 25 };
}

/**
 * Short-lived cache for the expensive part of every page load.
 *
 * loadCore reads nine collections in full and runs four aggregations. At the
 * current event volume that is several seconds per request, and every panel on
 * a page triggers it independently — so the dashboard felt broken rather than
 * merely slow. A few seconds of staleness is a better trade than a page that
 * takes eight seconds to paint.
 *
 * Keyed on the databases being read as well as the window, so a different
 * deployment target or a test using its own server can never read another's
 * data. Cleared by closeDb so a connection teardown cannot leave a stale
 * handle behind it.
 */
const coreCache = new Map();

function coreCacheTtlMs() {
  const configured = Number(String(process.env.DASHBOARD_CORE_CACHE_MS || "").trim());
  return Number.isFinite(configured) && configured >= 0 ? configured : 30000;
}

export function resetCoreCache() {
  coreCache.clear();
}

// A connection teardown must not leave derived data from the old connection
// readable — tests in particular swap databases between assertions.
onDbClose(resetCoreCache);

function coreCacheKey(range) {
  return [
    process.env.MONGODB_URI || "",
    process.env.MONGODB_DB || "",
    process.env.BACKEND_MONGODB_URI || "",
    process.env.BACKEND_MONGODB_DB || "",
    range ? `${range.days}` : "norange",
  ].join("|");
}

/**
 * @param range The reporting period, when the caller has one. Plugin activity
 *   is then aggregated once per exact window so day bucketing never has to
 *   guess which period a boundary day belongs to, and the wide lookback still
 *   covers cohort math that reaches back further than any single window.
 */
async function loadCore(range = null) {
  const ttl = coreCacheTtlMs();
  const cacheKey = coreCacheKey(range);
  if (ttl > 0) {
    const hit = coreCache.get(cacheKey);
    // Date.now() is compared against the stored stamp rather than a timer, so
    // an idle process cannot serve something arbitrarily old.
    if (hit && Date.now() - hit.storedAt < ttl) return hit.value;
  }
  const value = await loadCoreUncached(range);
  if (ttl > 0) coreCache.set(cacheKey, { storedAt: Date.now(), value });
  return value;
}

async function loadCoreUncached(range = null) {
  const [users, billings, sessions, reservations, ledgers, purchases, subscriptions, refunds, starterGrants] = await Promise.all([
    (await getBackendUsersCollection()).find({}, { projection: { email: 1, name: 1, picture: 1, favorites: 1, earlyAccess: 1, createdAt: 1, updatedAt: 1 } }).toArray(),
    (await getBackendUserBillingCollection()).find({}, { projection: { user: 1, availableCredits: 1, heldCredits: 1, lifetimePurchasedCredits: 1, lifetimeBonusCredits: 1, lifetimeSpentCredits: 1, lifetimeRefundedCredits: 1, subscriptionStatus: 1, subscriptionPlanCode: 1, pricingTier: 1, pricingCountry: 1, updatedAt: 1 } }).toArray(),
    (await getBackendSessionsCollection()).find({}, { projection: { user: 1, source: 1, countryCode: 1, pricingTierAtAuth: 1, completed: 1, completedAt: 1, createdAt: 1 } }).toArray(),
    (await getBackendUsageReservationsCollection()).find({}, { projection: { user: 1, status: 1, toolCode: 1, featureCode: 1, creditsReserved: 1, processor: 1, createdAt: 1, updatedAt: 1, committedAt: 1, releasedAt: 1, expiresAt: 1 } }).toArray(),
    (await getBackendCreditLedgerCollection()).find({}, { projection: { user: 1, reservation: 1, reason: 1, deltaCredits: 1, toolCode: 1, featureCode: 1, createdAt: 1 } }).toArray(),
    (await getBackendPurchasesCollection()).find({}, { projection: { user: 1, productCode: 1, kind: 1, status: 1, amountPaise: 1, currency: 1, pricingTier: 1, capturedAt: 1, refundedAt: 1, refundedAmountPaise: 1, createdAt: 1, updatedAt: 1 } }).toArray(),
    (await getBackendSubscriptionsCollection()).find({}, { projection: { user: 1, planCode: 1, status: 1, amountSubunits: 1, pricingCurrency: 1, currentPeriodStart: 1, currentPeriodEnd: 1, nextChargeAt: 1, createdAt: 1, updatedAt: 1 } }).toArray(),
    (await getBackendRefundsCollection()).find({}, { projection: { user: 1, purchase: 1, amountPaise: 1, status: 1, reason: 1, createdAt: 1, updatedAt: 1 } }).toArray(),
    (await getBackendStarterGrantsCollection()).find({}, { projection: { user: 1, grantedCredits: 1, status: 1, riskScore: 1, decisionReason: 1, source: 1, grantedAt: 1, blockedAt: 1, createdAt: 1, updatedAt: 1 } }).toArray(),
  ]);
  // Aggregated in MongoDB rather than loaded as documents, so adding plugin
  // activity does not grow the amount of data pulled into memory here. The
  // lookback only has to cover the widest period any caller can request —
  // productLifecycle accepts an arbitrary `days` value, so it is configurable
  // rather than fixed, and the $match is served by the eventAt index.
  // An unset or non-numeric value must fall back to the default, not collapse
  // the window: Math.max(1, 0) would silently reduce the lookback to a day.
  const configuredLookback = Number(String(process.env.ANALYTICS_ACTIVITY_LOOKBACK_DAYS || "").trim());
  const baseLookback = Number.isFinite(configuredLookback) && configuredLookback > 0 ? configuredLookback : 400;
  const span = range ? range.days * 2 : 0;
  const lookbackDays = Math.max(baseLookback, Math.ceil(span) || 0);

  // Three indexes, each aggregated with an exact eventAt range.
  //
  // Windowing plugin days by comparing calendar-day STRINGS while sessions were
  // filtered by INSTANT put the two evidence sources on different clocks: the
  // whole calendar day containing a period boundary landed in one period for
  // plugin activity and the other for logins. Letting the aggregation's $match
  // do the windowing removes the mismatch entirely — every day in a window
  // index is in that window by construction, so no day-string filtering is
  // needed on read.
  // Resolved once and shared: every aggregation below must agree about which
  // pseudonymous ids belong to which accounts, or a user's history splits.
  const lookbackStart = new Date(Date.now() - lookbackDays * DAY_MS);
  const [identityLinks, sessionLinks] = await Promise.all([
    identityLinksByAnonymousId(),
    accountsBySessionId(lookbackStart),
  ]);
  const [pluginActivity, pluginActivityCurrent, pluginActivityPrevious, pluginToolUseLifetime, pluginToolUseCurrent, pluginToolUsePrevious] = await Promise.all([
    // Wide index: cohort retention measures each user's return relative to
    // their own signup date, so it needs history beyond any single window.
    pluginActivityDaysByIdentity(lookbackStart, new Date(), identityLinks, sessionLinks),
    range
      ? pluginActivityDaysByIdentity(range.currentStart, range.now, identityLinks, sessionLinks)
      : Promise.resolve(null),
    range
      ? pluginActivityDaysByIdentity(range.previousStart, range.currentStart, identityLinks, sessionLinks)
      : Promise.resolve(null),
    // Tool use is indexed over both spans for the same reason activity is. The
    // users table reports credited jobs over a user's whole history while the
    // top-tool column was window-scoped, so anyone whose last tool use predated
    // the selected range read as "No tool use recorded" beside a non-zero
    // lifetime job count — one row describing two different periods.
    pluginToolUseByIdentity(lookbackStart, new Date(), identityLinks, sessionLinks),
    range
      ? pluginToolUseByIdentity(range.currentStart, range.now, identityLinks, sessionLinks)
      : Promise.resolve(null),
    // The previous window needs its own index for the same reason activity
    // does: every "what changed" percentage is measured against it, so it must
    // be built on the same clock rather than reused from another span.
    range
      ? pluginToolUseByIdentity(range.previousStart, range.currentStart, identityLinks, sessionLinks)
      : Promise.resolve(null),
  ]);

  return {
    users, billings, sessions, reservations, ledgers, purchases, subscriptions, refunds, starterGrants,
    pluginActivity,
    pluginActivityCurrent: pluginActivityCurrent || pluginActivity,
    pluginActivityPrevious: pluginActivityPrevious || pluginActivity,
    pluginToolUse: pluginToolUseCurrent || pluginToolUseLifetime,
    pluginToolUsePrevious: pluginToolUsePrevious || pluginToolUseLifetime,
    pluginToolUseLifetime,
  };
}

const emptyIdentityMaps = () => ({ byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map() });

function indexCore(core) {
  const emptyToolUse = { top: emptyIdentityMaps(), jobs: emptyIdentityMaps() };
  const group = (rows, field = "user") => {
    const map = new Map();
    for (const row of rows) {
      const key = id(row[field]);
      if (!key) continue;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(row);
    }
    return map;
  };
  return {
    // userFacts reports on the CURRENT window, so it gets the current-window
    // index. The wide index is kept separately for cohort retention, which
    // measures each user against their own signup date rather than a window.
    pluginActivity: core.pluginActivityCurrent || core.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), namesByAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") },
    pluginTopTools: core.pluginToolUse?.top || emptyToolUse.top,
    pluginToolJobs: core.pluginToolUse?.jobs || emptyToolUse.jobs,
    // Lifetime tool use answers the two lifetime columns — latest tool and
    // total jobs — so they no longer disagree with each other about the period.
    pluginTopToolsLifetime: core.pluginToolUseLifetime?.top || core.pluginToolUse?.top || emptyToolUse.top,
    pluginToolJobsLifetime: core.pluginToolUseLifetime?.jobs || core.pluginToolUse?.jobs || emptyToolUse.jobs,
    pluginToolUseAvailable: (core.pluginToolUseLifetime?.available ?? true) && (core.pluginToolUse?.available ?? true),
    pluginActivityLifetime: core.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), namesByAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") },
    billing: new Map(core.billings.map((row) => [id(row.user), row])),
    sessions: group(core.sessions), reservations: group(core.reservations), ledgers: group(core.ledgers),
    purchases: group(core.purchases), subscriptions: group(core.subscriptions), refunds: group(core.refunds), grants: group(core.starterGrants),
    compensatedReservations: new Set(core.ledgers.filter((row) => row.reason === "compensation_credit").map((row) => id(row.reservation)).filter(Boolean)),
  };
}

function userFacts(user, indexed, range) {
  const userId = id(user._id);
  const sessions = (indexed.sessions.get(userId) || []).filter(successfulSession).sort((a, b) => loginAt(a) - loginAt(b));
  const reservations = (indexed.reservations.get(userId) || []).sort((a, b) => reservationAt(a) - reservationAt(b));
  const committed = reservations.filter((row) => row.status === "committed" && !indexed.compensatedReservations.has(id(row._id)));
  const purchases = indexed.purchases.get(userId) || [];
  const billing = indexed.billing.get(userId) || null;
  const subscriptions = indexed.subscriptions.get(userId) || [];
  const subscription = activeSubscription(subscriptions, range.now);
  const currentSessions = sessions.filter((row) => inRange(loginAt(row), range.currentStart, range.now));
  const currentCommitted = committed.filter((row) => inRange(reservationAt(row), range.currentStart, range.now));
  // Active days combine backend logins with plugin activity, bucketed in the
  // configured reporting timezone. Using logins alone made returns from the
  // plugin — which reuses a stored token and creates no new session — invisible.
  const activity = indexed.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), namesByAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") };
  const formatDay = activity.formatDay || dayKeyFormatter(activity.timezone);
  const distinctDays = new Set(currentSessions.map((row) => formatDay.format(loginAt(row))));
  // Both indexes must be merged, not chosen between: a user can have recent
  // events keyed by account id AND older ones keyed by email from before the
  // identity fix. Taking the first non-empty one silently discarded the other.
  const pluginDays = new Set([
    ...(activity.byUserId.get(userId) || []),
    ...(activity.byEmail.get(String(user.email || "").toLowerCase()) || []),
  ]);
  // Every day in this index is already inside the reporting window — the
  // aggregation matched on eventAt, the same instant-level test the session
  // filter uses. Re-filtering by day string here is what previously handed a
  // boundary day to the wrong period.
  const pluginActiveDays = pluginDays.size;
  for (const day of pluginDays) distinctDays.add(day);
  const lastLogin = sessions.at(-1) || null;
  const firstCommitted = committed[0] || null;
  const captured = purchases.filter((row) => row.status === "captured");
  const lastLoginDate = lastLogin ? loginAt(lastLogin) : null;
  // Dormancy is measured from the last sign of life, not the last login. A user
  // who opens the plugin daily creates no new session document, so measuring
  // from logins alone labelled active daily users "at_risk" and then "dormant".
  //
  // This must read the LIFETIME index, not the window-scoped one. lastLoginDate
  // above is unfiltered, so pairing it with a window-truncated plugin date made
  // "last activity" depend on the selected range: narrowing the users page to
  // 7 days pushed a plugin-active user past the 14-day at_risk and 30-day
  // dormant thresholds purely because their activity fell outside the window.
  // False when the analytics store could not be read. Verdicts that depend on
  // plugin evidence must stay silent rather than assert a conclusion from data
  // that was never retrieved.
  // The tool-use aggregation is part of the same evidence: if it failed, a zero
  // job count is "not measured", not "did nothing", and the table says so.
  const activityAvailable = activity.available !== false
    && (indexed.pluginActivityLifetime?.available !== false)
    && indexed.pluginToolUseAvailable !== false;
  const lifetime = indexed.pluginActivityLifetime || activity;
  const lifetimePluginDays = new Set([
    ...(lifetime.byUserId.get(userId) || []),
    ...(lifetime.byEmail.get(String(user.email || "").toLowerCase()) || []),
  ]);
  const latestPluginDay = [...lifetimePluginDays].sort().at(-1) || null;
  const latestPluginDate = latestPluginDay ? new Date(`${latestPluginDay}T23:59:59Z`) : null;
  const lastActivityDate = [lastLoginDate, latestPluginDate]
    .filter(Boolean)
    .sort((a, b) => a - b)
    .at(-1) || null;
  const ageSinceLogin = lastActivityDate ? Math.max(0, Math.floor((range.now - lastActivityDate) / DAY_MS)) : null;
  const segments = [];
  if (inRange(user.createdAt, range.currentStart, range.now)) segments.push("new");
  // Activation means the person used a tool, whether or not it charged them.
  // Keying it on credited work alone reported anyone who only used credit-free
  // tools as never activated — and most tools do real work without charging.
  // A tool that charges nothing produces no reservation, so it can never win
  // the credited ranking above. Falling back to observed activity means the
  // column shows what the person actually used rather than implying they used
  // nothing. The `credited` flag lets the UI say which kind of evidence it is.
  //
  // Read from the LIFETIME index. The credited columns beside this one are
  // lifetime totals, so resolving the observed ones against the selected window
  // made a single row describe two different periods: a user last seen in the
  // plugin five weeks ago showed a non-zero lifetime job count next to "No tool
  // use recorded". The in-range figures are resolved separately below.
  const emailKey = String(user.email || "").toLowerCase();
  // The two indexes hold DIFFERENT events, never the same one twice: each row
  // is filed under its email when it has one and under its account id
  // otherwise. So jobs are summed across both, while the top tool is whichever
  // of the two happened later.
  const sumIdentity = (maps) =>
    asNumber(maps?.byUserId.get(userId), 0) + (emailKey ? asNumber(maps?.byEmail.get(emailKey), 0) : 0);
  const latestIdentity = (maps) => {
    const candidates = [maps?.byUserId.get(userId), emailKey ? maps?.byEmail.get(emailKey) : null].filter(Boolean);
    return candidates.sort((a, b) => asDate(a.lastEventAt) - asDate(b.lastEventAt)).at(-1) || null;
  };
  const observedTopTool = latestIdentity(indexed.pluginTopToolsLifetime) || latestIdentity(indexed.pluginTopTools);
  // Tool jobs the plugin observed — runs that actually finished, whether or not
  // a reservation recorded them. Kept apart from the credited count rather than
  // added to it: a credited run emits both a committed reservation and a
  // completed tool action, so summing those two would report one job as two.
  const observedJobs = sumIdentity(indexed.pluginToolJobsLifetime);
  const observedJobsInRange = sumIdentity(indexed.pluginToolJobs);
  const activated = Boolean(firstCommitted || observedTopTool);
  if (!activated) segments.push("not_activated");
  if (activated) segments.push("activated");
  // Engagement is about how much work someone got done, not how much of it
  // billed. Counting credited jobs alone left heavy users of a tool whose runs
  // are only visible in telemetry out of the segment entirely.
  if (Math.max(currentCommitted.length, observedJobsInRange) >= 3) segments.push("engaged");
  if (distinctDays.size >= 2) segments.push("returning");
  if (billing && asNumber(billing.availableCredits) <= lowCreditThreshold()) segments.push("low_credit");
  // at_risk and dormant are claims that a user has STOPPED, and plugin activity
  // is the evidence that would disprove them. When that index could not be read
  // we do not know, so we do not assert: an analytics outage would otherwise
  // convert a daily plugin user into a churn-risk list entry, which is the same
  // "absence never measured" mistake the availability flag exists to prevent.
  if (activityAvailable) {
    if (firstCommitted && (ageSinceLogin === null || ageSinceLogin >= 14)) segments.push("at_risk");
    if (ageSinceLogin === null || ageSinceLogin >= 30) segments.push("dormant");
  }
  if (purchases.some((row) => ["pending", "failed"].includes(row.status))) segments.push("payment_attention");
  const tools = new Map();
  for (const reservation of committed) {
    const tool = normalizeToolCode(reservation.toolCode, reservation.featureCode);
    const current = tools.get(tool.key) || { key: tool.key, label: tool.label, completed: 0, credits: 0, latestAt: null };
    current.completed += 1;
    current.credits += asNumber(reservation.creditsReserved);
    const occurredAt = reservationAt(reservation);
    if (!current.latestAt || occurredAt > asDate(current.latestAt)) current.latestAt = occurredAt;
    tools.set(tool.key, current);
  }
  // The most recently used tool that charged credits.
  const creditedTopTool = [...tools.values()].sort((a, b) => asDate(b.latestAt) - asDate(a.latestAt))[0] || null;
  // Whichever the person touched last, regardless of whether it billed them.
  // Ranking credited work first meant a single old paid job outranked a tool
  // used minutes ago, so the column answered "what did they last pay for"
  // rather than "what are they using".
  const creditedAt = creditedTopTool ? asDate(creditedTopTool.latestAt) : null;
  const observedAt = observedTopTool ? asDate(observedTopTool.lastEventAt) : null;
  // When the person was last seen doing anything — signing in, running a
  // credited job, or using any tool. Login alone cannot answer this: the plugin
  // authenticates from a stored token and creates no new session, so a daily
  // user's "last login" can sit weeks in the past while they are active now.
  const lastActiveAt = [lastLoginDate, creditedAt, observedAt]
    .filter((value) => value instanceof Date && !Number.isNaN(value.getTime()))
    .sort((a, b) => a - b)
    .at(-1) || null;
  const lastActiveSource =
    lastActiveAt === observedAt && observedAt ? "plugin"
      : lastActiveAt === creditedAt && creditedAt ? "tool job"
        : lastActiveAt ? "login" : null;
  const topTool =
    creditedTopTool && (!observedAt || (creditedAt && creditedAt >= observedAt))
      ? { ...creditedTopTool, credited: true, lastUsedAt: creditedAt }
      : observedTopTool
        ? { ...observedTopTool, lastUsedAt: observedAt }
        : null;
  return { userId, billing, subscriptions, subscription, sessions, reservations, committed, purchases, captured, currentSessions, currentCommitted, distinctDays, pluginActiveDays, activityAvailable, activated, lastActiveAt, lastActiveSource, lastLogin, lastLoginDate, firstCommitted, segments, topTool, observedJobs, observedJobsInRange };
}

/**
 * @param activityIndex Plugin activity aggregated with this period's exact
 *   eventAt bounds. Passing the right index per period is what keeps a boundary
 *   day from being claimed by both — sessions are filtered by instant, so
 *   windowing plugin days by calendar-day string put the two evidence sources
 *   on different clocks and corrupted the previous-period baseline that every
 *   "what changed" percentage is measured against.
 */
/**
 * Where a user sits in the lifecycle.
 *
 * Commercial standing is deliberately NOT read from Purchase.status alone. A
 * subscription Purchase is written as "pending" at checkout initiation and, due
 * to a defect in the billing app, never advances to "captured" — so a paying
 * subscriber was rendered byte-for-byte identically to someone who had never
 * opened a checkout, and was excluded from every stage-based filter and count.
 * The wallet's own subscription state and lifetime purchased credits are
 * already loaded here and are authoritative about whether money changed hands.
 *
 * "pending" means checkout was STARTED, not paid, so it earns its own rung
 * rather than being promoted to customer.
 */
function resolveLifecycleStage(facts, billing) {
  const subscriptionStatus = String(billing?.subscriptionStatus || "");
  const hasPaidSubscription = ["active", "cancel_scheduled"].includes(subscriptionStatus);
  const hasPurchasedCredits = asNumber(billing?.lifetimePurchasedCredits) > 0;
  // facts.subscription is the subscriptions collection itself, which is where a
  // subscription granted from the backend actually lands. Without it, a comped
  // or hand-granted subscriber was invisible to every stage-based count.
  if (facts.captured.length || hasPaidSubscription || hasPurchasedCredits || facts.subscription) return "customer";
  if (facts.activityAvailable && facts.distinctDays.size >= 2) return "returning";
  if (facts.activated) return "activated";
  // Reached checkout but no money is confirmed. Distinct from a user who never
  // tried, which is the distinction that matters when triaging a failed payment.
  const startedCheckout =
    subscriptionStatus === "payment_pending" ||
    facts.purchases.some((row) => ["created", "pending"].includes(row.status));
  if (startedCheckout) return "checkout_started";
  if (facts.lastLogin) return "logged_in";
  return "signed_up";
}

function periodSummary(core, start, end, activityIndex = null, jobsIndex = null) {
  const sessions = core.sessions.filter(successfulSession).filter((row) => inRange(loginAt(row), start, end));
  const compensated = new Set(core.ledgers.filter((row) => row.reason === "compensation_credit").map((row) => id(row.reservation)).filter(Boolean));
  const reservations = core.reservations.filter((row) => row.status === "committed" && !compensated.has(id(row._id)) && inRange(reservationAt(row), start, end));
  const newUsers = core.users.filter((row) => inRange(row.createdAt, start, end));
  // Active days per user, from logins and plugin activity alike, bucketed in
  // the reporting timezone. Counting logins only made plugin-only returns
  // invisible, which is why the dashboard reported no returning users.
  const activity = activityIndex || core.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), namesByAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") };
  const formatDay = activity.formatDay || dayKeyFormatter(activity.timezone);
  const daysByUser = new Map();
  const addDay = (key, day) => {
    if (!key) return;
    if (!daysByUser.has(key)) daysByUser.set(key, new Set());
    daysByUser.get(key).add(day);
  };
  for (const session of sessions) {
    addDay(id(session.user), formatDay.format(loginAt(session)));
  }
  const emailToUserId = new Map(
    core.users.map((row) => [String(row.email || "").toLowerCase(), id(row._id)]).filter(([email]) => email)
  );
  // The index handed in was aggregated with this period's exact eventAt
  // bounds, so every day it contains belongs to this period and no day-string
  // window test is needed — that test is what split a boundary day across both
  // periods while logins, filtered by instant, went to only one.
  const mergePluginDays = (key, days) => {
    if (!key || !days) return;
    for (const day of days) addDay(key, day);
  };
  // Only identities that resolve to a real account are merged. An events-only
  // identity that matches no user would otherwise invent a person who does not
  // exist in the users collection and inflate the active-user count.
  const knownUserIds = new Set(core.users.map((row) => id(row._id)));
  for (const [userId, days] of activity.byUserId) {
    if (knownUserIds.has(userId)) mergePluginDays(userId, days);
  }
  // Events identified only by email — including historical rows written before
  // the identity fix — resolve to a real account here rather than being lost.
  for (const [email, days] of activity.byEmail) mergePluginDays(emailToUserId.get(email), days);
  // Accounts with at least one active day in the window, whether that day came
  // from a login or from using the plugin. Anonymous plugin visitors are
  // deliberately excluded here because this metric counts known users.
  const activeUsers = daysByUser.size;
  // Signed-out visitors, counted by pseudonymous id. Reported separately rather
  // than folded into activeUsers: that metric means "known accounts", and
  // merging the two would make neither number answerable.
  const anonymousDayCounts = [...(activity.byAnonymous ? activity.byAnonymous.values() : [])];
  const anonymousVisitors = anonymousDayCounts.length;
  const returningAnonymousVisitors = anonymousDayCounts.filter((days) => days.size >= 2).length;
  const firstCommitByUser = new Map();
  for (const row of core.reservations.filter((item) => item.status === "committed" && !compensated.has(id(item._id)))) {
    const key = id(row.user); const occurredAt = reservationAt(row);
    if (!firstCommitByUser.has(key) || occurredAt < firstCommitByUser.get(key)) firstCommitByUser.set(key, occurredAt);
  }
  // Activation is the first time a person got work done, not the first time
  // they were billed for it. The users page has counted any tool use as
  // activation since credit-free tools were measured; this metric had not
  // caught up, so the summary and the table disagreed about the same word.
  //
  // Plugin evidence resolves to a calendar day rather than an instant, so a
  // first activity is dated to the start of its day. That is only ever used to
  // decide which window it falls in.
  const lifetimeActivity = core.pluginActivity || activity;
  const firstActivityByUser = new Map(firstCommitByUser);
  const noteFirstActivity = (key, days) => {
    if (!key || !days?.size) return;
    const earliest = [...days].sort()[0];
    if (!earliest) return;
    const date = new Date(`${earliest}T00:00:00Z`);
    const known = firstActivityByUser.get(key);
    if (!known || date < known) firstActivityByUser.set(key, date);
  };
  for (const [userId, days] of lifetimeActivity.byUserId) {
    if (knownUserIds.has(userId)) noteFirstActivity(userId, days);
  }
  for (const [email, days] of lifetimeActivity.byEmail) noteFirstActivity(emailToUserId.get(email), days);
  const activated = [...firstActivityByUser.values()].filter((value) => value >= start && value < end).length;
  // Completed work in this window, from both records of it.
  //
  // Committed reservations are the billing record; completed tool actions are
  // what the plugin saw. Per account the two describe the SAME runs once a tool
  // charges credits, so the larger is taken and never the sum. Accounts that
  // appear in only one of them still contribute — which is the whole point:
  // this metric used to be committed reservations alone and so missed every job
  // whose reservation never landed.
  const jobs = jobsIndex || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map() };
  const creditedByUser = new Map();
  let unlinkedCreditedJobs = 0;
  for (const row of reservations) {
    const key = id(row.user);
    // A reservation with no user link is still a job that happened. It cannot
    // be de-duplicated against telemetry, so it is added on its own.
    if (!key) { unlinkedCreditedJobs += 1; continue; }
    creditedByUser.set(key, (creditedByUser.get(key) || 0) + 1);
  }
  const observedByUser = new Map();
  const addObserved = (key, count) => {
    if (!key || !count) return;
    observedByUser.set(key, (observedByUser.get(key) || 0) + count);
  };
  for (const [userId, count] of jobs.byUserId) {
    if (knownUserIds.has(userId)) addObserved(userId, count);
  }
  for (const [email, count] of jobs.byEmail) addObserved(emailToUserId.get(email), count);
  let completedJobs = unlinkedCreditedJobs;
  for (const key of new Set([...creditedByUser.keys(), ...observedByUser.keys()])) {
    completedJobs += Math.max(creditedByUser.get(key) || 0, observedByUser.get(key) || 0);
  }
  // Work done by someone who never signed in. Counted here because the metric
  // is "jobs completed", not "jobs completed by known accounts" — that
  // distinction is carried by activeUsers/anonymousVisitors instead.
  const anonymousJobs = [...(jobs.byAnonymous ? jobs.byAnonymous.values() : [])].reduce((sum, count) => sum + count, 0);
  completedJobs += anonymousJobs;
  const captured = core.purchases.filter((row) => row.status === "captured" && inRange(row.capturedAt || row.updatedAt || row.createdAt, start, end));
  const refunds = core.refunds.filter((row) => row.status === "processed" && inRange(row.updatedAt || row.createdAt, start, end));
  return {
    newUsers: newUsers.length,
    activeUsers,
    activatedUsers: activated,
    returningUsers: [...daysByUser.values()].filter((days) => days.size >= 2).length,
    anonymousVisitors,
    returningAnonymousVisitors,
    completedJobs,
    creditsConsumed: reservations.reduce((sum, row) => sum + asNumber(row.creditsReserved), 0),
    grossRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0),
    refundsPaise: refunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0),
  };
}

export async function productSummary(days = 30) {
  const range = period(days); const core = await loadCore(range);
  const current = periodSummary(core, range.currentStart, range.now, core.pluginActivityCurrent, core.pluginToolUse?.jobs);
  // The previous window ends where the current one begins, so the boundary day
  // belongs to the current period only.
  const previous = periodSummary(core, range.previousStart, range.currentStart, core.pluginActivityPrevious, core.pluginToolUsePrevious?.jobs);
  const metrics = Object.fromEntries(Object.entries(current).map(([key, value]) => [key, { value, previous: previous[key], change: change(value, previous[key]) }]));
  metrics.netRevenuePaise = { value: current.grossRevenuePaise - current.refundsPaise, previous: previous.grossRevenuePaise - previous.refundsPaise, change: change(current.grossRevenuePaise - current.refundsPaise, previous.grossRevenuePaise - previous.refundsPaise) };
  const missingWallets = core.users.length - core.billings.length;
  const expired = core.reservations.filter((row) => row.status === "expired" && inRange(row.updatedAt || row.expiresAt, range.currentStart, range.now)).length;
  const pendingPayments = core.purchases.filter((row) => ["pending", "failed"].includes(row.status)).length;
  const unattributed = core.reservations.filter((row) => terminalReservation(row) && !row.toolCode && !row.featureCode).length;
  // Money may have moved without anything being delivered. A purchase sits at
  // created/pending from checkout initiation until the provider confirms it, so
  // one that is still pending an hour later is either an abandoned checkout or
  // a payment that settled and never got recorded — and the two are
  // indistinguishable here, which is exactly why it needs surfacing rather than
  // silently ageing.
  const stalledCheckouts = core.purchases.filter(
    (row) => ["created", "pending"].includes(row.status) &&
      range.now - asDate(row.createdAt) > 60 * 60 * 1000
  );
  const capturedPurchaseIds = new Set(core.purchases.filter((row) => row.status === "captured").map((row) => id(row._id)));
  const unmatchedRefunds = core.refunds.filter((row) => row.status === "processed" && !capturedPurchaseIds.has(id(row.purchase))).length;
  const telemetry = await telemetryHealth(range.now);
  const attention = [
    missingWallets ? { severity: "warning", title: `${missingWallets} users have no billing wallet`, detail: "Their balance cannot be shown until the wallet is initialized.", href: "/users.html?wallet=missing" } : null,
    expired ? { severity: "warning", title: `${expired} tool jobs expired`, detail: "Review the affected tools and processing flow.", href: "/tools.html?status=expired" } : null,
    pendingPayments ? { severity: "warning", title: `${pendingPayments} payment attempts need context`, detail: "Pending and failed attempts are not counted as revenue.", href: "/credits.html" } : null,
    unattributed ? { severity: "info", title: `${unattributed} terminal jobs are unattributed`, detail: "They remain visible but are not assigned to a product tool.", href: "/data-health.html" } : null,
    stalledCheckouts.length ? { severity: "critical", title: `${stalledCheckouts.length} checkouts have not settled`, detail: "Started over an hour ago and still not captured. If money left the customer's account, they have paid and received nothing.", href: "/credits.html" } : null,
    unmatchedRefunds ? { severity: "warning", title: `${unmatchedRefunds} processed refunds lack a captured purchase match`, detail: "They are excluded from current revenue until the commercial records are reconciled.", href: "/data-health.html" } : null,
    // Freshness, not an outage. Tool and user metrics are computed from
    // telemetry regardless of this status, so raising it as critical put a
    // permanent red alert on a dashboard whose data was fine — and a status
    // that is always on is a status nobody reads.
    telemetry.status === "unavailable"
      ? { severity: "warning", title: "No plugin activity has been received", detail: telemetry.message, href: "/data-health.html" }
      : telemetry.status === "stale"
        ? { severity: "info", title: "Plugin activity is quiet", detail: telemetry.message, href: "/data-health.html" }
        : null,
  ].filter(Boolean);
  const changed = Object.entries(metrics).filter(([key]) => !["grossRevenuePaise", "refundsPaise"].includes(key)).sort((a, b) => Math.abs(b[1].change) - Math.abs(a[1].change)).slice(0, 3).map(([key, value]) => ({ metric: key, direction: value.change > 0 ? "up" : value.change < 0 ? "down" : "flat", change: value.change, current: value.value, previous: value.previous }));
  return { asOf: range.now, period: { days: range.days, currentStart: range.currentStart, previousStart: range.previousStart }, coverage: { activation: "Credited tool activation only", telemetry }, metrics, whatChanged: changed, needsAttention: attention };
}

/**
 * Signed-out visitors, as rows rather than a single number.
 *
 * They outnumber signed-in users several times over, and reducing them to a
 * headcount meant the majority of the active population could not be inspected,
 * filtered or attributed to a tool. They have a stable pseudonymous id, tool
 * usage and active days — everything a row needs except a name.
 *
 * Anyone who later signed up is excluded: their history has already been
 * stitched onto the account, and listing them here too would count one person
 * twice.
 */
/**
 * Which record a job count came from.
 *
 * The larger of the two is taken, never the sum: once a tool charges credits a
 * single run emits both a committed reservation and a completed tool action, so
 * adding them would report one job as two. The larger is the best available
 * lower bound on how many distinct runs there were, and preferring the credited
 * count outright under-reported anyone whose billing lagged their work.
 */
function jobsSource(credited, observed) {
  if (!credited && !observed) return null;
  if (!credited) return "observed";
  if (observed > credited) return "combined";
  return "credited";
}

function anonymousVisitorRows(indexed, range) {
  const activity = indexed.pluginActivity;
  const lifetime = indexed.pluginActivityLifetime || activity;
  const topTools = indexed.pluginTopToolsLifetime || indexed.pluginTopTools || { byAnonymous: new Map() };
  const jobsLifetime = indexed.pluginToolJobsLifetime || { byAnonymous: new Map() };
  const jobsInWindow = indexed.pluginToolJobs || { byAnonymous: new Map() };
  const rows = [];

  for (const [anonymousId, days] of activity.byAnonymous) {
    if (!anonymousId || !days?.size) continue;
    const topTool = topTools.byAnonymous.get(anonymousId) || null;
    const displayName = activity.namesByAnonymous?.get(anonymousId) || null;
    const lifetimeDays = lifetime.byAnonymous?.get(anonymousId) || days;
    const lastActiveAt = topTool ? asDate(topTool.lastEventAt) : null;
    const firstSeenDay = [...lifetimeDays].sort()[0] || null;

    rows.push({
      id: anonymousId,
      anonymous: true,
      name: displayName,
      email: null,
      // Day-resolution: an anonymous visitor has no account record to date.
      joinedAt: firstSeenDay ? new Date(`${firstSeenDay}T00:00:00Z`) : null,
      segments: days.size >= 2 ? ["anonymous", "returning"] : ["anonymous"],
      lifecycleStage: "anonymous",
      lastLoginAt: null,
      lastActiveAt,
      lastActiveSource: "plugin",
      latestLoginSource: null,
      country: null,
      successfulLogins: 0,
      pluginActiveDays: days.size,
      activeDaysInRange: days.size,
      pluginActivityAvailable: activity.available !== false,
      // A signed-out visitor has no wallet and so can never hold a reservation,
      // but they still finish tool jobs. Reporting a hard 0 here said they did
      // nothing; the observed count says what they actually did.
      creditedJobs: 0,
      creditedJobsInRange: 0,
      observedJobs: asNumber(jobsLifetime.byAnonymous?.get(anonymousId), 0),
      observedJobsInRange: asNumber(jobsInWindow.byAnonymous?.get(anonymousId), 0),
      completedJobs: asNumber(jobsLifetime.byAnonymous?.get(anonymousId), 0),
      completedJobsSource: jobsLifetime.byAnonymous?.get(anonymousId) ? "observed" : null,
      jobsInRange: asNumber(jobsInWindow.byAnonymous?.get(anonymousId), 0),
      topTool,
      toolKeys: topTool ? [topTool.key] : [],
      walletStatus: "not_applicable",
      availableCredits: null,
      heldCredits: null,
      subscriptionStatus: null,
      subscriptionPlan: null,
      newsletter: null,
    });
  }
  return rows;
}

export async function productUsers(query = {}, newsletterByEmail = new Map()) {
  const range = period(query.days || 30); const core = await loadCore(range); const indexed = indexCore(core);
  const search = String(query.search || "").trim().toLowerCase(); const segment = String(query.segment || "all");
  const country = String(query.country || "all"); const source = String(query.source || "all"); const tool = String(query.tool || "all");
  const wallet = String(query.wallet || "all"); const subscription = String(query.subscription || "all"); const newsletter = String(query.newsletter || "all");
  let rows = core.users.map((user) => {
    const facts = userFacts(user, indexed, range); const billing = facts.billing;
    const latestSource = facts.lastLogin?.source || null;
    const latestCountry = facts.lastLogin?.countryCode || billing?.pricingCountry || null;
    const newsletterProfile = newsletterByEmail.get(String(user.email || "").toLowerCase()) || null;
    // Always present, on both kinds of row. Visitor rows set it and account
    // rows omitted it, so a consumer testing `=== false` saw neither.
    return { id: facts.userId, anonymous: false, name: user.name || null, email: user.email || null, picture: user.picture || null, joinedAt: user.createdAt || null, segments: facts.segments, lifecycleStage: resolveLifecycleStage(facts, billing), lastLoginAt: facts.lastLoginDate, lastActiveAt: facts.lastActiveAt, lastActiveSource: facts.lastActiveSource, latestLoginSource: latestSource, country: latestCountry, successfulLogins: facts.sessions.length, pluginActiveDays: facts.activityAvailable ? facts.pluginActiveDays : null, activeDaysInRange: facts.activityAvailable ? facts.distinctDays.size : null, pluginActivityAvailable: facts.activityAvailable, creditedJobs: facts.committed.length, creditedJobsInRange: facts.currentCommitted.length, observedJobs: facts.observedJobs, observedJobsInRange: facts.observedJobsInRange, completedJobs: Math.max(facts.committed.length, facts.observedJobs), completedJobsSource: jobsSource(facts.committed.length, facts.observedJobs), jobsInRange: Math.max(facts.currentCommitted.length, facts.observedJobsInRange), topTool: facts.topTool, toolKeys: [...new Set([...facts.committed.map((row) => normalizeToolCode(row.toolCode, row.featureCode).key), ...(facts.topTool ? [facts.topTool.key] : [])])], walletStatus: billing ? "initialized" : "missing", availableCredits: billing ? asNumber(billing.availableCredits) : null, heldCredits: billing ? asNumber(billing.heldCredits) : null, subscriptionStatus: billing?.subscriptionStatus || facts.subscription?.status || null, subscriptionPlan: billing?.subscriptionPlanCode || facts.subscription?.planCode || null, subscriptionSource: billing?.subscriptionStatus ? "wallet" : facts.subscription ? "subscription_record" : null, subscriptionEndsAt: facts.subscription?.currentPeriodEnd || null, newsletter: newsletterProfile ? { id: newsletterProfile.id, status: newsletterProfile.status, tags: newsletterProfile.tags || [] } : null };
  });
  // Signed-out visitors join the same list so they sort, filter and paginate
  // alongside accounts instead of existing only as a headcount — and they join
  // BEFORE the facets are built, so a tool only they used is still offered in
  // the tool filter rather than being unreachable.
  const includeAnonymous = String(query.identity || "all") !== "accounts";
  if (includeAnonymous) rows = rows.concat(anonymousVisitorRows(indexed, range));
  const unique = (values) => [...new Set(values.filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b)));
  const facets = {
    countries: unique(rows.map((row) => row.country)),
    sources: unique(rows.map((row) => row.latestLoginSource)),
    tools: unique(rows.flatMap((row) => row.toolKeys)).map((key) => ({ key, label: normalizeToolCode(key).label })),
    subscriptions: unique(rows.map((row) => row.subscriptionStatus)),
    newsletterStatuses: unique(rows.map((row) => row.newsletter?.status || "not_subscribed")),
  };
  rows = rows.filter((row) => {
    if (search && !`${row.name || ""} ${row.email || ""}`.toLowerCase().includes(search)) return false;
    if (segment !== "all" && !row.segments.includes(segment)) return false;
    if (country !== "all" && String(row.country || "unknown") !== country) return false;
    if (source !== "all" && String(row.latestLoginSource || "unknown") !== source) return false;
    if (tool !== "all" && !row.toolKeys.includes(tool)) return false;
    if (wallet !== "all" && row.walletStatus !== wallet) return false;
    if (subscription !== "all" && String(row.subscriptionStatus || "inactive") !== subscription) return false;
    if (newsletter !== "all" && String(row.newsletter?.status || "not_subscribed") !== newsletter) return false;
    return true;
  });
  const sorters = { recent: (a, b) => (asDate(b.lastActiveAt) || asDate(b.lastLoginAt) || 0) - (asDate(a.lastActiveAt) || asDate(a.lastLoginAt) || 0), joined: (a, b) => (asDate(b.joinedAt) || 0) - (asDate(a.joinedAt) || 0), jobs: (a, b) => b.completedJobs - a.completedJobs, credits: (a, b) => asNumber(a.availableCredits, -1) - asNumber(b.availableCredits, -1) };
  rows.sort(sorters[String(query.sort || "recent")] || sorters.recent);
  const { page, pageSize } = pageOptions(query); const total = rows.length; const offset = (page - 1) * pageSize;
  const segmentCounts = {}; for (const row of rows) for (const item of row.segments) segmentCounts[item] = (segmentCounts[item] || 0) + 1;
  return { asOf: range.now, coverage: { behavior: "Successful authentication, credited tool activity, and tool jobs observed in plugin telemetry", newsletter: newsletterByEmail.size ? "connected" : "unavailable" }, summary: { users: total, segmentCounts }, facets, items: rows.slice(offset, offset + pageSize), pagination: { page, pageSize, total, pages: Math.max(1, Math.ceil(total / pageSize)) } };
}

export async function productUserDetail(userId) {
  if (!ObjectId.isValid(String(userId))) return null;
  // The range is built first so loadCore can aggregate plugin activity for this
  // exact window. Calling it without a range fell back to the wide lookback, so
  // the profile reported lifetime plugin days as if they were the last 30.
  const detailRange = period(30);
  const core = await loadCore(detailRange); const indexed = indexCore(core); const user = core.users.find((row) => id(row._id) === String(userId)); if (!user) return null;
  const range = detailRange; const facts = userFacts(user, indexed, range); const billing = facts.billing;
  const [legacyFeedback, feedback, requests] = await Promise.all([
    (await getBackendFeedbackCollection()).find({ $or: [{ authId: String(userId) }, { userId: new ObjectId(String(userId)) }] }, { projection: { feedbackType: 1, score: 1, toolId: 1, createdAt: 1 } }).sort({ createdAt: -1 }).limit(30).toArray(),
    (await getBackendFeedbacksCollection()).find({ userId: new ObjectId(String(userId)) }, { projection: { rating: 1, comment: 1, path: 1, isAnonymous: 1, createdAt: 1 } }).sort({ createdAt: -1 }).limit(30).toArray(),
    (await getBackendFeatureRequestsCollection()).find({ authorId: String(userId), isDeleted: { $ne: true } }, { projection: { title: 1, status: 1, board: 1, votes: 1, commentsCount: 1, createdAt: 1 } }).sort({ createdAt: -1 }).limit(30).toArray(),
  ]);
  const safePurchase = (row) => ({ id: id(row._id), kind: row.kind, productCode: row.productCode, status: row.status, amount: asNumber(row.amountPaise), currency: row.currency || "INR", createdAt: row.createdAt });
  return { asOf: range.now, user: { id: facts.userId, name: user.name || null, email: user.email || null, picture: user.picture || null, joinedAt: user.createdAt, favorites: user.favorites || [], segments: facts.segments, lifecycleStage: resolveLifecycleStage(facts, billing), creditedJobs: facts.committed.length, observedJobs: facts.observedJobs, completedJobs: Math.max(facts.committed.length, facts.observedJobs), completedJobsSource: jobsSource(facts.committed.length, facts.observedJobs), topTool: facts.topTool, subscription: facts.subscription ? { planCode: facts.subscription.planCode || null, status: facts.subscription.status || null, currentPeriodEnd: facts.subscription.currentPeriodEnd || null, walletBacked: Boolean(billing) } : null }, billing: billing ? { availableCredits: asNumber(billing.availableCredits), heldCredits: asNumber(billing.heldCredits), lifetimeSpentCredits: asNumber(billing.lifetimeSpentCredits), lifetimePurchasedCredits: asNumber(billing.lifetimePurchasedCredits), lifetimeBonusCredits: asNumber(billing.lifetimeBonusCredits), subscriptionStatus: billing.subscriptionStatus || facts.subscription?.status || null, subscriptionPlan: billing.subscriptionPlanCode || facts.subscription?.planCode || null, pricingTier: billing.pricingTier, pricingCountry: billing.pricingCountry } : null, sessions: facts.sessions.slice(-50).reverse().map((row) => ({ source: row.source || "unknown", country: row.countryCode || null, completedAt: loginAt(row) })), reservations: facts.reservations.slice(-100).reverse().map((row) => ({ id: id(row._id), ...normalizeToolCode(row.toolCode, row.featureCode), rawToolCode: row.toolCode || null, featureCode: row.featureCode || null, status: indexed.compensatedReservations.has(id(row._id)) ? "compensated" : row.status, credits: asNumber(row.creditsReserved), processor: row.processor || null, occurredAt: reservationAt(row), durationMs: row.status === "committed" && row.committedAt && row.createdAt ? asDate(row.committedAt) - asDate(row.createdAt) : null })), ledger: (indexed.ledgers.get(facts.userId) || []).slice().sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt)).slice(0, 100).map((row) => ({ reason: row.reason, deltaCredits: asNumber(row.deltaCredits), tool: normalizeToolCode(row.toolCode, row.featureCode), createdAt: row.createdAt })), purchases: facts.purchases.map(safePurchase), subscriptions: (indexed.subscriptions.get(facts.userId) || []).map((row) => ({ planCode: row.planCode, status: row.status, currentPeriodStart: row.currentPeriodStart, currentPeriodEnd: row.currentPeriodEnd, nextChargeAt: row.nextChargeAt })), refunds: (indexed.refunds.get(facts.userId) || []).map((row) => ({ status: row.status, amountPaise: asNumber(row.amountPaise), reason: row.reason || null, createdAt: row.createdAt })), feedback: [...legacyFeedback.map((row) => ({ source: "legacy", rawScore: row.score ?? null, score: row.score === null || row.score === undefined ? null : Math.round((asNumber(row.score) / 10 * 5) * 10) / 10, scale: 5, sourceScale: 10, type: row.feedbackType || null, tool: row.toolId || null, createdAt: row.createdAt })), ...feedback.map((row) => ({ source: "current", rawScore: row.rating ?? null, score: row.rating ?? null, rating: row.rating ?? null, scale: 5, sourceScale: 5, comment: row.isAnonymous ? null : row.comment || null, path: row.path || null, createdAt: row.createdAt }))].sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt)), featureRequests: requests.map((row) => ({ title: row.title, status: row.status, board: row.board, votes: asNumber(row.votes), comments: asNumber(row.commentsCount), createdAt: row.createdAt })), coverage: { periodDays: detailRange.days, nonCreditToolActivity: !facts.activityAvailable ? "unavailable" : facts.pluginActiveDays > 0 ? "measured" : "no_activity_recorded", pluginActiveDays: facts.activityAvailable ? facts.pluginActiveDays : null, activeDaysInRange: facts.activityAvailable ? facts.distinctDays.size : null, message: !facts.activityAvailable ? "Plugin activity could not be read, so this profile shows logins and credited tool activity only. This is a read failure, not an absence of activity." : facts.pluginActiveDays > 0 ? `Includes successful logins, credited tool activity, and tool jobs observed in plugin activity where no credit reservation was recorded. Covers the last ${detailRange.days} days.` : "Includes successful logins and credited tool activity. No plugin activity has been recorded for this user in the selected period." } };
}

function groupToolReservations(rows, compensatedReservations = new Set()) {
  const groups = new Map();
  for (const row of rows) {
    const tool = normalizeToolCode(row.toolCode, row.featureCode); const current = groups.get(tool.key) || { key: tool.key, label: tool.label, total: 0, committed: 0, released: 0, expired: 0, compensated: 0, processing: 0, creditsConsumed: 0, users: new Set(), repeat: new Map(), durations: [], features: new Map() };
    current.total += 1; current.users.add(id(row.user)); current.repeat.set(id(row.user), (current.repeat.get(id(row.user)) || 0) + 1);
    const effectiveStatus = compensatedReservations.has(id(row._id)) ? "compensated" : row.status;
    if (effectiveStatus === "reserved") current.processing += 1; else if (effectiveStatus in current) current[effectiveStatus] += 1;
    if (effectiveStatus === "committed") { current.creditsConsumed += asNumber(row.creditsReserved); if (row.committedAt && row.createdAt) current.durations.push(asDate(row.committedAt) - asDate(row.createdAt)); }
    const feature = String(tool.feature || row.featureCode || "General"); current.features.set(feature, (current.features.get(feature) || 0) + 1); groups.set(tool.key, current);
  }
  return groups;
}

function serializeTool(group, previous = null) {
  const terminal = group.committed + group.released + group.expired + group.compensated;
  return { key: group.key, label: group.label, coverage: "measured", uniqueUsers: group.users.size, completedJobs: group.committed, creditsConsumed: group.creditsConsumed, completionRate: percent(group.committed, terminal), releasedJobs: group.released, expiredJobs: group.expired, compensatedJobs: group.compensated, processingJobs: group.processing, averageCompletionMs: group.durations.length ? Math.round(group.durations.reduce((a, b) => a + b, 0) / group.durations.length) : null, repeatUsers: [...group.repeat.values()].filter((count) => count >= 2).length, previousCompletedJobs: previous?.committed || 0, completedJobsChange: change(group.committed, previous?.committed || 0), features: [...group.features.entries()].map(([feature, count]) => ({ feature, count })).sort((a, b) => b.count - a.count) };
}

/**
 * Per-tool activity derived from plugin telemetry rather than credit
 * reservations.
 *
 * Credit reservations only exist for tools that charge credits, so any tool
 * without an active credit system was previously unmeasurable — it could only
 * ever be reported as "unavailable". This aggregates the stored plugin events
 * instead, which every tool produces, and is grouped in MongoDB so the whole
 * collection is never pulled into memory.
 *
 * Identity falls back through account id, email, anonymous id and finally
 * device id, so an unauthenticated user still counts once rather than not at
 * all. The chosen key is only ever used for distinct counting.
 */
async function toolActivityFromTelemetry(start, end) {
  try {
    const collection = await getEventsCollection();
    const openIndex = await toolOpensBySessionTool(start, end);
    const rows = await collection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: start, $lt: end },
            tool: { $nin: [null, "", "unknown"] },
            // Heartbeats and background plumbing carry a tool but represent no
            // user action; including them let idle time outrank real usage in
            // the tools ranking, which sorts on event volume.
            eventType: { $nin: PASSIVE_EVENT_TYPES },
          },
        },
        {
          // Grouped per session as well as per tool so a completion can be
          // tested against the open that justifies it. Collapsed back to one
          // row per tool below.
          $group: {
            _id: { tool: "$tool", session: "$sessionId" },
            events: { $sum: 1 },
            sessions: { $addToSet: "$sessionId" },
            // Email is preferred over userId as the distinct-counting key, and
            // lower-cased. Events written before the identity fix store an
            // email IN user.userId while newer ones store an account id there
            // and the email alongside; keying on userId would therefore count
            // the same person once per era. Preferring email converges both on
            // one key. Anonymous visitors fall through to their pseudonymous id
            // and finally the device. This key is only ever used for counting
            // distinct humans, never for joining to an account.
            identities: {
              $addToSet: {
                $toLower: {
                  $ifNull: [
                    "$user.email",
                    { $ifNull: ["$user.userId", { $ifNull: ["$user.anonymousId", "$deviceId"] }] },
                  ],
                },
              },
            },
            accountIds: { $addToSet: "$user.userId" },
            opens: { $sum: { $cond: [{ $eq: ["$eventType", "tool_opened"] }, 1, 0] } },
            actionsStarted: { $sum: { $cond: [{ $eq: ["$eventType", "tool_action_started"] }, 1, 0] } },
            completionsAt: { $push: { $cond: [{ $eq: ["$eventType", "tool_action_completed"] }, "$eventAt", "$$REMOVE"] } },
            actionsFailed: { $sum: { $cond: [{ $eq: ["$eventType", "tool_action_failed"] }, 1, 0] } },
            errors: { $sum: { $cond: [{ $eq: ["$eventType", "user_facing_error_displayed"] }, 1, 0] } },
            featureUses: { $sum: { $cond: [{ $eq: ["$eventType", "feature_used"] }, 1, 0] } },
            activeMs: {
              $sum: {
                $cond: [
                  { $eq: ["$eventType", "active_tool_time"] },
                  { $ifNull: ["$payload.durationMs", 0] },
                  0,
                ],
              },
            },
            lastEventAt: { $max: "$eventAt" },
          },
        },
      ], aggregationOptions())
      .toArray();

    // Completions the user did not ask for are removed here, per session, for
    // the same reason and by the same rule as the per-user job counts: opening
    // the plugin starts background services that report finished work nobody
    // requested. Both pages must apply the identical rule or they will disagree
    // about the same runs.
    let backgroundCompletions = 0;
    const flattened = rows.map((row) => {
      const rawTool = String(row._id?.tool || "").trim().toLowerCase();
      const initiated = countUserInitiated(row.completionsAt || [], row._id?.session, rawTool, openIndex);
      backgroundCompletions += initiated.background;
      return { ...row, _id: row._id?.tool, actionsCompleted: initiated.jobs };
    });

    // Non-tool surfaces are filtered AFTER normalization so aliases collapse
    // first. "dashboard" in particular is the default tool for session,
    // heartbeat and analytics-plumbing events, so it would otherwise dominate
    // both the tools grid and the observed-event total.
    const measurable = flattened.filter((row) => {
      const normalized = normalizeToolCode(row._id);
      return !NON_TOOL_SURFACES.has(String(row._id || "").trim().toLowerCase()) &&
        !NON_TOOL_SURFACES.has(normalized.key);
    });

    // Aliases can map several raw codes onto one key, so merge rather than
    // letting the last one win.
    const merged = new Map();
    for (const row of measurable) {
      const normalized = normalizeToolCode(row._id);
      const existing = merged.get(normalized.key);
      if (!existing) {
        merged.set(normalized.key, { ...row, _id: normalized.key, _label: normalized.label, _rawTools: [row._id] });
        continue;
      }
      existing._rawTools.push(row._id);
      existing.events += asNumber(row.events);
      existing.opens += asNumber(row.opens);
      existing.actionsStarted += asNumber(row.actionsStarted);
      existing.actionsCompleted += asNumber(row.actionsCompleted);
      existing.actionsFailed += asNumber(row.actionsFailed);
      existing.errors += asNumber(row.errors);
      existing.featureUses += asNumber(row.featureUses);
      existing.activeMs += asNumber(row.activeMs);
      existing.sessions = [...new Set([...(existing.sessions || []), ...(row.sessions || [])])];
      existing.identities = [...new Set([...(existing.identities || []), ...(row.identities || [])])];
      existing.accountIds = [...new Set([...(existing.accountIds || []), ...(row.accountIds || [])])];
      const latest = asDate(row.lastEventAt);
      if (latest && (!existing.lastEventAt || latest > asDate(existing.lastEventAt))) existing.lastEventAt = row.lastEventAt;
    }

    const byTool = new Map(
      [...merged.values()].map((row) => {
        const normalized = normalizeToolCode(row._id);
        return [
          normalized.key,
          {
            key: normalized.key,
            label: normalized.label,
            rawTool: (row._rawTools || [row._id]).join(", "),
            events: asNumber(row.events),
            sessions: (row.sessions || []).filter(Boolean).length,
            uniqueUsers: (row.identities || []).filter(Boolean).length,
            knownAccounts: (row.accountIds || []).filter(Boolean).length,
            opens: asNumber(row.opens),
            actionsStarted: asNumber(row.actionsStarted),
            actionsCompleted: asNumber(row.actionsCompleted),
            // A tool that reports invocations and starts but never a completion
            // is not idle — it is under-instrumented, and its real job count is
            // unknowable from telemetry. Naming it beats substituting a number
            // that means something else.
            reportsCompletions: asNumber(row.actionsCompleted) > 0
              || !(asNumber(row.featureUses) || asNumber(row.actionsStarted)),
            actionsFailed: asNumber(row.actionsFailed),
            errors: asNumber(row.errors),
            featureUses: asNumber(row.featureUses),
            activeMs: asNumber(row.activeMs),
            lastEventAt: asDate(row.lastEventAt),
          },
        ];
      })
    );
    return { byTool, backgroundCompletions };
  } catch (error) {
    // Telemetry is supplementary to the credit-backed numbers; if it is
    // unreachable the reservation-derived metrics must still render.
    console.error("Tool telemetry aggregation failed:", error?.message || error);
    return { byTool: new Map(), backgroundCompletions: 0 };
  }
}

export async function productTools(days = 30) {
  const range = period(days); const [reservations, catalog, users, compensations] = await Promise.all([(await getBackendUsageReservationsCollection()).find({}).toArray(), (await getBackendToolsCollection()).find({}, { projection: { name: 1, slug: 1, category: 1, badge: 1, isActive: 1 } }).toArray(), (await getBackendUsersCollection()).find({}, { projection: { createdAt: 1, favorites: 1 } }).toArray(), (await getBackendCreditLedgerCollection()).find({ reason: "compensation_credit" }, { projection: { reservation: 1 } }).toArray()]);
  const compensated = new Set(compensations.map((row) => id(row.reservation)).filter(Boolean));
  const current = groupToolReservations(reservations.filter((row) => inRange(reservationAt(row), range.currentStart, range.now)), compensated);
  const previous = groupToolReservations(reservations.filter((row) => inRange(reservationAt(row), range.previousStart, range.currentStart)), compensated);
  const joinedAt = new Map(users.map((row) => [id(row._id), asDate(row.createdAt)]));
  const favoriteKey = (value) => ({ frames: "frames-to-pdf", "import-tool": "file-importer" }[String(value || "").toLowerCase()] || normalizeToolCode(value).key);
  const favoriteCounts = users.flatMap((row) => Array.isArray(row.favorites) ? row.favorites : []).reduce((counts, value) => counts.set(favoriteKey(value), (counts.get(favoriteKey(value)) || 0) + 1), new Map());
  const measured = [...current.values()].map((group) => {
    const serialized = serializeTool(group, previous.get(group.key));
    const newUsers = [...group.users].filter((userId) => inRange(joinedAt.get(userId), range.currentStart, range.now)).length;
    return { ...serialized, newUsers, existingUsers: Math.max(0, serialized.uniqueUsers - newUsers), favorites: favoriteCounts.get(serialized.key) || 0 };
  }).sort((a, b) => b.completedJobs - a.completedJobs);
  const { byTool: telemetry, backgroundCompletions } = await toolActivityFromTelemetry(range.currentStart, range.now);
  const existing = new Set(measured.map((row) => row.key));

  // Credit-measured tools gain their telemetry counters alongside the
  // reservation-derived ones. The two are kept in separate fields because they
  // measure different things: reservations count billable jobs, telemetry
  // counts observed activity.
  const measuredWithTelemetry = measured.map((row) => {
    const activity = telemetry.get(row.key);
    return activity
      ? { ...row, telemetry: activity, observedJobs: activity.actionsCompleted, coverage: "measured" }
      : { ...row, telemetry: null, observedJobs: null, coverage: "measured" };
  });

  // Tools with no credit system emit no reservations, so telemetry is their
  // only possible source of usage. They are reported as observed activity
  // rather than as unmeasurable.
  const telemetryOnly = [...telemetry.values()]
    .filter((activity) => !existing.has(activity.key))
    .map((activity) => ({
      key: activity.key,
      label: activity.label,
      category: null,
      coverage: "telemetry",
      // Says what was observed, not why. The old wording asserted the tool
      // charges nothing, which stopped being true once credits were added
      // across the plugin — a tool lands here whenever no reservation was
      // recorded for it in this period, whatever the reason.
      message: "No credit reservations were recorded for this tool in this period, so its numbers come from plugin activity.",
      uniqueUsers: activity.uniqueUsers,
      // completedJobs deliberately stays null on these rows. On credit-backed
      // tools it counts committed reservations (billable jobs); telemetry
      // counts observed actions. Publishing both under one field name would let
      // a consumer sum two different units into a meaningless total. The
      // observed counts live under `telemetry` instead.
      completedJobs: null,
      observedJobs: activity.actionsCompleted,
      failedJobs: null,
      creditsConsumed: 0,
      favorites: favoriteCounts.get(activity.key) || 0,
      telemetry: activity,
    }))
    .sort((a, b) => b.telemetry.events - a.telemetry.events);

  const covered = new Set([...existing, ...telemetryOnly.map((row) => row.key)]);
  const unavailable = catalog
    .map((row) => {
      const normalized = normalizeToolCode(row.slug);
      return {
        key: normalized.key,
        label: row.name || normalized.label,
        category: row.category || null,
        catalogStatus: row.badge?.label || (row.isActive ? "Active" : "Inactive"),
        favorites: favoriteCounts.get(normalized.key) || 0,
        coverage: "unavailable",
        message: "No credit reservations and no plugin activity recorded for this tool in the selected period.",
      };
    })
    .filter((row) => !covered.has(row.key));

  return {
    asOf: range.now,
    period: { days: range.days, start: range.currentStart },
    summary: {
      measuredTools: measuredWithTelemetry.length,
      telemetryOnlyTools: telemetryOnly.length,
      unavailableTools: unavailable.length,
      completedJobs: measuredWithTelemetry.reduce((sum, row) => sum + row.completedJobs, 0),
      // Finished work the plugin observed, across every tool including the ones
      // with no credit system. Reported beside the credited total rather than
      // merged into it: on a credit-backed tool the same run appears in both,
      // so a sum would double it. A large gap between the two is the signal
      // that reservations are not landing.
      observedJobs: [...telemetry.values()].reduce((sum, row) => sum + row.actionsCompleted, 0),
      // Tools whose telemetry can never answer "how many jobs finished".
      toolsWithoutCompletions: [...telemetry.values()].filter((row) => !row.reportsCompletions).map((row) => row.label),
      // Finished-work events nobody asked for: background services reporting
      // themselves at plugin launch. Reported so the size of the filter is
      // visible rather than silently applied.
      backgroundCompletions,
      expiredJobs: measuredWithTelemetry.reduce((sum, row) => sum + row.expiredJobs, 0),
      creditsConsumed: measuredWithTelemetry.reduce((sum, row) => sum + row.creditsConsumed, 0),
      observedToolEvents: [...telemetry.values()].reduce((sum, row) => sum + row.events, 0),
    },
    items: [...measuredWithTelemetry, ...telemetryOnly, ...unavailable],
  };
}

export async function productToolDetail(toolCode, days = 30) {
  const payload = await productTools(days); const normalized = normalizeToolCode(toolCode).key; return payload.items.find((row) => row.key === normalized) || null;
}

export async function productLifecycle(days = 90) {
  const range = period(days); const core = await loadCore(range); const indexed = indexCore(core); const cohort = core.users.filter((row) => inRange(row.createdAt, range.currentStart, range.now));
  const facts = cohort.map((user) => ({ user, facts: userFacts(user, indexed, range) }));
  const loggedIn = facts.filter((row) => row.facts.sessions.length); const activated = facts.filter((row) => row.facts.activated); const returned = facts.filter((row) => row.facts.distinctDays.size >= 2); const purchased = facts.filter((row) => resolveLifecycleStage(row.facts, row.facts.billing) === "customer");
  const stages = [{ key: "signed_up", label: "Signed up", users: cohort.length }, { key: "logged_in", label: "Successful login", users: loggedIn.length }, { key: "activated", label: "Used a tool", users: activated.length }, { key: "returned", label: "Returned another day", users: returned.length }, { key: "purchased", label: "Confirmed purchase", users: purchased.length }].map((stage, index, all) => ({ ...stage, conversionFromPrevious: index ? percent(stage.users, all[index - 1].users) : 100 }));
  const loginTimes = loggedIn.map(({ user, facts: item }) => loginAt(item.sessions[0]) - asDate(user.createdAt)); const activationTimes = activated
    // Activation no longer requires a credited reservation, so a user can be
    // activated with firstCommitted null. Time-to-activation is only meaningful
    // where a timestamped first use exists; telemetry activation is known to
    // the day, not the instant, so those users are excluded from the median
    // rather than given a fabricated time.
    .filter(({ facts: item }) => item.firstCommitted)
    .map(({ user, facts: item }) => reservationAt(item.firstCommitted) - asDate(user.createdAt));
  const weeks = new Map();
  for (const { user, facts: item } of facts) {
    const created = asDate(user.createdAt); const monday = new Date(Date.UTC(created.getUTCFullYear(), created.getUTCMonth(), created.getUTCDate() - ((created.getUTCDay() + 6) % 7))); const key = monday.toISOString().slice(0, 10); const row = weeks.get(key) || { week: key, signedUp: 0, loggedIn: 0, activated: 0, returned7d: 0, returned30d: 0, latestSignupAt: created };
    if (created > row.latestSignupAt) row.latestSignupAt = created;
        // Any tool use, matching the funnel's "Used a tool" stage. Counting only
    // credited work here made the cohort table's activation rate disagree with
    // the funnel drawn directly above it.
    row.signedUp += 1; row.loggedIn += Number(item.sessions.length > 0); row.activated += Number(item.activated);
    // Retention must use the same evidence AND the same rule as the funnel's
    // "returned" stage, which counts distinct calendar days.
    //
    // Deriving an instant from a plugin day string and then requiring a full
    // 24 hours to have elapsed silently dropped the most common return of all:
    // for a 09:00 signup, the next day's midnight is only 15 hours later, so a
    // genuine D+1 return failed the test — while the identical return observed
    // as a login, which keeps its true instant, passed. Both sides are now
    // bucketed to calendar days with the same formatter, so the two kinds of
    // evidence answer the same question and agree with the funnel.
    const lifetimeActivity = indexed.pluginActivityLifetime;
    const formatCohortDay = lifetimeActivity.formatDay || dayKeyFormatter(lifetimeActivity.timezone);
    const signupDay = formatCohortDay.format(created);
    const laterDays = new Set([
      ...item.sessions.map((session) => formatCohortDay.format(loginAt(session))),
      ...(lifetimeActivity.byUserId.get(id(user._id)) || []),
      ...(lifetimeActivity.byEmail.get(String(user.email || "").toLowerCase()) || []),
    ]);
    const daysAfterSignup = [...laterDays]
      .filter((day) => day > signupDay)
      .map((day) => Math.round((Date.parse(`${day}T00:00:00Z`) - Date.parse(`${signupDay}T00:00:00Z`)) / DAY_MS))
      .filter((offset) => Number.isFinite(offset) && offset >= 1);
    row.returned7d += Number(daysAfterSignup.some((offset) => offset <= 7)); row.returned30d += Number(daysAfterSignup.some((offset) => offset <= 30)); weeks.set(key, row);
  }
  return { asOf: range.now, coverage: "Activation counts any tool use, whether or not it charged credits.", stages, timing: { medianTimeToLoginMs: median(loginTimes), medianTimeToActivationMs: median(activationTimes) }, stuck: { signedUpNotLoggedIn: cohort.length - loggedIn.length, loggedInNotActivated: loggedIn.filter((row) => !row.facts.activated).length, activatedNotReturned: activated.filter((row) => row.facts.distinctDays.size < 2).length }, cohorts: [...weeks.values()].sort((a, b) => a.week.localeCompare(b.week)).map((row) => ({ week: row.week, signedUp: row.signedUp, loggedIn: row.loggedIn, activated: row.activated, returned7d: row.returned7d, returned30d: row.returned30d, activationRate: percent(row.activated, row.signedUp), return7dRate: range.now - row.latestSignupAt >= 7 * DAY_MS ? percent(row.returned7d, row.signedUp) : null, return30dRate: range.now - row.latestSignupAt >= 30 * DAY_MS ? percent(row.returned30d, row.signedUp) : null })) };
}

export async function productCommercial(days = 30) {
  const range = period(days); const core = await loadCore(); const countBy = (rows, field) => Object.fromEntries([...rows.reduce((map, row) => map.set(String(row[field] || "unknown"), (map.get(String(row[field] || "unknown")) || 0) + 1), new Map()).entries()].sort((a, b) => b[1] - a[1]));
  const purchases = core.purchases.filter((row) => inRange(row.capturedAt || row.updatedAt || row.createdAt, range.currentStart, range.now)); const refunds = core.refunds.filter((row) => inRange(row.updatedAt || row.createdAt, range.currentStart, range.now)); const captured = purchases.filter((row) => row.status === "captured"); const processedRefunds = refunds.filter((row) => row.status === "processed");
  const ledgerRange = core.ledgers.filter((row) => inRange(row.createdAt, range.currentStart, range.now));
  // Windowed like everything else on the panel. Purchases and credit movements
  // honoured the range while these two were lifetime totals, so moving the
  // selector from 90 days to 7 changed half the numbers and left the other half
  // standing — on the same screen, under the same heading.
  const subscriptionsInRange = core.subscriptions.filter((row) => inRange(row.updatedAt || row.createdAt, range.currentStart, range.now));
  const grantsInRange = core.starterGrants.filter((row) => inRange(row.grantedAt || row.blockedAt || row.updatedAt || row.createdAt, range.currentStart, range.now));
  return { asOf: range.now, period: { days: range.days, start: range.currentStart }, summary: { wallets: core.billings.length, availableCredits: core.billings.reduce((sum, row) => sum + asNumber(row.availableCredits), 0), heldCredits: core.billings.reduce((sum, row) => sum + asNumber(row.heldCredits), 0), lowCreditUsers: core.billings.filter((row) => asNumber(row.availableCredits) <= lowCreditThreshold()).length, zeroCreditUsers: core.billings.filter((row) => asNumber(row.availableCredits) === 0).length, grossRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0), refundsPaise: processedRefunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0), netRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0) - processedRefunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0), revenueStatus: captured.length ? "confirmed" : "no_confirmed_captured_payments" }, purchaseAttempts: countBy(purchases, "status"), subscriptions: countBy(subscriptionsInRange, "status"), starterGrants: countBy(grantsInRange, "status"), creditMovements: countBy(ledgerRange, "reason") };
}

/**
 * What a piece of feedback was about, named as a tool where it is one.
 *
 * Legacy rows carry a tool id; current rows carry the page path the user was on
 * when they rated. Grouping the two raw put "/tools" and "palettable" side by
 * side as if they were peers, and split a tool across its aliases. A path that
 * points at a tool resolves to that tool; anything else keeps its own name so
 * nothing is silently relabelled as a product.
 */
function feedbackToolLabel(value) {
  const raw = String(value || "").trim();
  if (!raw) return "Unspecified";
  const candidate = raw.startsWith("/") ? raw.split("/").filter(Boolean).at(-1) : raw;
  if (!candidate) return raw;
  const normalized = normalizeToolCode(candidate);
  if (NON_TOOL_SURFACES.has(normalized.key) || normalized.key === "unattributed") return raw;
  // normalizeToolCode title-cases anything it does not recognise, so an unknown
  // path segment would come back looking like a product. Only a code the tool
  // vocabulary actually knows is relabelled.
  return normalized.known ? normalized.label : raw;
}

export async function productFeedback(days = 90) {
  const range = period(days); const [legacy, current, requests] = await Promise.all([(await getBackendFeedbackCollection()).find({}).toArray(), (await getBackendFeedbacksCollection()).find({}).toArray(), (await getBackendFeatureRequestsCollection()).find({ isDeleted: { $ne: true } }).toArray()]);
  const feedback = [...legacy.map((row) => ({ id: id(row._id), source: "legacy", authorUserId: ObjectId.isValid(String(row.userId || row.authId || "")) ? id(row.userId || row.authId) : null, rawScore: row.score === null || row.score === undefined ? null : asNumber(row.score), scale: 10, type: row.feedbackType || null, tool: row.toolId || null, comment: row.feedback || null, createdAt: row.createdAt })), ...current.map((row) => ({ id: id(row._id), source: "current", authorUserId: ObjectId.isValid(String(row.userId || "")) && !row.isAnonymous ? id(row.userId) : null, rawScore: row.rating === null || row.rating === undefined ? null : asNumber(row.rating), scale: 5, type: "rating", tool: row.path || null, comment: row.isAnonymous ? null : row.comment || null, createdAt: row.createdAt }))].map((row) => ({ ...row, score: row.rawScore === null ? null : Math.round((row.rawScore / row.scale * 5) * 10) / 10, toolLabel: feedbackToolLabel(row.tool) })).filter((row) => inRange(row.createdAt, range.currentStart, range.now)).sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt));
  const requestRows = requests.filter((row) => inRange(row.createdAt, range.currentStart, range.now)); const countBy = (rows, getter) => Object.fromEntries([...rows.reduce((map, row) => { const key = String(getter(row) || "Unspecified"); map.set(key, (map.get(key) || 0) + 1); return map; }, new Map()).entries()].sort((a, b) => b[1] - a[1])); const scores = feedback.map((row) => row.score).filter(Number.isFinite);
  return { asOf: range.now, coverage: { feedbackSources: ["feedback (10-point legacy scale)", "feedbacks (5-point current scale)"], sampleSize: feedback.length, scoreNormalization: "Legacy 10-point ratings are converted to a 5-point scale before averaging." }, summary: { feedback: feedback.length, ratedResponses: scores.length, averageScore: scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length * 10) / 10 : null, featureRequests: requestRows.length, votes: requestRows.reduce((sum, row) => sum + asNumber(row.votes), 0) }, ratingDistribution: countBy(feedback.filter((row) => Number.isFinite(row.score)), (row) => row.score), feedbackByTool: countBy(feedback, (row) => row.toolLabel), requestStatus: countBy(requestRows, (row) => row.status), requestBoards: countBy(requestRows, (row) => row.board), recentFeedback: feedback.slice(0, 25), topRequests: requestRows.sort((a, b) => asNumber(b.votes) - asNumber(a.votes) || asDate(b.createdAt) - asDate(a.createdAt)).slice(0, 25).map((row) => ({ id: id(row._id), authorUserId: ObjectId.isValid(String(row.authorId || "")) ? id(row.authorId) : null, title: row.title, status: row.status, board: row.board, votes: asNumber(row.votes), comments: asNumber(row.commentsCount), createdAt: row.createdAt })) };
}

export async function productDataHealth(newsletterConfigured = false) {
  const core = await loadCore(); const latest = (rows, fields) => rows.reduce((result, row) => { for (const field of fields) { const value = asDate(row[field]); if (value && (!result || value > result)) result = value; } return result; }, null);
  const [telemetry, identity] = await Promise.all([
    telemetryHealth(new Date()),
    identityHealth(new Date(Date.now() - 30 * DAY_MS)),
  ]);
  telemetry.ageHours = telemetry.latestAt ? Math.round(((Date.now() - telemetry.latestAt) / 36e5) * 10) / 10 : null;
  const completedSessions = core.sessions.filter(successfulSession).length; const incompleteLinkedSessions = core.sessions.filter((row) => row.user && !successfulSession(row)).length; const terminal = core.reservations.filter(terminalReservation); const attributed = terminal.filter((row) => row.toolCode || row.featureCode).length;
  const capturedPurchaseIds = new Set(core.purchases.filter((row) => row.status === "captured").map((row) => id(row._id)));
  const processedRefunds = core.refunds.filter((row) => row.status === "processed");
  const unmatchedProcessedRefunds = processedRefunds.filter((row) => !capturedPurchaseIds.has(id(row.purchase))).length;
  return { ok: true, asOf: new Date(), components: { backendDatabase: { status: "healthy", users: core.users.length }, newsletter: { status: newsletterConfigured ? "configured" : "unavailable" }, telemetry, identity }, freshness: { users: latest(core.users, ["updatedAt", "createdAt"]), sessions: latest(core.sessions, ["completedAt", "createdAt"]), ledgers: latest(core.ledgers, ["createdAt"]), reservations: latest(core.reservations, ["updatedAt", "createdAt"]), purchases: latest(core.purchases, ["updatedAt", "createdAt"]) }, coverage: { toolJobs: { backgroundCompletionsExcluded: asNumber(core.pluginToolUseLifetime?.backgroundCompletions), ungatedTools: core.pluginToolUseLifetime?.ungatedTools || [], message: "A completed tool action counts as a job only when the same session shows the user opening that tool first. Opening the plugin starts background services that report finished work nobody asked for, and they are excluded by this rule. Tools listed as ungated never report an open, so the rule cannot be applied to them and all of their completions are counted." }, wallets: { users: core.users.length, initialized: core.billings.length, missing: core.users.length - core.billings.length, percent: percent(core.billings.length, core.users.length) }, sessions: { completed: completedSessions, incompleteLinked: incompleteLinkedSessions }, identityJoins: { sessionsWithoutUser: core.sessions.filter((row) => !row.user).length, reservationsWithoutUser: core.reservations.filter((row) => !row.user).length }, toolAttribution: { terminalJobs: terminal.length, attributed, unattributed: terminal.length - attributed, percent: percent(attributed, terminal.length) }, revenue: { capturedPurchases: capturedPurchaseIds.size, processedRefunds: processedRefunds.length, unmatchedProcessedRefunds, message: unmatchedProcessedRefunds ? `${unmatchedProcessedRefunds} processed refunds do not match a captured purchase record.` : capturedPurchaseIds.size ? "Confirmed captured payments are available." : "No confirmed captured payments are present." } } };
}
