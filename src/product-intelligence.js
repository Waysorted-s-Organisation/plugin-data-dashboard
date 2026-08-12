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
          },
        },
      ])
      .toArray();

    for (const row of rows) {
      if (!row._id) continue;
      const userId = row.userId ? String(row.userId) : null;
      const email = row.email ? String(row.email).toLowerCase() : null;
      // The account id is preferred; email is the fallback because the join on
      // the read side resolves it against a real user record.
      if (!userId && !email) continue;
      links.set(String(row._id), userId || email);
    }
  } catch (error) {
    // Losing the links degrades attribution; it must not break the page.
    console.error("Identity link aggregation failed:", error?.message || error);
  }
  return links;
}

async function pluginActivityDaysByIdentity(start, end, identityLinks = new Map()) {
  const timezone = reportingTimezone();
  const byUserId = new Map();
  const byEmail = new Map();
  // Signed-out visitors. They have no account and no email, but the plugin
  // gives every session a stable pseudonymous id, so their behaviour is
  // measurable even though who they are is not. Grouping only by userId/email
  // discarded them entirely, leaving the platform's largest population
  // unmeasured purely because it had no name attached.
  const byAnonymous = new Map();
  let available = true;
  try {
    const collection = await getEventsCollection();
    const rows = await collection
      .aggregate([
        { $match: { eventAt: { $gte: start, $lte: end }, eventType: { $nin: PASSIVE_EVENT_TYPES } } },
        {
          $group: {
            _id: {
              identity: { $ifNull: ["$user.userId", "$user.email"] },
              // Falls back to the device so a signed-out visitor is still
              // counted once even when an older event carries no pseudonymous id.
              anonymous: { $ifNull: ["$user.anonymousId", "$deviceId"] },
              day: { $dateToString: { date: "$eventAt", format: "%Y-%m-%d", timezone } },
            },
          },
        },
        {
          $group: {
            _id: { identity: "$_id.identity", anonymous: "$_id.anonymous" },
            days: { $addToSet: "$_id.day" },
          },
        },
      ])
      .toArray();

    for (const row of rows) {
      const identity = row._id?.identity;
      const anonymous = row._id?.anonymous;
      // A signed-out row is routed to the account it was later linked to, so
      // pre-signup history lands with the person rather than beside them.
      const linked = identity ? null : identityLinks.get(String(anonymous || ""));
      const resolved = identity || linked || null;
      const target = !resolved ? byAnonymous : looksLikeEmail(resolved) ? byEmail : byUserId;
      const rawKey = resolved || anonymous;
      if (!rawKey) continue;
      const key = looksLikeEmail(rawKey) ? String(rawKey).toLowerCase() : String(rawKey);
      const existing = target.get(key) || new Set();
      for (const day of row.days || []) existing.add(day);
      target.set(key, existing);
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
  return { byUserId, byEmail, byAnonymous, timezone, available, formatDay: dayKeyFormatter(timezone) };
}

/**
 * The tool each identity used most, derived from plugin activity.
 *
 * The users table's top-tool column was built purely from credit-charged
 * reservations, so a tool that charges nothing could never appear there and its
 * users read as having used nothing at all. Telemetry knows what they actually
 * opened, whether or not it billed them.
 */
async function pluginTopToolByIdentity(start, end, identityLinks = new Map()) {
  const byUserId = new Map();
  const byEmail = new Map();
  const byAnonymous = new Map();
  try {
    const collection = await getEventsCollection();
    const rows = await collection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: start, $lte: end },
            eventType: { $nin: PASSIVE_EVENT_TYPES },
            tool: { $nin: [null, "", "unknown"] },
          },
        },
        {
          $group: {
            _id: {
              identity: { $ifNull: ["$user.userId", "$user.email"] },
              anonymous: { $ifNull: ["$user.anonymousId", "$deviceId"] },
              tool: "$tool",
            },
            events: { $sum: 1 },
            lastEventAt: { $max: "$eventAt" },
          },
        },
        { $sort: { lastEventAt: -1 } },
      ])
      .toArray();

    for (const row of rows) {
      const normalized = normalizeToolCode(row._id?.tool);
      // Navigation chrome is not a tool the user "used".
      if (NON_TOOL_SURFACES.has(String(row._id?.tool || "").trim().toLowerCase())) continue;
      if (NON_TOOL_SURFACES.has(normalized.key)) continue;

      const identity = row._id?.identity;
      const anonymous = row._id?.anonymous;
      // A signed-out row is routed to the account it was later linked to, so
      // pre-signup history lands with the person rather than beside them.
      const linked = identity ? null : identityLinks.get(String(anonymous || ""));
      const resolved = identity || linked || null;
      const target = !resolved ? byAnonymous : looksLikeEmail(resolved) ? byEmail : byUserId;
      const rawKey = resolved || anonymous;
      if (!rawKey) continue;
      const key = looksLikeEmail(rawKey) ? String(rawKey).toLowerCase() : String(rawKey);

      const existing = target.get(key);
      // Rows arrive newest-first, so the first one for an identity is the tool
      // they most recently used.
      const lastEventAt = asDate(row.lastEventAt);
      if (!existing || (lastEventAt && lastEventAt > existing.lastEventAt)) {
        target.set(key, {
          key: normalized.key,
          label: normalized.label,
          events: asNumber(row.events),
          credited: false,
          lastEventAt: asDate(row.lastEventAt),
        });
      }
    }
  } catch (error) {
    // A ReferenceError here previously looked identical to an unreachable
    // database: the map came back empty and every caller treated that as
    // "this user has used nothing".
    console.error(
      error instanceof ReferenceError || error instanceof TypeError
        ? `Plugin top-tool aggregation has a bug: ${error?.stack || error}`
        : `Plugin top-tool aggregation failed: ${error?.message || error}`
    );
  }
  return { byUserId, byEmail, byAnonymous };
}

async function telemetryHealth(now = new Date()) {
  try {
    const collection = await getEventsCollection();
    const match = { schemaVersion: { $gte: 1 }, eventType: { $in: SEMANTIC_EVENT_TYPES } };
    const [latest, earliest, activeDays] = await Promise.all([
      collection.findOne(match, { sort: { eventAt: -1 }, projection: { eventAt: 1 } }),
      collection.findOne(match, { sort: { eventAt: 1 }, projection: { eventAt: 1 } }),
      collection.aggregate([{ $match: { ...match, eventAt: { $gte: new Date(now.getTime() - 8 * DAY_MS) } } }, { $group: { _id: { $dateToString: { date: "$eventAt", format: "%Y-%m-%d", timezone: "UTC" } } } }]).toArray(),
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
 * @param range The reporting period, when the caller has one. Plugin activity
 *   is then aggregated once per exact window so day bucketing never has to
 *   guess which period a boundary day belongs to, and the wide lookback still
 *   covers cohort math that reaches back further than any single window.
 */
async function loadCore(range = null) {
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
  const identityLinks = await identityLinksByAnonymousId();
  const [pluginActivity, pluginActivityCurrent, pluginActivityPrevious, pluginTopTools] = await Promise.all([
    // Wide index: cohort retention measures each user's return relative to
    // their own signup date, so it needs history beyond any single window.
    pluginActivityDaysByIdentity(new Date(Date.now() - lookbackDays * DAY_MS), new Date(), identityLinks),
    range
      ? pluginActivityDaysByIdentity(range.currentStart, range.now, identityLinks)
      : Promise.resolve(null),
    range
      ? pluginActivityDaysByIdentity(range.previousStart, range.currentStart, identityLinks)
      : Promise.resolve(null),
    pluginTopToolByIdentity(
      range ? range.currentStart : new Date(Date.now() - lookbackDays * DAY_MS),
      range ? range.now : new Date(),
      identityLinks
    ),
  ]);

  return {
    users, billings, sessions, reservations, ledgers, purchases, subscriptions, refunds, starterGrants,
    pluginActivity,
    pluginActivityCurrent: pluginActivityCurrent || pluginActivity,
    pluginActivityPrevious: pluginActivityPrevious || pluginActivity,
    pluginTopTools,
  };
}

function indexCore(core) {
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
    pluginActivity: core.pluginActivityCurrent || core.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") },
    pluginTopTools: core.pluginTopTools || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map() },
    pluginActivityLifetime: core.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") },
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
  const currentSessions = sessions.filter((row) => inRange(loginAt(row), range.currentStart, range.now));
  const currentCommitted = committed.filter((row) => inRange(reservationAt(row), range.currentStart, range.now));
  // Active days combine backend logins with plugin activity, bucketed in the
  // configured reporting timezone. Using logins alone made returns from the
  // plugin — which reuses a stored token and creates no new session — invisible.
  const activity = indexed.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") };
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
  const activityAvailable = activity.available !== false && (indexed.pluginActivityLifetime?.available !== false);
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
  const observedTopTool =
    indexed.pluginTopTools?.byUserId.get(userId) ||
    indexed.pluginTopTools?.byEmail.get(String(user.email || "").toLowerCase()) ||
    null;
  const activated = Boolean(firstCommitted || observedTopTool);
  if (!activated) segments.push("not_activated");
  if (activated) segments.push("activated");
  if (currentCommitted.length >= 3) segments.push("engaged");
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
  return { userId, billing, sessions, reservations, committed, purchases, captured, currentSessions, currentCommitted, distinctDays, pluginActiveDays, activityAvailable, activated, lastActiveAt, lastActiveSource, lastLogin, lastLoginDate, firstCommitted, segments, topTool };
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
  if (facts.captured.length || hasPaidSubscription || hasPurchasedCredits) return "customer";
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

function periodSummary(core, start, end, activityIndex = null) {
  const sessions = core.sessions.filter(successfulSession).filter((row) => inRange(loginAt(row), start, end));
  const compensated = new Set(core.ledgers.filter((row) => row.reason === "compensation_credit").map((row) => id(row.reservation)).filter(Boolean));
  const reservations = core.reservations.filter((row) => row.status === "committed" && !compensated.has(id(row._id)) && inRange(reservationAt(row), start, end));
  const newUsers = core.users.filter((row) => inRange(row.createdAt, start, end));
  // Active days per user, from logins and plugin activity alike, bucketed in
  // the reporting timezone. Counting logins only made plugin-only returns
  // invisible, which is why the dashboard reported no returning users.
  const activity = activityIndex || core.pluginActivity || { byUserId: new Map(), byEmail: new Map(), byAnonymous: new Map(), available: false, timezone: "UTC", formatDay: dayKeyFormatter("UTC") };
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
  const activated = [...firstCommitByUser.values()].filter((value) => value >= start && value < end).length;
  const captured = core.purchases.filter((row) => row.status === "captured" && inRange(row.capturedAt || row.updatedAt || row.createdAt, start, end));
  const refunds = core.refunds.filter((row) => row.status === "processed" && inRange(row.updatedAt || row.createdAt, start, end));
  return {
    newUsers: newUsers.length,
    activeUsers,
    activatedUsers: activated,
    returningUsers: [...daysByUser.values()].filter((days) => days.size >= 2).length,
    anonymousVisitors,
    returningAnonymousVisitors,
    completedJobs: reservations.length,
    creditsConsumed: reservations.reduce((sum, row) => sum + asNumber(row.creditsReserved), 0),
    grossRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0),
    refundsPaise: refunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0),
  };
}

export async function productSummary(days = 30) {
  const range = period(days); const core = await loadCore(range);
  const current = periodSummary(core, range.currentStart, range.now, core.pluginActivityCurrent);
  // The previous window ends where the current one begins, so the boundary day
  // belongs to the current period only.
  const previous = periodSummary(core, range.previousStart, range.currentStart, core.pluginActivityPrevious);
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
    return { id: facts.userId, name: user.name || null, email: user.email || null, picture: user.picture || null, joinedAt: user.createdAt || null, segments: facts.segments, lifecycleStage: resolveLifecycleStage(facts, billing), lastLoginAt: facts.lastLoginDate, lastActiveAt: facts.lastActiveAt, lastActiveSource: facts.lastActiveSource, latestLoginSource: latestSource, country: latestCountry, successfulLogins: facts.sessions.length, pluginActiveDays: facts.activityAvailable ? facts.pluginActiveDays : null, activeDaysInRange: facts.activityAvailable ? facts.distinctDays.size : null, pluginActivityAvailable: facts.activityAvailable, creditedJobs: facts.committed.length, creditedJobsInRange: facts.currentCommitted.length, completedJobs: facts.committed.length, jobsInRange: facts.currentCommitted.length, topTool: facts.topTool, toolKeys: [...new Set(facts.committed.map((row) => normalizeToolCode(row.toolCode, row.featureCode).key))], walletStatus: billing ? "initialized" : "missing", availableCredits: billing ? asNumber(billing.availableCredits) : null, heldCredits: billing ? asNumber(billing.heldCredits) : null, subscriptionStatus: billing?.subscriptionStatus || null, subscriptionPlan: billing?.subscriptionPlanCode || null, newsletter: newsletterProfile ? { id: newsletterProfile.id, status: newsletterProfile.status, tags: newsletterProfile.tags || [] } : null };
  });
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
  return { asOf: range.now, coverage: { behavior: "Successful authentication and credited tool activity", newsletter: newsletterByEmail.size ? "connected" : "unavailable" }, summary: { users: total, segmentCounts }, facets, items: rows.slice(offset, offset + pageSize), pagination: { page, pageSize, total, pages: Math.max(1, Math.ceil(total / pageSize)) } };
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
  return { asOf: range.now, user: { id: facts.userId, name: user.name || null, email: user.email || null, picture: user.picture || null, joinedAt: user.createdAt, favorites: user.favorites || [], segments: facts.segments, lifecycleStage: resolveLifecycleStage(facts, billing) }, billing: billing ? { availableCredits: asNumber(billing.availableCredits), heldCredits: asNumber(billing.heldCredits), lifetimeSpentCredits: asNumber(billing.lifetimeSpentCredits), lifetimePurchasedCredits: asNumber(billing.lifetimePurchasedCredits), lifetimeBonusCredits: asNumber(billing.lifetimeBonusCredits), subscriptionStatus: billing.subscriptionStatus, subscriptionPlan: billing.subscriptionPlanCode, pricingTier: billing.pricingTier, pricingCountry: billing.pricingCountry } : null, sessions: facts.sessions.slice(-50).reverse().map((row) => ({ source: row.source || "unknown", country: row.countryCode || null, completedAt: loginAt(row) })), reservations: facts.reservations.slice(-100).reverse().map((row) => ({ id: id(row._id), ...normalizeToolCode(row.toolCode, row.featureCode), rawToolCode: row.toolCode || null, featureCode: row.featureCode || null, status: indexed.compensatedReservations.has(id(row._id)) ? "compensated" : row.status, credits: asNumber(row.creditsReserved), processor: row.processor || null, occurredAt: reservationAt(row), durationMs: row.status === "committed" && row.committedAt && row.createdAt ? asDate(row.committedAt) - asDate(row.createdAt) : null })), ledger: (indexed.ledgers.get(facts.userId) || []).slice().sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt)).slice(0, 100).map((row) => ({ reason: row.reason, deltaCredits: asNumber(row.deltaCredits), tool: normalizeToolCode(row.toolCode, row.featureCode), createdAt: row.createdAt })), purchases: facts.purchases.map(safePurchase), subscriptions: (indexed.subscriptions.get(facts.userId) || []).map((row) => ({ planCode: row.planCode, status: row.status, currentPeriodStart: row.currentPeriodStart, currentPeriodEnd: row.currentPeriodEnd, nextChargeAt: row.nextChargeAt })), refunds: (indexed.refunds.get(facts.userId) || []).map((row) => ({ status: row.status, amountPaise: asNumber(row.amountPaise), reason: row.reason || null, createdAt: row.createdAt })), feedback: [...legacyFeedback.map((row) => ({ source: "legacy", rawScore: row.score ?? null, score: row.score === null || row.score === undefined ? null : Math.round((asNumber(row.score) / 10 * 5) * 10) / 10, scale: 5, sourceScale: 10, type: row.feedbackType || null, tool: row.toolId || null, createdAt: row.createdAt })), ...feedback.map((row) => ({ source: "current", rawScore: row.rating ?? null, score: row.rating ?? null, rating: row.rating ?? null, scale: 5, sourceScale: 5, comment: row.isAnonymous ? null : row.comment || null, path: row.path || null, createdAt: row.createdAt }))].sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt)), featureRequests: requests.map((row) => ({ title: row.title, status: row.status, board: row.board, votes: asNumber(row.votes), comments: asNumber(row.commentsCount), createdAt: row.createdAt })), coverage: { periodDays: detailRange.days, nonCreditToolActivity: !facts.activityAvailable ? "unavailable" : facts.pluginActiveDays > 0 ? "measured" : "no_activity_recorded", pluginActiveDays: facts.activityAvailable ? facts.pluginActiveDays : null, activeDaysInRange: facts.activityAvailable ? facts.distinctDays.size : null, message: !facts.activityAvailable ? "Plugin activity could not be read, so this profile shows logins and credited tool activity only. This is a read failure, not an absence of activity." : facts.pluginActiveDays > 0 ? `Includes successful logins, credited tool activity, and plugin activity for tools that do not consume credits. Covers the last ${detailRange.days} days.` : "Includes successful logins and credited tool activity. No plugin activity has been recorded for this user in the selected period." } };
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
    const rows = await collection
      .aggregate([
        {
          $match: {
            eventAt: { $gte: start, $lte: end },
            tool: { $nin: [null, "", "unknown"] },
            // Heartbeats and background plumbing carry a tool but represent no
            // user action; including them let idle time outrank real usage in
            // the tools ranking, which sorts on event volume.
            eventType: { $nin: PASSIVE_EVENT_TYPES },
          },
        },
        {
          $group: {
            _id: "$tool",
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
            actionsCompleted: { $sum: { $cond: [{ $eq: ["$eventType", "tool_action_completed"] }, 1, 0] } },
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
      ])
      .toArray();

    // Non-tool surfaces are filtered AFTER normalization so aliases collapse
    // first. "dashboard" in particular is the default tool for session,
    // heartbeat and analytics-plumbing events, so it would otherwise dominate
    // both the tools grid and the observed-event total.
    const measurable = rows.filter((row) => {
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

    return new Map(
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
            actionsFailed: asNumber(row.actionsFailed),
            errors: asNumber(row.errors),
            featureUses: asNumber(row.featureUses),
            activeMs: asNumber(row.activeMs),
            lastEventAt: asDate(row.lastEventAt),
          },
        ];
      })
    );
  } catch (error) {
    // Telemetry is supplementary to the credit-backed numbers; if it is
    // unreachable the reservation-derived metrics must still render.
    console.error("Tool telemetry aggregation failed:", error?.message || error);
    return new Map();
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
  const telemetry = await toolActivityFromTelemetry(range.currentStart, range.now);
  const existing = new Set(measured.map((row) => row.key));

  // Credit-measured tools gain their telemetry counters alongside the
  // reservation-derived ones. The two are kept in separate fields because they
  // measure different things: reservations count billable jobs, telemetry
  // counts observed activity.
  const measuredWithTelemetry = measured.map((row) => {
    const activity = telemetry.get(row.key);
    return activity ? { ...row, telemetry: activity, coverage: "measured" } : { ...row, telemetry: null, coverage: "measured" };
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
      message: "Measured from plugin activity. This tool does not consume credits, so there are no billable job counts.",
      uniqueUsers: activity.uniqueUsers,
      // completedJobs deliberately stays null on these rows. On credit-backed
      // tools it counts committed reservations (billable jobs); telemetry
      // counts observed actions. Publishing both under one field name would let
      // a consumer sum two different units into a meaningless total. The
      // observed counts live under `telemetry` instead.
      completedJobs: null,
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
    row.signedUp += 1; row.loggedIn += Number(item.sessions.length > 0); row.activated += Number(Boolean(item.firstCommitted));
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
  return { asOf: range.now, summary: { wallets: core.billings.length, availableCredits: core.billings.reduce((sum, row) => sum + asNumber(row.availableCredits), 0), heldCredits: core.billings.reduce((sum, row) => sum + asNumber(row.heldCredits), 0), lowCreditUsers: core.billings.filter((row) => asNumber(row.availableCredits) <= lowCreditThreshold()).length, zeroCreditUsers: core.billings.filter((row) => asNumber(row.availableCredits) === 0).length, grossRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0), refundsPaise: processedRefunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0), netRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0) - processedRefunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0), revenueStatus: captured.length ? "confirmed" : "no_confirmed_captured_payments" }, purchaseAttempts: countBy(purchases, "status"), subscriptions: countBy(core.subscriptions, "status"), starterGrants: countBy(core.starterGrants, "status"), creditMovements: countBy(ledgerRange, "reason") };
}

export async function productFeedback(days = 90) {
  const range = period(days); const [legacy, current, requests] = await Promise.all([(await getBackendFeedbackCollection()).find({}).toArray(), (await getBackendFeedbacksCollection()).find({}).toArray(), (await getBackendFeatureRequestsCollection()).find({ isDeleted: { $ne: true } }).toArray()]);
  const feedback = [...legacy.map((row) => ({ id: id(row._id), source: "legacy", authorUserId: ObjectId.isValid(String(row.userId || row.authId || "")) ? id(row.userId || row.authId) : null, rawScore: row.score === null || row.score === undefined ? null : asNumber(row.score), scale: 10, type: row.feedbackType || null, tool: row.toolId || null, comment: row.feedback || null, createdAt: row.createdAt })), ...current.map((row) => ({ id: id(row._id), source: "current", authorUserId: ObjectId.isValid(String(row.userId || "")) && !row.isAnonymous ? id(row.userId) : null, rawScore: row.rating === null || row.rating === undefined ? null : asNumber(row.rating), scale: 5, type: "rating", tool: row.path || null, comment: row.isAnonymous ? null : row.comment || null, createdAt: row.createdAt }))].map((row) => ({ ...row, score: row.rawScore === null ? null : Math.round((row.rawScore / row.scale * 5) * 10) / 10 })).filter((row) => inRange(row.createdAt, range.currentStart, range.now)).sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt));
  const requestRows = requests.filter((row) => inRange(row.createdAt, range.currentStart, range.now)); const countBy = (rows, getter) => Object.fromEntries([...rows.reduce((map, row) => { const key = String(getter(row) || "Unspecified"); map.set(key, (map.get(key) || 0) + 1); return map; }, new Map()).entries()].sort((a, b) => b[1] - a[1])); const scores = feedback.map((row) => row.score).filter(Number.isFinite);
  return { asOf: range.now, coverage: { feedbackSources: ["feedback (10-point legacy scale)", "feedbacks (5-point current scale)"], sampleSize: feedback.length, scoreNormalization: "Legacy 10-point ratings are converted to a 5-point scale before averaging." }, summary: { feedback: feedback.length, ratedResponses: scores.length, averageScore: scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length * 10) / 10 : null, featureRequests: requestRows.length, votes: requestRows.reduce((sum, row) => sum + asNumber(row.votes), 0) }, ratingDistribution: countBy(feedback.filter((row) => Number.isFinite(row.score)), (row) => row.score), feedbackByTool: countBy(feedback, (row) => row.tool), requestStatus: countBy(requestRows, (row) => row.status), requestBoards: countBy(requestRows, (row) => row.board), recentFeedback: feedback.slice(0, 25), topRequests: requestRows.sort((a, b) => asNumber(b.votes) - asNumber(a.votes) || asDate(b.createdAt) - asDate(a.createdAt)).slice(0, 25).map((row) => ({ id: id(row._id), authorUserId: ObjectId.isValid(String(row.authorId || "")) ? id(row.authorId) : null, title: row.title, status: row.status, board: row.board, votes: asNumber(row.votes), comments: asNumber(row.commentsCount), createdAt: row.createdAt })) };
}

export async function productDataHealth(newsletterConfigured = false) {
  const core = await loadCore(); const latest = (rows, fields) => rows.reduce((result, row) => { for (const field of fields) { const value = asDate(row[field]); if (value && (!result || value > result)) result = value; } return result; }, null);
  const telemetry = await telemetryHealth(new Date());
  telemetry.ageHours = telemetry.latestAt ? Math.round(((Date.now() - telemetry.latestAt) / 36e5) * 10) / 10 : null;
  const completedSessions = core.sessions.filter(successfulSession).length; const incompleteLinkedSessions = core.sessions.filter((row) => row.user && !successfulSession(row)).length; const terminal = core.reservations.filter(terminalReservation); const attributed = terminal.filter((row) => row.toolCode || row.featureCode).length;
  const capturedPurchaseIds = new Set(core.purchases.filter((row) => row.status === "captured").map((row) => id(row._id)));
  const processedRefunds = core.refunds.filter((row) => row.status === "processed");
  const unmatchedProcessedRefunds = processedRefunds.filter((row) => !capturedPurchaseIds.has(id(row.purchase))).length;
  return { ok: true, asOf: new Date(), components: { backendDatabase: { status: "healthy", users: core.users.length }, newsletter: { status: newsletterConfigured ? "configured" : "unavailable" }, telemetry }, freshness: { users: latest(core.users, ["updatedAt", "createdAt"]), sessions: latest(core.sessions, ["completedAt", "createdAt"]), ledgers: latest(core.ledgers, ["createdAt"]), reservations: latest(core.reservations, ["updatedAt", "createdAt"]), purchases: latest(core.purchases, ["updatedAt", "createdAt"]) }, coverage: { wallets: { users: core.users.length, initialized: core.billings.length, missing: core.users.length - core.billings.length, percent: percent(core.billings.length, core.users.length) }, sessions: { completed: completedSessions, incompleteLinked: incompleteLinkedSessions }, identityJoins: { sessionsWithoutUser: core.sessions.filter((row) => !row.user).length, reservationsWithoutUser: core.reservations.filter((row) => !row.user).length }, toolAttribution: { terminalJobs: terminal.length, attributed, unattributed: terminal.length - attributed, percent: percent(attributed, terminal.length) }, revenue: { capturedPurchases: capturedPurchaseIds.size, processedRefunds: processedRefunds.length, unmatchedProcessedRefunds, message: unmatchedProcessedRefunds ? `${unmatchedProcessedRefunds} processed refunds do not match a captured purchase record.` : capturedPurchaseIds.size ? "Confirmed captured payments are available." : "No confirmed captured payments are present." } } };
}
