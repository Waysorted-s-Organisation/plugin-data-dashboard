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
const SEMANTIC_EVENT_TYPES = ["plugin_session_started", "plugin_session_ended", "tool_opened", "tool_closed", "tool_action_started", "tool_action_completed", "tool_action_failed", "feature_used", "active_tool_time", "favorite_changed", "billing_cta_viewed", "billing_cta_clicked", "user_facing_error_displayed", "feedback_submitted"];

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
    if (!latestAt) return { status: "unavailable", latestAt: null, healthyDays: 0, message: "No verified semantic plugin events are available." };
    if (now - latestAt > 2 * 60 * 60 * 1000) return { status: "stale", latestAt, earliestAt, healthyDays: activeDays.length, message: "Non-credit behavior metrics are hidden because semantic telemetry is stale." };
    if (!earliestAt || now - earliestAt < 7 * DAY_MS || activeDays.length < 7) return { status: "warming_up", latestAt, earliestAt, healthyDays: activeDays.length, message: `Semantic telemetry is current but has ${activeDays.length} of 7 required coverage days.` };
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

async function loadCore() {
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
  return { users, billings, sessions, reservations, ledgers, purchases, subscriptions, refunds, starterGrants };
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
  const distinctDays = new Set(currentSessions.map((row) => loginAt(row).toISOString().slice(0, 10)));
  const lastLogin = sessions.at(-1) || null;
  const firstCommitted = committed[0] || null;
  const captured = purchases.filter((row) => row.status === "captured");
  const lastLoginDate = lastLogin ? loginAt(lastLogin) : null;
  const ageSinceLogin = lastLoginDate ? Math.floor((range.now - lastLoginDate) / DAY_MS) : null;
  const segments = [];
  if (inRange(user.createdAt, range.currentStart, range.now)) segments.push("new");
  if (!firstCommitted) segments.push("not_activated");
  if (firstCommitted) segments.push("activated");
  if (currentCommitted.length >= 3) segments.push("engaged");
  if (distinctDays.size >= 2) segments.push("returning");
  if (billing && asNumber(billing.availableCredits) <= lowCreditThreshold()) segments.push("low_credit");
  if (firstCommitted && (ageSinceLogin === null || ageSinceLogin >= 14)) segments.push("at_risk");
  if (ageSinceLogin === null || ageSinceLogin >= 30) segments.push("dormant");
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
  const topTool = [...tools.values()].sort((a, b) => b.completed - a.completed || b.credits - a.credits)[0] || null;
  return { userId, billing, sessions, reservations, committed, purchases, captured, currentSessions, currentCommitted, distinctDays, lastLogin, lastLoginDate, firstCommitted, segments, topTool };
}

function periodSummary(core, start, end) {
  const sessions = core.sessions.filter(successfulSession).filter((row) => inRange(loginAt(row), start, end));
  const compensated = new Set(core.ledgers.filter((row) => row.reason === "compensation_credit").map((row) => id(row.reservation)).filter(Boolean));
  const reservations = core.reservations.filter((row) => row.status === "committed" && !compensated.has(id(row._id)) && inRange(reservationAt(row), start, end));
  const newUsers = core.users.filter((row) => inRange(row.createdAt, start, end));
  const activeUsers = new Set(sessions.map((row) => id(row.user))).size;
  const daysByUser = new Map();
  for (const session of sessions) {
    const key = id(session.user);
    if (!daysByUser.has(key)) daysByUser.set(key, new Set());
    daysByUser.get(key).add(loginAt(session).toISOString().slice(0, 10));
  }
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
    completedJobs: reservations.length,
    creditsConsumed: reservations.reduce((sum, row) => sum + asNumber(row.creditsReserved), 0),
    grossRevenuePaise: captured.reduce((sum, row) => sum + asNumber(row.amountPaise), 0),
    refundsPaise: refunds.reduce((sum, row) => sum + asNumber(row.amountPaise), 0),
  };
}

export async function productSummary(days = 30) {
  const range = period(days); const core = await loadCore();
  const current = periodSummary(core, range.currentStart, range.now);
  const previous = periodSummary(core, range.previousStart, range.currentStart);
  const metrics = Object.fromEntries(Object.entries(current).map(([key, value]) => [key, { value, previous: previous[key], change: change(value, previous[key]) }]));
  metrics.netRevenuePaise = { value: current.grossRevenuePaise - current.refundsPaise, previous: previous.grossRevenuePaise - previous.refundsPaise, change: change(current.grossRevenuePaise - current.refundsPaise, previous.grossRevenuePaise - previous.refundsPaise) };
  const missingWallets = core.users.length - core.billings.length;
  const expired = core.reservations.filter((row) => row.status === "expired" && inRange(row.updatedAt || row.expiresAt, range.currentStart, range.now)).length;
  const pendingPayments = core.purchases.filter((row) => ["pending", "failed"].includes(row.status)).length;
  const unattributed = core.reservations.filter((row) => terminalReservation(row) && !row.toolCode && !row.featureCode).length;
  const capturedPurchaseIds = new Set(core.purchases.filter((row) => row.status === "captured").map((row) => id(row._id)));
  const unmatchedRefunds = core.refunds.filter((row) => row.status === "processed" && !capturedPurchaseIds.has(id(row.purchase))).length;
  const telemetry = await telemetryHealth(range.now);
  const attention = [
    missingWallets ? { severity: "warning", title: `${missingWallets} users have no billing wallet`, detail: "Their balance cannot be shown until the wallet is initialized.", href: "/users.html?wallet=missing" } : null,
    expired ? { severity: "warning", title: `${expired} tool jobs expired`, detail: "Review the affected tools and processing flow.", href: "/tools.html?status=expired" } : null,
    pendingPayments ? { severity: "warning", title: `${pendingPayments} payment attempts need context`, detail: "Pending and failed attempts are not counted as revenue.", href: "/credits.html" } : null,
    unattributed ? { severity: "info", title: `${unattributed} terminal jobs are unattributed`, detail: "They remain visible but are not assigned to a product tool.", href: "/data-health.html" } : null,
    unmatchedRefunds ? { severity: "warning", title: `${unmatchedRefunds} processed refunds lack a captured purchase match`, detail: "They are excluded from current revenue until the commercial records are reconciled.", href: "/data-health.html" } : null,
    telemetry.status !== "healthy" ? { severity: "critical", title: "Non-credit behavior tracking is stale", detail: telemetry.message, href: "/data-health.html" } : null,
  ].filter(Boolean);
  const changed = Object.entries(metrics).filter(([key]) => !["grossRevenuePaise", "refundsPaise"].includes(key)).sort((a, b) => Math.abs(b[1].change) - Math.abs(a[1].change)).slice(0, 3).map(([key, value]) => ({ metric: key, direction: value.change > 0 ? "up" : value.change < 0 ? "down" : "flat", change: value.change, current: value.value, previous: value.previous }));
  return { asOf: range.now, period: { days: range.days, currentStart: range.currentStart, previousStart: range.previousStart }, coverage: { activation: "Credited tool activation only", telemetry }, metrics, whatChanged: changed, needsAttention: attention };
}

export async function productUsers(query = {}, newsletterByEmail = new Map()) {
  const range = period(query.days || 30); const core = await loadCore(); const indexed = indexCore(core);
  const search = String(query.search || "").trim().toLowerCase(); const segment = String(query.segment || "all");
  const country = String(query.country || "all"); const source = String(query.source || "all"); const tool = String(query.tool || "all");
  const wallet = String(query.wallet || "all"); const subscription = String(query.subscription || "all"); const newsletter = String(query.newsletter || "all");
  let rows = core.users.map((user) => {
    const facts = userFacts(user, indexed, range); const billing = facts.billing;
    const latestSource = facts.lastLogin?.source || null;
    const latestCountry = facts.lastLogin?.countryCode || billing?.pricingCountry || null;
    const newsletterProfile = newsletterByEmail.get(String(user.email || "").toLowerCase()) || null;
    return { id: facts.userId, name: user.name || null, email: user.email || null, picture: user.picture || null, joinedAt: user.createdAt || null, segments: facts.segments, lifecycleStage: facts.captured.length ? "customer" : facts.distinctDays.size >= 2 ? "returning" : facts.firstCommitted ? "activated" : facts.lastLogin ? "logged_in" : "signed_up", lastLoginAt: facts.lastLoginDate, latestLoginSource: latestSource, country: latestCountry, successfulLogins: facts.sessions.length, completedJobs: facts.committed.length, jobsInRange: facts.currentCommitted.length, topTool: facts.topTool, toolKeys: [...new Set(facts.committed.map((row) => normalizeToolCode(row.toolCode, row.featureCode).key))], walletStatus: billing ? "initialized" : "missing", availableCredits: billing ? asNumber(billing.availableCredits) : null, heldCredits: billing ? asNumber(billing.heldCredits) : null, subscriptionStatus: billing?.subscriptionStatus || null, subscriptionPlan: billing?.subscriptionPlanCode || null, newsletter: newsletterProfile ? { id: newsletterProfile.id, status: newsletterProfile.status, tags: newsletterProfile.tags || [] } : null };
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
  const sorters = { recent: (a, b) => (asDate(b.lastLoginAt) || 0) - (asDate(a.lastLoginAt) || 0), joined: (a, b) => (asDate(b.joinedAt) || 0) - (asDate(a.joinedAt) || 0), jobs: (a, b) => b.completedJobs - a.completedJobs, credits: (a, b) => asNumber(a.availableCredits, -1) - asNumber(b.availableCredits, -1) };
  rows.sort(sorters[String(query.sort || "recent")] || sorters.recent);
  const { page, pageSize } = pageOptions(query); const total = rows.length; const offset = (page - 1) * pageSize;
  const segmentCounts = {}; for (const row of rows) for (const item of row.segments) segmentCounts[item] = (segmentCounts[item] || 0) + 1;
  return { asOf: range.now, coverage: { behavior: "Successful authentication and credited tool activity", newsletter: newsletterByEmail.size ? "connected" : "unavailable" }, summary: { users: total, segmentCounts }, facets, items: rows.slice(offset, offset + pageSize), pagination: { page, pageSize, total, pages: Math.max(1, Math.ceil(total / pageSize)) } };
}

export async function productUserDetail(userId) {
  if (!ObjectId.isValid(String(userId))) return null;
  const core = await loadCore(); const indexed = indexCore(core); const user = core.users.find((row) => id(row._id) === String(userId)); if (!user) return null;
  const range = period(30); const facts = userFacts(user, indexed, range); const billing = facts.billing;
  const [legacyFeedback, feedback, requests] = await Promise.all([
    (await getBackendFeedbackCollection()).find({ $or: [{ authId: String(userId) }, { userId: new ObjectId(String(userId)) }] }, { projection: { feedbackType: 1, score: 1, toolId: 1, createdAt: 1 } }).sort({ createdAt: -1 }).limit(30).toArray(),
    (await getBackendFeedbacksCollection()).find({ userId: new ObjectId(String(userId)) }, { projection: { rating: 1, comment: 1, path: 1, isAnonymous: 1, createdAt: 1 } }).sort({ createdAt: -1 }).limit(30).toArray(),
    (await getBackendFeatureRequestsCollection()).find({ authorId: String(userId), isDeleted: { $ne: true } }, { projection: { title: 1, status: 1, board: 1, votes: 1, commentsCount: 1, createdAt: 1 } }).sort({ createdAt: -1 }).limit(30).toArray(),
  ]);
  const safePurchase = (row) => ({ id: id(row._id), kind: row.kind, productCode: row.productCode, status: row.status, amount: asNumber(row.amountPaise), currency: row.currency || "INR", createdAt: row.createdAt });
  return { asOf: range.now, user: { id: facts.userId, name: user.name || null, email: user.email || null, picture: user.picture || null, joinedAt: user.createdAt, favorites: user.favorites || [], segments: facts.segments, lifecycleStage: facts.captured.length ? "customer" : facts.distinctDays.size >= 2 ? "returning" : facts.firstCommitted ? "activated" : facts.lastLogin ? "logged_in" : "signed_up" }, billing: billing ? { availableCredits: asNumber(billing.availableCredits), heldCredits: asNumber(billing.heldCredits), lifetimeSpentCredits: asNumber(billing.lifetimeSpentCredits), lifetimePurchasedCredits: asNumber(billing.lifetimePurchasedCredits), lifetimeBonusCredits: asNumber(billing.lifetimeBonusCredits), subscriptionStatus: billing.subscriptionStatus, subscriptionPlan: billing.subscriptionPlanCode, pricingTier: billing.pricingTier, pricingCountry: billing.pricingCountry } : null, sessions: facts.sessions.slice(-50).reverse().map((row) => ({ source: row.source || "unknown", country: row.countryCode || null, completedAt: loginAt(row) })), reservations: facts.reservations.slice(-100).reverse().map((row) => ({ id: id(row._id), ...normalizeToolCode(row.toolCode, row.featureCode), rawToolCode: row.toolCode || null, featureCode: row.featureCode || null, status: indexed.compensatedReservations.has(id(row._id)) ? "compensated" : row.status, credits: asNumber(row.creditsReserved), processor: row.processor || null, occurredAt: reservationAt(row), durationMs: row.status === "committed" && row.committedAt && row.createdAt ? asDate(row.committedAt) - asDate(row.createdAt) : null })), ledger: (indexed.ledgers.get(facts.userId) || []).slice().sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt)).slice(0, 100).map((row) => ({ reason: row.reason, deltaCredits: asNumber(row.deltaCredits), tool: normalizeToolCode(row.toolCode, row.featureCode), createdAt: row.createdAt })), purchases: facts.purchases.map(safePurchase), subscriptions: (indexed.subscriptions.get(facts.userId) || []).map((row) => ({ planCode: row.planCode, status: row.status, currentPeriodStart: row.currentPeriodStart, currentPeriodEnd: row.currentPeriodEnd, nextChargeAt: row.nextChargeAt })), refunds: (indexed.refunds.get(facts.userId) || []).map((row) => ({ status: row.status, amountPaise: asNumber(row.amountPaise), reason: row.reason || null, createdAt: row.createdAt })), feedback: [...legacyFeedback.map((row) => ({ source: "legacy", score: row.score ?? null, scale: 10, type: row.feedbackType || null, tool: row.toolId || null, createdAt: row.createdAt })), ...feedback.map((row) => ({ source: "current", score: row.rating ?? null, rating: row.rating ?? null, scale: 5, comment: row.isAnonymous ? null : row.comment || null, path: row.path || null, createdAt: row.createdAt }))].sort((a, b) => asDate(b.createdAt) - asDate(a.createdAt)), featureRequests: requests.map((row) => ({ title: row.title, status: row.status, board: row.board, votes: asNumber(row.votes), comments: asNumber(row.commentsCount), createdAt: row.createdAt })), coverage: { nonCreditToolActivity: "unavailable", message: "This profile includes successful logins and credited tool activity. Non-credit plugin telemetry is hidden until coverage is healthy." } };
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
  const existing = new Set(measured.map((row) => row.key));
  const unavailable = catalog.map((row) => ({ key: normalizeToolCode(row.slug).key, label: row.name || normalizeToolCode(row.slug).label, category: row.category || null, catalogStatus: row.badge?.label || (row.isActive ? "Active" : "Inactive"), favorites: favoriteCounts.get(normalizeToolCode(row.slug).key) || 0, coverage: existing.has(normalizeToolCode(row.slug).key) ? "measured" : "unavailable", message: existing.has(normalizeToolCode(row.slug).key) ? null : "Usage tracking unavailable until semantic plugin telemetry is healthy." })).filter((row) => !existing.has(row.key));
  return { asOf: range.now, period: { days: range.days, start: range.currentStart }, summary: { measuredTools: measured.length, unavailableTools: unavailable.length, completedJobs: measured.reduce((sum, row) => sum + row.completedJobs, 0), expiredJobs: measured.reduce((sum, row) => sum + row.expiredJobs, 0), creditsConsumed: measured.reduce((sum, row) => sum + row.creditsConsumed, 0) }, items: [...measured, ...unavailable] };
}

export async function productToolDetail(toolCode, days = 30) {
  const payload = await productTools(days); const normalized = normalizeToolCode(toolCode).key; return payload.items.find((row) => row.key === normalized) || null;
}

export async function productLifecycle(days = 90) {
  const range = period(days); const core = await loadCore(); const indexed = indexCore(core); const cohort = core.users.filter((row) => inRange(row.createdAt, range.currentStart, range.now));
  const facts = cohort.map((user) => ({ user, facts: userFacts(user, indexed, range) }));
  const loggedIn = facts.filter((row) => row.facts.sessions.length); const activated = facts.filter((row) => row.facts.firstCommitted); const returned = facts.filter((row) => new Set(row.facts.sessions.map((session) => loginAt(session).toISOString().slice(0, 10))).size >= 2); const purchased = facts.filter((row) => row.facts.captured.length);
  const stages = [{ key: "signed_up", label: "Signed up", users: cohort.length }, { key: "logged_in", label: "Successful login", users: loggedIn.length }, { key: "activated", label: "Credited tool activation", users: activated.length }, { key: "returned", label: "Returned another day", users: returned.length }, { key: "purchased", label: "Confirmed purchase", users: purchased.length }].map((stage, index, all) => ({ ...stage, conversionFromPrevious: index ? percent(stage.users, all[index - 1].users) : 100 }));
  const loginTimes = loggedIn.map(({ user, facts: item }) => loginAt(item.sessions[0]) - asDate(user.createdAt)); const activationTimes = activated.map(({ user, facts: item }) => reservationAt(item.firstCommitted) - asDate(user.createdAt));
  const weeks = new Map();
  for (const { user, facts: item } of facts) {
    const created = asDate(user.createdAt); const monday = new Date(Date.UTC(created.getUTCFullYear(), created.getUTCMonth(), created.getUTCDate() - ((created.getUTCDay() + 6) % 7))); const key = monday.toISOString().slice(0, 10); const row = weeks.get(key) || { week: key, signedUp: 0, loggedIn: 0, activated: 0, returned7d: 0, returned30d: 0, latestSignupAt: created };
    if (created > row.latestSignupAt) row.latestSignupAt = created;
    row.signedUp += 1; row.loggedIn += Number(item.sessions.length > 0); row.activated += Number(Boolean(item.firstCommitted));
    const laterSessions = item.sessions.map(loginAt).filter((date) => date > created); row.returned7d += Number(laterSessions.some((date) => date - created >= DAY_MS && date - created <= 7 * DAY_MS)); row.returned30d += Number(laterSessions.some((date) => date - created >= DAY_MS && date - created <= 30 * DAY_MS)); weeks.set(key, row);
  }
  return { asOf: range.now, coverage: "Activation covers credited tool completions only.", stages, timing: { medianTimeToLoginMs: median(loginTimes), medianTimeToActivationMs: median(activationTimes) }, stuck: { signedUpNotLoggedIn: cohort.length - loggedIn.length, loggedInNotActivated: loggedIn.filter((row) => !row.facts.firstCommitted).length, activatedNotReturned: activated.filter((row) => new Set(row.facts.sessions.map((session) => loginAt(session).toISOString().slice(0, 10))).size < 2).length }, cohorts: [...weeks.values()].sort((a, b) => a.week.localeCompare(b.week)).map((row) => ({ week: row.week, signedUp: row.signedUp, loggedIn: row.loggedIn, activated: row.activated, returned7d: row.returned7d, returned30d: row.returned30d, activationRate: percent(row.activated, row.signedUp), return7dRate: range.now - row.latestSignupAt >= 7 * DAY_MS ? percent(row.returned7d, row.signedUp) : null, return30dRate: range.now - row.latestSignupAt >= 30 * DAY_MS ? percent(row.returned30d, row.signedUp) : null })) };
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
