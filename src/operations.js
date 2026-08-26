import { ObjectId } from "mongodb";

import {
  getBackendCreditLedgerCollection,
  getBackendSessionsCollection,
  getBackendUserBillingCollection,
  getBackendUsersCollection,
} from "./db.js";
import { normalizeToolCode } from "./product-metrics.js";

const DAY_MS = 24 * 60 * 60 * 1000;

function lowCreditThreshold() {
  const configured = Number(process.env.CREDIT_LOW_THRESHOLD || 20);
  return Number.isFinite(configured) && configured >= 0 ? configured : 20;
}

function id(value) {
  if (value === null || value === undefined) return null;
  return String(value);
}

function number(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function date(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function rangeStart(days) {
  if (String(days).toLowerCase() === "all") return null;
  const parsed = Number(days);
  const safeDays = [7, 30, 90].includes(parsed) ? parsed : 30;
  return new Date(Date.now() - safeDays * DAY_MS);
}

function pageOptions(query = {}) {
  const page = Math.max(1, Math.floor(number(query.page, 1)));
  const requested = Math.floor(number(query.pageSize, 25));
  const pageSize = [25, 50, 100].includes(requested) ? requested : 25;
  return { page, pageSize };
}

/**
 * The tool a ledger row belongs to, named the way every other page names it.
 *
 * This used to return the raw string off the document. Nothing collapsed the
 * aliases the rest of the dashboard already knows about — `frame_gallery`,
 * `frame-gallery` and `frames` are one product — so the credits page split one
 * tool into three rows, divided its uses and credits between them, and labelled
 * them differently from the tools page. Two pages, same reservations, different
 * answers.
 */
function normalizedTool(row) {
  const raw = row?.toolCode || row?.featureCode || null;
  if (!raw) return { key: "unattributed", label: "Unattributed" };
  const normalized = normalizeToolCode(raw);
  return { key: normalized.key, label: normalized.label };
}

function serializeLedger(row) {
  return {
    id: id(row._id),
    reason: row.reason || null,
    deltaCredits: number(row.deltaCredits),
    balanceAfter: row.balanceAfter === null || row.balanceAfter === undefined
      ? null
      : number(row.balanceAfter),
    // The label, not the object: this row is rendered directly.
    tool: normalizedTool(row).label,
    reservationId: id(row.reservation),
    createdAt: row.createdAt || null,
  };
}

async function loadConsumption({ days = "all", userIds = null } = {}) {
  const ledger = await getBackendCreditLedgerCollection();
  const start = rangeStart(days);
  const commitMatch = { reason: "reservation_commit" };
  if (start) commitMatch.createdAt = { $gte: start };
  if (Array.isArray(userIds) && userIds.length) commitMatch.user = { $in: userIds };

  const commits = await ledger.find(commitMatch, {
    projection: {
      user: 1,
      reservation: 1,
      toolCode: 1,
      featureCode: 1,
      createdAt: 1,
    },
  }).toArray();
  if (!commits.length) return [];

  const reservationIds = commits.map((row) => row.reservation).filter(Boolean);
  const lifecycleRows = reservationIds.length
    ? await ledger.find({
        reservation: { $in: reservationIds },
        reason: { $in: ["reservation_hold", "compensation_credit"] },
      }, {
        projection: {
          reservation: 1,
          reason: 1,
          deltaCredits: 1,
          toolCode: 1,
          featureCode: 1,
        },
      }).toArray()
    : [];

  const lifecycle = new Map();
  for (const row of lifecycleRows) {
    const key = id(row.reservation);
    if (!key) continue;
    const current = lifecycle.get(key) || { held: 0, compensated: 0, source: null };
    if (row.reason === "reservation_hold") {
      current.held += Math.abs(number(row.deltaCredits));
      current.source ||= row;
    } else if (row.reason === "compensation_credit") {
      current.compensated += Math.max(0, number(row.deltaCredits));
    }
    lifecycle.set(key, current);
  }

  return commits
    .map((commit) => {
      const state = lifecycle.get(id(commit.reservation)) || { held: 0, compensated: 0, source: null };
      const credits = Math.max(0, state.held - state.compensated);
      return {
        userId: id(commit.user),
        reservationId: id(commit.reservation),
        ...normalizedTool(commit.toolCode || commit.featureCode ? commit : state.source),
        credits,
        // Fully refunded: the user was charged and then made whole, so this is
        // not consumption. The users page already excludes these; counting them
        // here made the two pages disagree about the same reservation.
        compensated: state.held > 0 && credits === 0,
        createdAt: commit.createdAt || null,
      };
    });
}

function aggregateConsumption(rows) {
  const tools = new Map();
  const users = new Map();
  for (const row of rows) {
    const tool = tools.get(row.key) || {
      tool: row.key,
      toolLabel: row.label,
      completedUses: 0,
      compensatedUses: 0,
      creditsSpent: 0,
      users: new Set(),
    };
    // A fully compensated reservation is a charge that was reversed, not a use.
    // The page summary already excluded these while the per-tool rows did not,
    // so the bars added up to more than the total printed above them.
    if (row.compensated) tool.compensatedUses += 1;
    else tool.completedUses += 1;
    tool.creditsSpent += row.credits;
    if (row.userId) tool.users.add(row.userId);
    tools.set(row.key, tool);

    if (!row.userId) continue;
    const user = users.get(row.userId) || {
      creditsSpent: 0,
      completedUses: 0,
      compensatedUses: 0,
      latestCreditAt: null,
      tools: new Map(),
    };
    user.creditsSpent += row.credits;
    if (row.compensated) user.compensatedUses += 1;
    else user.completedUses += 1;
    if (!user.latestCreditAt || date(row.createdAt) > date(user.latestCreditAt)) {
      user.latestCreditAt = row.createdAt;
    }
    const userTool = user.tools.get(row.key)
      || { tool: row.key, toolLabel: row.label, creditsSpent: 0, completedUses: 0, compensatedUses: 0, latestCreditAt: null };
    // Per tool, not just per user. The caller below sorts these by recency and
    // had nothing to sort on — the field only existed on the user — so every
    // comparison was NaN and the order was whichever tool happened to be seen
    // first.
    if (!userTool.latestCreditAt || date(row.createdAt) > date(userTool.latestCreditAt)) {
      userTool.latestCreditAt = row.createdAt;
    }
    userTool.creditsSpent += row.credits;
    if (row.compensated) userTool.compensatedUses += 1;
    else userTool.completedUses += 1;
    user.tools.set(row.key, userTool);
    users.set(row.userId, user);
  }

  return {
    tools: Array.from(tools.values())
      .map((row) => ({
        tool: row.tool,
        toolLabel: row.toolLabel,
        completedUses: row.completedUses,
        compensatedUses: row.compensatedUses,
        creditsSpent: row.creditsSpent,
        userCount: row.users.size,
      }))
      .sort((a, b) => b.creditsSpent - a.creditsSpent || b.completedUses - a.completedUses),
    users,
  };
}

async function loadProfiles() {
  const [usersCollection, billingCollection] = await Promise.all([
    getBackendUsersCollection(),
    getBackendUserBillingCollection(),
  ]);
  const [users, billings] = await Promise.all([
    usersCollection.find({}, {
      projection: { email: 1, name: 1, picture: 1, creditsRemaining: 1, createdAt: 1, updatedAt: 1 },
    }).toArray(),
    billingCollection.find({}, {
      projection: {
        user: 1,
        availableCredits: 1,
        heldCredits: 1,
        lifetimePurchasedCredits: 1,
        lifetimeBonusCredits: 1,
        lifetimeSpentCredits: 1,
        lifetimeRefundedCredits: 1,
        subscriptionStatus: 1,
        subscriptionPlanCode: 1,
        pricingTier: 1,
        pricingCountry: 1,
        updatedAt: 1,
      },
    }).toArray(),
  ]);
  const billingByUser = new Map(billings.map((row) => [id(row.user), row]));
  return users.map((user) => ({ user, billing: billingByUser.get(id(user._id)) || null }));
}

function publicProfile(profile, consumption) {
  const { user, billing } = profile;
  const tools = consumption
    ? Array.from(consumption.tools.values()).sort(
        (a, b) => b.creditsSpent - a.creditsSpent || b.completedUses - a.completedUses
      )
    : [];
  return {
    id: id(user._id),
    name: user.name || null,
    email: user.email || null,
    picture: user.picture || null,
    walletStatus: billing ? "initialized" : "missing",
    availableCredits: billing ? number(billing.availableCredits) : null,
    heldCredits: billing ? number(billing.heldCredits) : null,
    lifetimeSpentCredits: billing ? number(billing.lifetimeSpentCredits) : null,
    lifetimePurchasedCredits: billing ? number(billing.lifetimePurchasedCredits) : null,
    lifetimeBonusCredits: billing ? number(billing.lifetimeBonusCredits) : null,
    lifetimeRefundedCredits: billing ? number(billing.lifetimeRefundedCredits) : null,
    subscriptionStatus: billing?.subscriptionStatus || null,
    subscriptionPlanCode: billing?.subscriptionPlanCode || null,
    pricingTier: billing?.pricingTier || null,
    pricingCountry: billing?.pricingCountry || null,
    walletUpdatedAt: billing?.updatedAt || null,
    legacyCreditsRemaining: user.creditsRemaining ?? null,
    creditsSpent: consumption?.creditsSpent || 0,
    completedUses: consumption?.completedUses || 0,
    latestCreditAt: consumption?.latestCreditAt || null,
    topTool: tools[0] || null,
  };
}

export async function creditOverview(days = 30) {
  const threshold = lowCreditThreshold();
  const [profiles, consumptionRows] = await Promise.all([
    loadProfiles(),
    loadConsumption({ days }),
  ]);
  const billings = profiles.map((row) => row.billing).filter(Boolean);
  const consumption = aggregateConsumption(consumptionRows);
  return {
    asOf: new Date().toISOString(),
    source: { database: "waysorted", collections: ["users", "userbillings", "creditledgers"] },
    threshold,
    summary: {
      users: profiles.length,
      wallets: billings.length,
      walletsMissing: profiles.length - billings.length,
      totalAvailableCredits: billings.reduce((sum, row) => sum + number(row.availableCredits), 0),
      totalHeldCredits: billings.reduce((sum, row) => sum + number(row.heldCredits), 0),
      creditsSpentInRange: consumptionRows.reduce((sum, row) => sum + row.credits, 0),
      // Excludes fully compensated reservations: the user was charged and then
      // made whole, so it is not consumption. The users page already excluded
      // these, so counting them here made the two pages disagree about the same
      // reservation. The rows themselves are kept — a tool whose uses are all
      // being refunded is a signal worth seeing, not one to hide.
      completedUsesInRange: consumptionRows.filter((row) => !row.compensated).length,
      lowCreditUsers: billings.filter((row) => number(row.availableCredits) <= threshold).length,
    },
    tools: consumption.tools,
    dataQuality: {
      toolAttributedUses: consumptionRows.filter((row) => row.key !== "unattributed").length,
      unattributedUses: consumptionRows.filter((row) => row.key === "unattributed").length,
    },
  };
}

export async function creditUsers(query = {}) {
  const threshold = lowCreditThreshold();
  const { page, pageSize } = pageOptions(query);
  const [profiles, allConsumptionRows] = await Promise.all([
    loadProfiles(),
    loadConsumption({ days: "all" }),
  ]);
  const consumption = aggregateConsumption(allConsumptionRows);
  const search = String(query.search || "").trim().toLowerCase();
  const tool = String(query.tool || "all").trim();
  const walletStatus = String(query.walletStatus || "all").trim();
  const subscriptionStatus = String(query.subscriptionStatus || "all").trim();
  const lowCredit = ["1", "true", "yes"].includes(String(query.lowCredit || "").toLowerCase());

  let rows = profiles.map((profile) => publicProfile(profile, consumption.users.get(id(profile.user._id))));
  rows = rows.filter((row) => {
    if (search && !`${row.name || ""} ${row.email || ""} ${row.id}`.toLowerCase().includes(search)) return false;
    if (walletStatus !== "all" && row.walletStatus !== walletStatus) return false;
    if (subscriptionStatus !== "all" && (row.subscriptionStatus || "inactive") !== subscriptionStatus) return false;
    if (lowCredit && (row.availableCredits === null || row.availableCredits > threshold)) return false;
    if (tool !== "all") {
      const userConsumption = consumption.users.get(row.id);
      if (!userConsumption?.tools.has(tool)) return false;
    }
    return true;
  });

  const sort = String(query.sort || "email");
  const sorters = {
    email: (a, b) => String(a.email || "").localeCompare(String(b.email || "")),
    available_desc: (a, b) => number(b.availableCredits, -1) - number(a.availableCredits, -1),
    available_asc: (a, b) => number(a.availableCredits, Number.MAX_SAFE_INTEGER) - number(b.availableCredits, Number.MAX_SAFE_INTEGER),
    spent_desc: (a, b) => number(b.lifetimeSpentCredits, -1) - number(a.lifetimeSpentCredits, -1),
    recent: (a, b) => number(date(b.latestCreditAt)?.getTime()) - number(date(a.latestCreditAt)?.getTime()),
  };
  rows.sort(sorters[sort] || sorters.email);
  const total = rows.length;
  const start = (page - 1) * pageSize;
  return {
    asOf: new Date().toISOString(),
    threshold,
    items: rows.slice(start, start + pageSize),
    pagination: { page, pageSize, total, pages: Math.max(1, Math.ceil(total / pageSize)) },
  };
}

async function resolveUser(userId, email = null) {
  const users = await getBackendUsersCollection();
  let user = null;
  if (userId && ObjectId.isValid(String(userId))) {
    user = await users.findOne({ _id: new ObjectId(String(userId)) });
  }
  if (!user && email) {
    user = await users.findOne({ email: String(email).trim().toLowerCase() });
  }
  return user;
}

export async function creditUserDetail(userId, days = 30) {
  const user = await resolveUser(userId);
  if (!user) return null;
  const [billingCollection, ledger, consumptionRows] = await Promise.all([
    getBackendUserBillingCollection(),
    getBackendCreditLedgerCollection(),
    loadConsumption({ days, userIds: [user._id] }),
  ]);
  const [billing, ledgerRows] = await Promise.all([
    billingCollection.findOne({ user: user._id }),
    ledger.find({ user: user._id }).sort({ createdAt: -1 }).limit(100).toArray(),
  ]);
  const aggregate = aggregateConsumption(consumptionRows);
  return {
    asOf: new Date().toISOString(),
    user: publicProfile({ user, billing }, aggregate.users.get(id(user._id))),
    tools: aggregate.tools,
    ledger: ledgerRows.map(serializeLedger),
  };
}

export async function recentUsers(query = {}) {
  const { page, pageSize } = pageOptions(query);
  const start = rangeStart(query.days || 7);
  const sessions = await getBackendSessionsCollection();
  const match = { user: { $type: "objectId" }, completed: true };
  if (start) match.$or = [{ completedAt: { $gte: start } }, { completedAt: null, createdAt: { $gte: start } }];
  const sessionRows = await sessions.find(match, {
    projection: { user: 1, source: 1, completedAt: 1, createdAt: 1 },
  }).sort({ completedAt: -1, createdAt: -1 }).toArray();

  const grouped = new Map();
  for (const row of sessionRows) {
    const userId = id(row.user);
    const loginAt = row.completedAt || row.createdAt;
    const current = grouped.get(userId) || { userId, loginCount: 0, lastLoginAt: null, latestSource: "otp" };
    current.loginCount += 1;
    if (!current.lastLoginAt || date(loginAt) > date(current.lastLoginAt)) {
      current.lastLoginAt = loginAt;
      current.latestSource = row.source || (row.completedAt ? "web" : "otp");
    }
    grouped.set(userId, current);
  }

  const objectIds = Array.from(grouped.keys()).filter(ObjectId.isValid).map((value) => new ObjectId(value));
  const [usersCollection, consumptionRows] = await Promise.all([
    getBackendUsersCollection(),
    loadConsumption({ days: "all", userIds: objectIds }),
  ]);
  const users = objectIds.length
    ? await usersCollection.find({ _id: { $in: objectIds } }, { projection: { email: 1, name: 1, picture: 1 } }).toArray()
    : [];
  const usersById = new Map(users.map((row) => [id(row._id), row]));
  const consumption = aggregateConsumption(consumptionRows);
  const search = String(query.search || "").trim().toLowerCase();
  const source = String(query.source || "all").trim().toLowerCase();
  let rows = Array.from(grouped.values()).map((row) => {
    const user = usersById.get(row.userId) || {};
    const usage = consumption.users.get(row.userId);
    const topTools = usage
      ? Array.from(usage.tools.values()).sort((a, b) => date(b.latestCreditAt) - date(a.latestCreditAt))
      : [];
    const latestRow = consumptionRows
      .filter((item) => item.userId === row.userId)
      .sort((a, b) => date(b.createdAt) - date(a.createdAt))[0];
    return {
      ...row,
      name: user.name || null,
      email: user.email || null,
      picture: user.picture || null,
      latestCreditTool: latestRow?.label || topTools[0]?.toolLabel || null,
      latestCreditToolKey: latestRow?.key || topTools[0]?.tool || null,
      latestCreditAt: latestRow?.createdAt || usage?.latestCreditAt || null,
    };
  });
  rows = rows.filter((row) => {
    if (search && !`${row.name || ""} ${row.email || ""}`.toLowerCase().includes(search)) return false;
    return source === "all" || String(row.latestSource || "otp").toLowerCase() === source;
  });
  rows.sort((a, b) => date(b.lastLoginAt) - date(a.lastLoginAt));
  const total = rows.length;
  const offset = (page - 1) * pageSize;
  return {
    asOf: new Date().toISOString(),
    coverage: "Tool usage includes credit-consuming activity only.",
    summary: {
      users: total,
      logins: rows.reduce((sum, row) => sum + row.loginCount, 0),
      withCreditActivity: rows.filter((row) => row.latestCreditTool).length,
    },
    items: rows.slice(offset, offset + pageSize),
    pagination: { page, pageSize, total, pages: Math.max(1, Math.ceil(total / pageSize)) },
  };
}

export async function operationsHealth(newsletterConfigured = false) {
  const [users, billing, ledger, sessions] = await Promise.all([
    getBackendUsersCollection(),
    getBackendUserBillingCollection(),
    getBackendCreditLedgerCollection(),
    getBackendSessionsCollection(),
  ]);
  const [userCount, walletCount, latestLedger, latestSession] = await Promise.all([
    users.estimatedDocumentCount(),
    billing.estimatedDocumentCount(),
    ledger.findOne({}, { sort: { createdAt: -1 }, projection: { createdAt: 1 } }),
    sessions.findOne({ user: { $type: "objectId" } }, { sort: { completedAt: -1, createdAt: -1 }, projection: { completedAt: 1, createdAt: 1 } }),
  ]);
  return {
    ok: true,
    asOf: new Date().toISOString(),
    backendDatabase: { status: "healthy", users: userCount, wallets: walletCount },
    creditLedger: { status: "healthy", latestAt: latestLedger?.createdAt || null },
    authenticationSessions: { status: "healthy", latestAt: latestSession?.completedAt || latestSession?.createdAt || null },
    newsletterIntegration: { status: newsletterConfigured ? "configured" : "not_configured" },
  };
}

export async function newsletterCreditProfile(externalUserId, email) {
  const user = await resolveUser(externalUserId, email);
  if (!user) {
    return {
      billing: null,
      credit_activity: { matched: false, tools: [], recent: [] },
      data_coverage: { waysorted_user: false, billing_wallet: false, message: "No matching Waysorted user was found." },
    };
  }
  const [billingCollection, ledger, consumptionRows] = await Promise.all([
    getBackendUserBillingCollection(),
    getBackendCreditLedgerCollection(),
    loadConsumption({ days: "all", userIds: [user._id] }),
  ]);
  const [billing, recentLedger] = await Promise.all([
    billingCollection.findOne({ user: user._id }),
    ledger.find({ user: user._id }).sort({ createdAt: -1 }).limit(20).toArray(),
  ]);
  const aggregate = aggregateConsumption(consumptionRows);
  const profile = publicProfile({ user, billing }, aggregate.users.get(id(user._id)));
  return {
    billing: {
      walletStatus: profile.walletStatus,
      availableCredits: profile.availableCredits,
      heldCredits: profile.heldCredits,
      lifetimeSpentCredits: profile.lifetimeSpentCredits,
      subscriptionStatus: profile.subscriptionStatus,
      subscriptionPlanCode: profile.subscriptionPlanCode,
      updatedAt: profile.walletUpdatedAt,
    },
    credit_activity: {
      matched: true,
      tools: aggregate.tools,
      recent: recentLedger.map(serializeLedger),
      latestAt: profile.latestCreditAt,
    },
    data_coverage: {
      waysorted_user: true,
      billing_wallet: Boolean(billing),
      message: billing
        ? "Newsletter identity is linked to the Waysorted billing wallet and credit ledger."
        : "Waysorted user found, but the billing wallet is not initialized.",
    },
  };
}
