import {
  getAttributionCampaignsCollection,
  getBackendAttributionVisitsCollection,
  getBackendPurchasesCollection,
} from "./db.js";

const DEFAULT_PUBLIC_URL = "https://www.waysorted.com";

function requiredText(value, label, maxLength) {
  const text = String(value || "").trim();
  if (!text) throw Object.assign(new Error(`${label} is required`), { statusCode: 400 });
  if (text.length > maxLength) throw Object.assign(new Error(`${label} is too long`), { statusCode: 400 });
  return text;
}

function utmValue(value, label, fallback) {
  const text = String(value || fallback || "").trim().toLowerCase();
  if (!text) throw Object.assign(new Error(`${label} is required`), { statusCode: 400 });
  if (text.length > 100 || !/^[a-z0-9][a-z0-9._~-]*$/.test(text)) {
    throw Object.assign(
      new Error(`${label} can use lowercase letters, numbers, dots, underscores, tildes, and hyphens`),
      { statusCode: 400 }
    );
  }
  return text;
}

export function publicWaysortedOrigin() {
  const configured = String(
    process.env.WAYSORTED_PUBLIC_URL || process.env.WAYSORTED_API_URL || DEFAULT_PUBLIC_URL
  ).trim();
  try {
    return new URL(configured).origin;
  } catch {
    return DEFAULT_PUBLIC_URL;
  }
}

function destinationPath(value) {
  const raw = String(value || "/payment").trim();
  const origin = publicWaysortedOrigin();
  let parsed;
  try {
    parsed = new URL(raw, `${origin}/`);
  } catch {
    throw Object.assign(new Error("Destination must be a valid Waysorted path"), { statusCode: 400 });
  }
  if (!raw.startsWith("/") || raw.startsWith("//") || parsed.origin !== origin) {
    throw Object.assign(new Error("Destination must be a path on the Waysorted website"), { statusCode: 400 });
  }
  return `${parsed.pathname}${parsed.search}${parsed.hash}`;
}

export function campaignUrl(campaign) {
  const url = new URL(campaign.destinationPath, `${publicWaysortedOrigin()}/`);
  url.searchParams.set("utm_source", campaign.utmSource);
  url.searchParams.set("utm_medium", campaign.utmMedium);
  url.searchParams.set("utm_campaign", campaign.utmCampaign);
  return url.toString();
}

function serializeCampaign(campaign) {
  return {
    id: String(campaign._id),
    name: campaign.name,
    utmSource: campaign.utmSource,
    utmMedium: campaign.utmMedium,
    utmCampaign: campaign.utmCampaign,
    destinationPath: campaign.destinationPath,
    checkoutUrl: campaignUrl(campaign),
    createdAt: campaign.createdAt,
    createdBy: campaign.createdBy || null,
  };
}

export async function listAttributionCampaigns() {
  const campaigns = await getAttributionCampaignsCollection();
  const items = await campaigns.find({}).sort({ createdAt: -1 }).limit(500).toArray();
  return { items: items.map(serializeCampaign), publicOrigin: publicWaysortedOrigin() };
}

function reportStart(days) {
  if (String(days).toLowerCase() === "all") return { days: "all", start: null };
  const parsed = Number(days);
  const safeDays = [7, 30, 90, 365].includes(parsed) ? parsed : 30;
  return {
    days: safeDays,
    start: new Date(Date.now() - safeDays * 24 * 60 * 60 * 1000),
  };
}

function campaignKey(source, campaign) {
  return `${String(source || "").trim().toLowerCase()}\u0000${String(campaign || "").trim().toLowerCase()}`;
}

function percent(numerator, denominator) {
  return denominator ? Math.round((numerator / denominator) * 1000) / 10 : 0;
}

export async function attributionCampaignReport(days = 30) {
  const range = reportStart(days);
  const [campaignsCollection, visitsCollection, purchasesCollection] = await Promise.all([
    getAttributionCampaignsCollection(),
    getBackendAttributionVisitsCollection(),
    getBackendPurchasesCollection(),
  ]);
  const campaignDocuments = await campaignsCollection.find({}).sort({ createdAt: -1 }).limit(500).toArray();
  const visitMatch = range.start ? { openedAt: { $gte: range.start } } : {};
  const purchaseMatch = {
    "attribution.utmSource": { $type: "string" },
    ...(range.start ? { createdAt: { $gte: range.start } } : {}),
  };
  const successfulStatuses = ["captured", "partially_refunded", "refunded"];
  const pendingStatuses = ["created", "pending"];
  const failedStatuses = ["failed", "cancelled"];

  const [visitRows, purchaseRows] = await Promise.all([
    visitsCollection.aggregate([
      { $match: visitMatch },
      {
        $group: {
          _id: {
            source: { $toLower: { $ifNull: ["$utmSource", ""] } },
            campaign: { $toLower: { $ifNull: ["$utmCampaign", ""] } },
          },
          opens: { $sum: 1 },
          visitorIds: { $addToSet: "$visitorId" },
        },
      },
    ]).toArray(),
    purchasesCollection.aggregate([
      { $match: purchaseMatch },
      {
        $group: {
          _id: {
            source: { $toLower: { $ifNull: ["$attribution.utmSource", ""] } },
            campaign: { $toLower: { $ifNull: ["$attribution.utmCampaign", ""] } },
            currency: { $toUpper: { $ifNull: ["$currency", "INR"] } },
          },
          checkoutAttempts: { $sum: 1 },
          successfulPurchases: { $sum: { $cond: [{ $in: ["$status", successfulStatuses] }, 1, 0] } },
          pendingAttempts: { $sum: { $cond: [{ $in: ["$status", pendingStatuses] }, 1, 0] } },
          failedAttempts: { $sum: { $cond: [{ $in: ["$status", failedStatuses] }, 1, 0] } },
          successfulVisitorIds: {
            $addToSet: {
              $cond: [
                { $in: ["$status", successfulStatuses] },
                { $ifNull: ["$attribution.visitorId", null] },
                null,
              ],
            },
          },
          netRevenueSubunits: {
            $sum: {
              $cond: [
                { $in: ["$status", successfulStatuses] },
                { $max: [{ $subtract: [{ $ifNull: ["$amountPaise", 0] }, { $ifNull: ["$refundedAmountPaise", 0] }] }, 0] },
                0,
              ],
            },
          },
        },
      },
    ]).toArray(),
  ]);

  const visitsByCampaign = new Map(
    visitRows.map((row) => [campaignKey(row._id?.source, row._id?.campaign), row]),
  );
  const purchasesByCampaign = new Map();
  for (const row of purchaseRows) {
    const key = campaignKey(row._id?.source, row._id?.campaign);
    const current = purchasesByCampaign.get(key) || {
      checkoutAttempts: 0,
      successfulPurchases: 0,
      pendingAttempts: 0,
      failedAttempts: 0,
      successfulVisitorIds: new Set(),
      revenue: [],
    };
    current.checkoutAttempts += Number(row.checkoutAttempts || 0);
    current.successfulPurchases += Number(row.successfulPurchases || 0);
    current.pendingAttempts += Number(row.pendingAttempts || 0);
    current.failedAttempts += Number(row.failedAttempts || 0);
    for (const visitorId of row.successfulVisitorIds || []) {
      if (visitorId) current.successfulVisitorIds.add(String(visitorId));
    }
    if (Number(row.netRevenueSubunits || 0)) {
      current.revenue.push({
        currency: row._id?.currency || "INR",
        amountSubunits: Number(row.netRevenueSubunits),
      });
    }
    purchasesByCampaign.set(key, current);
  }

  const allVisitors = new Set();
  const allConvertedVisitors = new Set();
  const summaryRevenue = new Map();
  const items = campaignDocuments.map((document) => {
    const campaign = serializeCampaign(document);
    const key = campaignKey(campaign.utmSource, campaign.utmCampaign);
    const visits = visitsByCampaign.get(key);
    const purchases = purchasesByCampaign.get(key);
    const visitorIds = (visits?.visitorIds || []).filter(Boolean).map(String);
    visitorIds.forEach((id) => allVisitors.add(id));
    const openedVisitorIds = new Set(visitorIds);
    const convertedVisitorIds = Array.from(purchases?.successfulVisitorIds || [])
      .filter((id) => openedVisitorIds.has(id));
    convertedVisitorIds.forEach((id) => allConvertedVisitors.add(id));
    for (const value of purchases?.revenue || []) {
      summaryRevenue.set(value.currency, (summaryRevenue.get(value.currency) || 0) + value.amountSubunits);
    }
    const uniqueVisitors = visitorIds.length;
    const convertedVisitors = convertedVisitorIds.length;
    return {
      ...campaign,
      metrics: {
        opens: Number(visits?.opens || 0),
        uniqueVisitors,
        checkoutAttempts: purchases?.checkoutAttempts || 0,
        successfulPurchases: purchases?.successfulPurchases || 0,
        convertedVisitors,
        pendingAttempts: purchases?.pendingAttempts || 0,
        failedAttempts: purchases?.failedAttempts || 0,
        conversionRate: percent(convertedVisitors, uniqueVisitors),
        revenue: [...(purchases?.revenue || [])].sort((a, b) => a.currency.localeCompare(b.currency)),
      },
    };
  });

  return {
    asOf: new Date(),
    period: { days: range.days, start: range.start },
    summary: {
      campaigns: items.length,
      opens: items.reduce((sum, item) => sum + item.metrics.opens, 0),
      uniqueVisitors: allVisitors.size,
      checkoutAttempts: items.reduce((sum, item) => sum + item.metrics.checkoutAttempts, 0),
      successfulPurchases: items.reduce((sum, item) => sum + item.metrics.successfulPurchases, 0),
      convertedVisitors: allConvertedVisitors.size,
      conversionRate: percent(allConvertedVisitors.size, allVisitors.size),
      revenue: Array.from(summaryRevenue, ([currency, amountSubunits]) => ({ currency, amountSubunits }))
        .sort((a, b) => a.currency.localeCompare(b.currency)),
    },
    items,
    publicOrigin: publicWaysortedOrigin(),
  };
}

export async function createAttributionCampaign(input, createdBy = null) {
  const document = {
    name: requiredText(input?.name, "Campaign name", 120),
    utmSource: utmValue(input?.utmSource, "UTM source"),
    utmMedium: utmValue(input?.utmMedium, "UTM medium", "referral"),
    utmCampaign: utmValue(input?.utmCampaign, "UTM campaign", "checkout"),
    destinationPath: destinationPath(input?.destinationPath),
    createdAt: new Date(),
    createdBy: createdBy || null,
  };
  const campaigns = await getAttributionCampaignsCollection();
  try {
    const result = await campaigns.insertOne(document);
    return serializeCampaign({ ...document, _id: result.insertedId });
  } catch (error) {
    if (error?.code === 11000) {
      throw Object.assign(new Error("A campaign with this source and campaign already exists"), { statusCode: 409 });
    }
    throw error;
  }
}
