import { getAttributionCampaignsCollection } from "./db.js";

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
