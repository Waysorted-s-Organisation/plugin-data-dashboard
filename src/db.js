import { MongoClient } from "mongodb";

let cachedClient = null;
let cachedDb = null;
let cachedBackendClient = null;
let cachedBackendDb = null;

function getMongoUri() {
  const candidates = [
    process.env.MONGODB_URI,
    process.env.NEXT_PUBLIC_MONGODB_URI_TOOLS,
    process.env.NEXT_PUBLIC_MONGODB_URI,
    process.env.MONGO_URI,
    process.env.MONGO_URL,
  ];

  const uri = candidates.find((value) => typeof value === "string" && value.trim());
  if (!uri) {
    throw new Error(
      "Missing Mongo URI. Set one of: MONGODB_URI, NEXT_PUBLIC_MONGODB_URI_TOOLS, NEXT_PUBLIC_MONGODB_URI, MONGO_URI, MONGO_URL"
    );
  }

  return uri.trim();
}

function dbNameFromUri(uri) {
  try {
    const withoutParams = uri.split("?")[0];
    const slashIndex = withoutParams.lastIndexOf("/");
    if (slashIndex < 0) return null;
    const dbName = withoutParams.slice(slashIndex + 1).trim();
    if (!dbName || dbName.toLowerCase() === "admin") return null;
    return dbName;
  } catch (_err) {
    return null;
  }
}

function getDbName(uri) {
  const explicit =
    process.env.MONGODB_DB ||
    process.env.MONGODB_DATABASE ||
    process.env.NEXT_PUBLIC_MONGODB_DB;

  if (explicit && explicit.trim()) {
    return explicit.trim();
  }

  const fromUri = dbNameFromUri(uri);
  return fromUri || "plugin_data_dashboard";
}

export async function getDb() {
  if (cachedDb) return cachedDb;

  const uri = getMongoUri();
  const dbName = getDbName(uri);

  if (!cachedClient) {
    cachedClient = new MongoClient(uri, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 10000,
    });
    await cachedClient.connect();
  }

  cachedDb = cachedClient.db(dbName);
  return cachedDb;
}

function getBackendMongoUri() {
  const uri = String(process.env.BACKEND_MONGODB_URI || "").trim();
  if (!uri) {
    const error = new Error("BACKEND_MONGODB_URI is not configured");
    error.code = "BACKEND_DATABASE_NOT_CONFIGURED";
    throw error;
  }
  return uri;
}

export async function getBackendDb() {
  if (cachedBackendDb) return cachedBackendDb;

  const uri = getBackendMongoUri();
  const dbName = String(process.env.BACKEND_MONGODB_DB || "waysorted").trim();
  if (!cachedBackendClient) {
    cachedBackendClient = new MongoClient(uri, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 10000,
    });
    await cachedBackendClient.connect();
  }
  cachedBackendDb = cachedBackendClient.db(dbName);
  return cachedBackendDb;
}

export async function getBackendUsersCollection() {
  return (await getBackendDb()).collection(
    String(process.env.BACKEND_USERS_COLLECTION || "users").trim()
  );
}

export async function getBackendUserBillingCollection() {
  return (await getBackendDb()).collection(
    String(process.env.BACKEND_USER_BILLING_COLLECTION || "userbillings").trim()
  );
}

export async function getBackendCreditLedgerCollection() {
  return (await getBackendDb()).collection(
    String(process.env.BACKEND_CREDIT_LEDGER_COLLECTION || "creditledgers").trim()
  );
}

export async function getBackendSessionsCollection() {
  return (await getBackendDb()).collection(
    String(process.env.BACKEND_SESSIONS_COLLECTION || "sessions").trim()
  );
}

function backendCollection(envName, fallback) {
  return getBackendDb().then((db) => db.collection(
    String(process.env[envName] || fallback).trim()
  ));
}

export const getBackendUsageReservationsCollection = () =>
  backendCollection("BACKEND_USAGE_RESERVATIONS_COLLECTION", "usagereservations");
export const getBackendStarterGrantsCollection = () =>
  backendCollection("BACKEND_STARTER_GRANTS_COLLECTION", "startergrants");
export const getBackendPurchasesCollection = () =>
  backendCollection("BACKEND_PURCHASES_COLLECTION", "purchases");
export const getBackendSubscriptionsCollection = () =>
  backendCollection("BACKEND_SUBSCRIPTIONS_COLLECTION", "subscriptions");
export const getBackendRefundsCollection = () =>
  backendCollection("BACKEND_REFUNDS_COLLECTION", "refunds");
export const getBackendFeedbackCollection = () =>
  backendCollection("BACKEND_FEEDBACK_COLLECTION", "feedback");
export const getBackendFeedbacksCollection = () =>
  backendCollection("BACKEND_FEEDBACKS_COLLECTION", "feedbacks");
export const getBackendFeatureRequestsCollection = () =>
  backendCollection("BACKEND_FEATURE_REQUESTS_COLLECTION", "featurerequests");
export const getBackendToolsCollection = () =>
  backendCollection("BACKEND_TOOLS_COLLECTION", "tools");

export async function closeDb() {
  if (cachedClient) {
    await cachedClient.close();
  }
  cachedClient = null;
  cachedDb = null;
  if (cachedBackendClient) {
    await cachedBackendClient.close();
  }
  cachedBackendClient = null;
  cachedBackendDb = null;
}

export async function getEventsCollection() {
  const db = await getDb();
  return db.collection("plugin_analytics_events");
}

export async function getEngagementCollection() {
  const db = await getDb();
  return db.collection("plugin_engagement");
}

export async function getSnapshotsCollection() {
  const db = await getDb();
  return db.collection("stats_snapshots");
}

export async function getAttributionCampaignsCollection() {
  const db = await getDb();
  return db.collection("attribution_campaigns");
}

export async function ensureIndexes() {
  const events = await getEventsCollection();
  const attributionCampaigns = await getAttributionCampaignsCollection();

  await Promise.all([
    events.createIndex({ eventId: 1 }, { unique: true, sparse: true }),
    events.createIndex({ eventAt: -1 }),
    events.createIndex({ sessionId: 1, eventAt: 1 }),
    events.createIndex({ eventType: 1, eventAt: -1 }),
    events.createIndex({ tool: 1, eventAt: -1 }),
    events.createIndex({ "payload.action": 1, eventAt: -1 }),
    events.createIndex({ "payload.messageType": 1, eventAt: -1 }),
    events.createIndex({ "payload.type": 1, eventAt: -1 }),
    events.createIndex({ "payload.interactionAction": 1, eventAt: -1 }),
    events.createIndex({ "user.userId": 1, eventAt: -1 }),
    events.createIndex({ "user.anonymousId": 1, eventAt: -1 }),
    events.createIndex({ source: 1, eventAt: -1 }),
    attributionCampaigns.createIndex(
      { utmSource: 1, utmCampaign: 1 },
      { unique: true }
    ),
    attributionCampaigns.createIndex({ createdAt: -1 }),
  ]);
}
