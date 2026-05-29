import { MongoClient } from "mongodb";

const clientCache = new Map();
const dbCache = new Map();

function firstNonEmpty(candidates) {
  return candidates.find((value) => typeof value === "string" && value.trim()) || null;
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

function getAnalyticsMongoUri() {
  const uri = firstNonEmpty([
    process.env.MONGODB_URI,
    process.env.MONGODB_URI_TOOLS,
    process.env.NEXT_PUBLIC_MONGODB_URI_TOOLS,
    process.env.NEXT_PUBLIC_MONGODB_URI,
    process.env.MONGO_URI,
    process.env.MONGO_URL,
  ]);

  if (!uri) {
    throw new Error(
      "Missing Mongo URI. Set one of: MONGODB_URI, MONGODB_URI_TOOLS, NEXT_PUBLIC_MONGODB_URI_TOOLS, NEXT_PUBLIC_MONGODB_URI, MONGO_URI, MONGO_URL"
    );
  }

  return uri.trim();
}

function getAnalyticsDbName(uri) {
  const explicit = firstNonEmpty([
    process.env.MONGODB_DB,
    process.env.MONGODB_DATABASE,
    process.env.NEXT_PUBLIC_MONGODB_DB,
  ]);

  if (explicit) {
    return explicit.trim();
  }

  const fromUri = dbNameFromUri(uri);
  return fromUri || "plugin_data_dashboard";
}

function getBackendMongoUri() {
  const uri = firstNonEmpty([
    process.env.BACKEND_MONGODB_URI,
    process.env.WAYSORTED_BACKEND_MONGODB_URI,
    process.env.MONGODB_URI_TOOLS,
    process.env.NEXT_PUBLIC_MONGODB_URI_TOOLS,
    process.env.MONGODB_URI,
    process.env.NEXT_PUBLIC_MONGODB_URI,
    process.env.MONGO_URI,
    process.env.MONGO_URL,
  ]);

  if (!uri) {
    throw new Error(
      "Missing backend Mongo URI. Set BACKEND_MONGODB_URI, WAYSORTED_BACKEND_MONGODB_URI, MONGODB_URI_TOOLS, NEXT_PUBLIC_MONGODB_URI_TOOLS, MONGODB_URI, NEXT_PUBLIC_MONGODB_URI, MONGO_URI, or MONGO_URL"
    );
  }

  return uri.trim();
}

function getBackendDbName(uri) {
  const explicit = firstNonEmpty([
    process.env.BACKEND_MONGODB_DB,
    process.env.BACKEND_MONGODB_DATABASE,
    process.env.MONGODB_DB_TOOLS,
    process.env.NEXT_PUBLIC_MONGODB_DB_TOOLS,
  ]);

  if (explicit) {
    return explicit.trim();
  }

  const fromUri = dbNameFromUri(uri);
  if (fromUri) {
    return fromUri;
  }

  return getAnalyticsDbName(getAnalyticsMongoUri());
}

async function getDatabase(kind) {
  const uri = kind === "backend" ? getBackendMongoUri() : getAnalyticsMongoUri();
  const dbName = kind === "backend" ? getBackendDbName(uri) : getAnalyticsDbName(uri);
  const clientKey = uri;
  const dbKey = `${uri}::${dbName}`;

  if (!clientCache.has(clientKey)) {
    const client = new MongoClient(uri, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 10000,
    });
    clientCache.set(clientKey, client.connect());
  }

  const client = await clientCache.get(clientKey);

  if (!dbCache.has(dbKey)) {
    dbCache.set(dbKey, client.db(dbName));
  }

  return dbCache.get(dbKey);
}

export async function getDb() {
  return getDatabase("analytics");
}

export async function getBackendDb() {
  return getDatabase("backend");
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

export async function getBackendUsersCollection() {
  const db = await getBackendDb();
  return db.collection((process.env.BACKEND_USERS_COLLECTION || "users").trim());
}

export async function getBackendUserBillingCollection() {
  const db = await getBackendDb();
  return db.collection((process.env.BACKEND_USER_BILLING_COLLECTION || "userbillings").trim());
}

export async function getBackendCreditLedgerCollection() {
  const db = await getBackendDb();
  return db.collection((process.env.BACKEND_CREDIT_LEDGER_COLLECTION || "creditledgers").trim());
}

export async function ensureIndexes() {
  const events = await getEventsCollection();
  const snapshots = await getSnapshotsCollection();

  await Promise.all([
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
    snapshots.createIndex({ date: -1 }),
    snapshots.createIndex({ date: 1 }, { unique: true }),
  ]);
}

export async function closeDatabases() {
  const clientEntries = Array.from(clientCache.entries());
  clientCache.clear();
  dbCache.clear();

  const connectedClients = await Promise.allSettled(
    clientEntries.map(([, clientPromise]) => clientPromise)
  );

  await Promise.all(
    connectedClients
      .filter((result) => result.status === "fulfilled" && result.value)
      .map((result) => result.value.close())
  );
}
