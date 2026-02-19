import { MongoClient } from "mongodb";

let cachedClient = null;
let cachedDb = null;

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

export async function getEventsCollection() {
  const db = await getDb();
  return db.collection("plugin_analytics_events");
}

export async function ensureIndexes() {
  const events = await getEventsCollection();

  await Promise.all([
    events.createIndex({ eventAt: -1 }),
    events.createIndex({ sessionId: 1, eventAt: 1 }),
    events.createIndex({ eventType: 1, eventAt: -1 }),
    events.createIndex({ tool: 1, eventAt: -1 }),
    events.createIndex({ "user.userId": 1, eventAt: -1 }),
    events.createIndex({ source: 1, eventAt: -1 }),
  ]);
}
