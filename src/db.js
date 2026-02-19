import { MongoClient } from "mongodb";

let cachedClient = null;
let cachedDb = null;

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export async function getDb() {
  if (cachedDb) return cachedDb;

  const uri = requiredEnv("MONGODB_URI");
  const dbName = process.env.MONGODB_DB || "plugin_data_dashboard";

  if (!cachedClient) {
    cachedClient = new MongoClient(uri);
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
