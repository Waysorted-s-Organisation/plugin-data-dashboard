import { getEventsCollection, getDb } from "./src/db.js";
import dotenv from "dotenv";

dotenv.config();

const ignoredEventTypes = [
  "session_heartbeat",
  "plugin_message",
  "analytics_transport_updated",
  "tool_context_changed",
  "backend_operation"
];

async function cleanDatabase() {
  try {
    const collection = await getEventsCollection();
    
    console.log("Starting DB cleanup for noisy events...");
    const result = await collection.deleteMany({
      eventType: { $in: ignoredEventTypes }
    });
    
    console.log(`Successfully deleted ${result.deletedCount} noisy events from the database.`);
  } catch (err) {
    console.error("Error during DB cleanup:", err);
  } finally {
    const db = await getDb();
    if (db && db.client) await db.client.close();
  }
}

cleanDatabase();
