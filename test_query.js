import { getEventsCollection, getDb } from "./src/db.js";
import dotenv from "dotenv";

dotenv.config();

async function analyzeEvents() {
  try {
    const collection = await getEventsCollection();
    
    const breakdown = await collection.aggregate([
      { $group: { _id: "$eventType", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();
    
    console.log("Event Type Breakdown:");
    for (const b of breakdown) {
      console.log(`${b._id}: ${b.count}`);
    }
  } catch (err) {
    console.error(err);
  } finally {
    const db = await getDb();
    if (db && db.client) await db.client.close();
  }
}

analyzeEvents();

analyzeEvents();
