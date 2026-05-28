import { getEventsCollection } from "./db.js";
import dotenv from "dotenv";

dotenv.config({ path: "../.env" });

async function run() {
  try {
    const events = await getEventsCollection();
    const sample = await events.find({ "user": { $exists: true } }).sort({ eventAt: -1 }).limit(5).toArray();
    console.log("Sample events with user object:");
    for (const s of sample) {
      console.log(JSON.stringify(s.user, null, 2));
    }
    
    // Let's also see if we have ANY document with 'credits' or 'creditsRemaining' in 'user'
    const hasCredits = await events.findOne({ "user.creditsRemaining": { $exists: true } });
    console.log("\nAny event with user.creditsRemaining?", hasCredits ? "Yes" : "No");
    if (hasCredits) {
      console.log(JSON.stringify(hasCredits.user, null, 2));
    }
  } catch (err) {
    console.error(err);
  }
  process.exit(0);
}

run();
