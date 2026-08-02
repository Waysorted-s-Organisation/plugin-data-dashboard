import assert from "node:assert/strict";
import test from "node:test";

import {
  buildRecipientNameMap,
  enrichCampaignRecipients,
} from "../src/newsletter-recipients.js";


test("buildRecipientNameMap keeps a separate verified name per email", () => {
  const names = buildRecipientNameMap([
    { email: "ANSH@example.com", name: "  Ansh Bhatt  " },
    { email: "aviral@example.com", name: "Aviral Garg" },
    { email: "missing@example.com", name: "" },
  ]);

  assert.deepEqual(names, {
    "ansh@example.com": "Ansh Bhatt",
    "aviral@example.com": "Aviral Garg",
  });
});


test("custom campaign recipients are enriched independently", () => {
  const payload = enrichCampaignRecipients(
    {
      recipient_source: "custom",
      recipients: ["ansh@example.com", "aviral@example.com", "unknown@example.com"],
    },
    [
      { email: "ansh@example.com", name: "Ansh Bhatt" },
      { email: "aviral@example.com", name: "Aviral Garg" },
    ]
  );

  assert.deepEqual(payload.recipients, [
    { email: "ansh@example.com", name: "Ansh Bhatt" },
    { email: "aviral@example.com", name: "Aviral Garg" },
    { email: "unknown@example.com" },
  ]);
  assert.equal(payload.recipient_name_map["ansh@example.com"], "Ansh Bhatt");
  assert.equal(payload.recipient_name_map["aviral@example.com"], "Aviral Garg");
});


test("an explicit recipient name is not overwritten", () => {
  const payload = enrichCampaignRecipients(
    {
      recipient_source: "custom",
      recipients: [{ email: "person@example.com", name: "Team Nickname" }],
    },
    [{ email: "person@example.com", name: "Database Name" }]
  );

  assert.equal(payload.recipients[0].name, "Team Nickname");
});
