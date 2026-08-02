function normalizedEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizedName(value) {
  return String(value || "").trim().replace(/\s+/g, " ").slice(0, 120);
}

export function buildRecipientNameMap(users = []) {
  const names = {};
  for (const user of users) {
    const email = normalizedEmail(user?.email);
    const name = normalizedName(user?.name);
    if (email && name) names[email] = name;
  }
  return names;
}

export function enrichCampaignRecipients(payload, users = []) {
  const output = { ...(payload || {}) };
  const nameMap = buildRecipientNameMap(users);
  output.recipient_name_map = nameMap;

  if (String(output.recipient_source || "").toLowerCase() === "custom") {
    output.recipients = (output.recipients || []).map((recipient) => {
      if (typeof recipient === "string") {
        const email = normalizedEmail(recipient);
        return nameMap[email]
          ? { email, name: nameMap[email] }
          : { email };
      }
      const email = normalizedEmail(recipient?.email);
      const existingName = normalizedName(
        recipient?.name || recipient?.display_name
      );
      return {
        ...(recipient || {}),
        email,
        ...(existingName || nameMap[email]
          ? { name: existingName || nameMap[email] }
          : {}),
      };
    });
  }

  return output;
}
