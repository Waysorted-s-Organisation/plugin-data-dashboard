# Waysorted Operations Dashboard

Owner-only product-intelligence console built around the questions a product owner needs to answer:

- **Summary** — joining, activation, return, completed work, credits and verified revenue.
- **Users** — lifecycle segments and a joined 360° profile.
- **Tools** — completion, release, expiry, repeat use and measurement coverage.
- **User Journey** — signup-to-login-to-credited-activation funnel and mature cohort returns.
- **Credits & Billing** — wallets, ledger-backed consumption, purchases, subscriptions and refunds.
- **Newsletter** — N1/N2 automations, audience, templates, campaigns and delivery analytics.
- **Feedback & Requests** — normalized customer feedback and roadmap demand.
- **Data Health** — freshness, coverage, telemetry state and API reliability.

The dashboard intentionally does not present the retired Product Overview, Feature Intelligence, Heatmap or manually incremented public counters. Historical plugin telemetry remains stored for a future ingest repair, but is not treated as current operational data.

## Setup

```bash
npm install
cp .env.example .env
npm run dev
```

Open `http://localhost:4080`. The root URL serves the balanced Summary control center.

### Required production settings

- `BACKEND_MONGODB_URI` — server-only MongoDB URI for Waysorted. Use a read-only database user.
- `BACKEND_MONGODB_DB=waysorted`
- `DASHBOARD_BASIC_AUTH_USER` — without it the dashboard refuses to serve; there is no open mode.
- `DASHBOARD_BASIC_AUTH_PASS`
- `NEWSLETTER_API_URL`
- `NEWSLETTER_MANAGEMENT_TOKEN`

Optional:

- `CREDIT_LOW_THRESHOLD=20`
- `ANALYTICS_STORE_PASSIVE_EVENTS` — defaults to `false`. Persists heartbeats and transport-config events, which are otherwise dropped at ingest. Enable only while debugging a plugin build.
- `REPORTING_TIMEZONE` — timezone for calendar-day bucketing of activity and returns. Defaults to `UTC`. Set to the timezone your users are in (e.g. `Asia/Kolkata`) so a return is counted on the day it felt like to them.
- `ANALYTICS_ACTIVITY_LOOKBACK_DAYS=400` — how far back plugin activity is aggregated for cohort retention.
- `ANALYTICS_AGGREGATION_TIMEOUT_MS=20000` — ceiling on any single telemetry aggregation. Each one degrades honestly when it cannot be read (activity reports itself unavailable rather than asserting zero), so losing one is survivable where the function timing out is not.
- `NEWSLETTER_PROXY_TIMEOUT_MS=15000`
- Backend collection-name overrides listed in `.env.example`.

`BACKEND_MONGODB_URI` never falls back to the analytics URI. Missing configuration returns an explicit `503`; the UI does not substitute zeroes.

### Semantic plugin telemetry

The compatibility ingest endpoint remains available, but current non-credit behavior is hidden until seven days of healthy semantic coverage exists:

```text
POST /api/plugin-analytics/session
POST /api/plugin-analytics/ingest
```

The Figma plugin exchanges its existing Waysorted bearer credential for a short-lived analytics session. Events include stable `eventId` values so retries are deduplicated. Configure:

- `MONGODB_URI`
- `MONGODB_DB=plugin_data_dashboard`
- `WAYSORTED_API_URL`
- `WAYSORTED_ANALYTICS_PROFILE_PATH=/api/user/profile`
- `ANALYTICS_SIGNING_SECRET`
- `ANALYTICS_SESSION_TTL_SECONDS=900`

`ANALYTICS_INGEST_TOKEN` remains available only as a legacy migration fallback. The semantic contract covers plugin sessions, tool open/close, tool action start/complete/fail, feature use, active time, favorites, billing CTA interactions, user-visible errors and feedback submission.

## Operations APIs

All operations APIs, the newsletter proxy and the dashboard UI sit behind dashboard Basic Auth and return `Cache-Control: no-store`.

**The gate fails closed.** With `DASHBOARD_BASIC_AUTH_USER` or `DASHBOARD_BASIC_AUTH_PASS` unset, every one of them answers `503 Dashboard authentication is not configured` — it does not serve them openly. `npm run dev` sets `ALLOW_UNAUTHENTICATED=true` so local work is unaffected; that variable is deliberately absent from `.env.example` so copying the example onto a server cannot carry the escape hatch with it.

The plugin is never affected by this gate. `POST /api/plugin-analytics/session`, `POST /api/plugin-analytics/ingest` and the public `GET /health` are all registered ahead of it, so telemetry and health probes keep working whatever the dashboard credentials are doing.

- `GET /api/operations/credits/overview?days=30`
- `GET /api/operations/credits/users?page=1&pageSize=25`
- `GET /api/operations/credits/users/:userId?days=30`
- `GET /api/operations/summary?days=30`
- `GET /api/operations/users?page=1&pageSize=25`
- `GET /api/operations/users/:userId`
- `GET /api/operations/tools?days=30`
- `GET /api/operations/tools/:toolCode?days=30`
- `GET /api/operations/lifecycle?days=90`
- `GET /api/operations/commercial?days=30`
- `GET /api/operations/feedback?days=90`
- `GET /api/operations/data-health`
- `GET /api/operations/health`
- `GET /api/newsletter/customers/:subscriberId`
- `/api/newsletter/*` — allowlisted server-side proxy to Newsletter management APIs.

Credit consumption counts committed reservation lifecycles, excludes released and pending holds, and subtracts compensation credits. Tool attribution uses `toolCode`, then `featureCode`, then the explicit `Unattributed` label.

Per-user tool jobs report committed reservations when the person has any, and fall back to completed tool actions observed in plugin telemetry when they do not — a run that never reached a committed reservation, or a signed-out visitor who cannot hold one, would otherwise read as zero next to a tool they demonstrably used. Each row carries `creditedJobs`, `observedJobs` and a `completedJobsSource` of `credited` or `observed`. **The two counts are never summed:** once a tool charges credits a single run emits both a committed reservation and a `tool_action_completed` event, so adding them would report one job as two.

A completed tool action counts as a job only when the same session shows the user opening that tool at or before it. Opening the plugin starts background services that report finished work nobody asked for; without this rule the job column measured how often people opened the plugin. Tools that never report `tool_opened` cannot be judged by the rule, so all of their completions are counted and the tool is listed as ungated on Data Health. The number of excluded completions is reported as `summary.backgroundCompletions` on `GET /api/operations/tools`.

A tool job means a run that **finished**: a committed reservation, or a `tool_action_completed` event. `feature_used` is not counted and never substituted — it marks an invocation, not a completion. Tools that emit activity but never a completion are listed in `GET /api/operations/tools` as `summary.toolsWithoutCompletions` and shown on the Tools page, because their job counts cannot be measured until the plugin reports finished runs.

Commercial standing reads the `subscriptions` collection as well as the wallet, so a subscription granted directly in the backend — which writes no `userbillings` document — still makes the user a customer. Such rows carry `subscriptionSource: "subscription_record"`, and the missing wallet is still reported rather than hidden.

Plugin telemetry is stitched to accounts by session id: every event in a session that carried an account anywhere belongs to that account, including the pre-authentication events at the head of a launch. This is what keeps a hard-login plugin from producing "signed-out visitor" rows. Sessions that never carried an account remain anonymous; sessions carrying two are left unattributed. Because the plugin currently mints a new device id per launch, signed-out visitors cannot be de-duplicated across launches — Data Health reports this under **Plugin identity**, and the fix is to persist the device id in `figma.clientStorage`.

If job counts still look wrong against production, `node scripts/diagnose-user-coverage.js` reports the reservation status distribution, reservations with a broken or missing user link, the credit-ledger reasons actually present, and any telemetry identity that does not join to a `users` document.

## Conversion tracker sync

The "Waysorted — User Conversion Tracker" spreadsheet is co-owned. The dashboard knows when someone signed up, what they last used, whether they are still active and what they pay. A person knows where they came from and what was said to them. The sync writes the first set and never touches the second.

```text
GET /api/exports/users-sheet?days=30
```

Returns the machine-owned columns of the **Users** tab, keyed on email. `humanOwnedColumns` names what the writer must leave alone — currently column E, *Source*. That is the acquisition channel; the dashboard knows the *authentication* source, which is a different fact that happens to sound alike, so the payload does not even carry a field for it.

Never written: the **Activity Log** tab (outreach notes are written by people) and the **Dashboard** tab (formulas, which recalculate themselves once Users is populated).

### Daily sync

`integrations/waysorted-sheet-sync.gs` runs inside the spreadsheet's own Apps Script project. Apps Script rather than a service account on purpose: it already runs as the sheet's owner, so there is no Google Cloud project to create, no key file to download, and no long-lived credential in this repository or in a deployment environment. The only secret is the dashboard's own Basic Auth password, held in the script's properties.

1. Extensions → Apps Script, paste the file, save.
2. Project Settings → Script Properties, add `DASHBOARD_URL`, `DASHBOARD_USER`, `DASHBOARD_PASS`, and press **Save script properties**. `DASHBOARD_USER`/`DASHBOARD_PASS` are the values of `DASHBOARD_BASIC_AUTH_USER`/`DASHBOARD_BASIC_AUTH_PASS` from the deployment. Run `showScriptProperties` if a name is rejected — it lists what is actually saved, and the character count of each value, without printing the password.
3. Run `removePlaceholderRows` once to clear the shipped Alice/Bob rows.
4. Run `syncUsersSheet` once to backfill.
5. Run `installDailyTrigger` once for the 06:00 daily run.

Rows are matched on email: an existing row is updated in place so anything typed beside it survives, a new account is appended, and an account that stops appearing is left alone rather than deleted — a row vanishing is more likely a filter or an outage than a person who ceased to exist.

### One-off export

```bash
node scripts/export-users-sheet.js            # CSV on stdout, ready to paste
node scripts/export-users-sheet.js --json     # full payload, with the evidence behind each status
```

A dropdown set to reject invalid input makes `setValue` **throw**, so before this was handled a single unrecognised tool name aborted the whole sync partway through a row. The script now reads each dropdown's own list, writes only values it accepts, falls back to `Unknown` where the list offers it, and reports whatever it had to drop so the list can be extended. The `Plan` column carries the tier's real name, taken from the subscription's plan code with the billing period stripped — `pro_monthly` and `pro_annual` are both `Pro`. There is no fallback on that column: writing `Free` against someone who is paying, because their tier is spelled differently in the tracker than in the billing records, is a lie that reads as a fact. It is left blank and reported instead.

## Verification

```bash
npm test
```

Test files run one at a time (`--test-concurrency=1`) with the core cache disabled (`DASHBOARD_CORE_CACHE_MS=0`). Every file drives the same module-level Express app and sets `process.env.MONGODB_URI` / `BACKEND_MONGODB_URI` to its own in-memory database, so running them in parallel lets one file point another's requests at the wrong database mid-assertion. The cache is keyed on those URIs, and `MongoMemoryServer` reuses ports, so a 30-second-old entry from a finished test can answer a later one that happens to draw the same port — disabling it makes every assertion read live data.

The isolated test suite covers authentication, wallet joins, lifecycle rules, revenue/refunds, tool-state classification, feedback normalization, pagination, low-credit boundaries, Newsletter joins, telemetry token validation and deduplication, sanitized health responses, and removal of retired pages/APIs.

## Vercel

`api/index.js` exports the Express application and `vercel.json` routes requests to it. Configure all required settings for Preview and Production. Never use `NEXT_PUBLIC_*` variables for backend credentials or Newsletter tokens.
