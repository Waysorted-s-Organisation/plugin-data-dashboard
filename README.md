# Waysorted Operations Dashboard

Owner-only product-intelligence console built around the questions a product owner needs to answer:

- **Summary** — joining, activation, return, completed work, credits and verified revenue.
- **Users** — lifecycle segments and a joined 360° profile.
- **Tools** — completion, release, expiry, repeat use and measurement coverage.
- **User Journey** — signup-to-login-to-credited-activation funnel and mature cohort returns.
- **Credits & Billing** — wallets, ledger-backed consumption, purchases, subscriptions and refunds.
- **Attribution** — owner-created UTM checkout links stored in the dashboard database.
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
- `DASHBOARD_BASIC_AUTH_USER`
- `DASHBOARD_BASIC_AUTH_PASS`
- `DASHBOARD_ADMIN_EMAILS=anshbhatt140@gmail.com` — additional email usernames using the same password.
- `NEWSLETTER_API_URL`
- `NEWSLETTER_MANAGEMENT_TOKEN`

Optional:

- `CREDIT_LOW_THRESHOLD=20`
- `NEWSLETTER_PROXY_TIMEOUT_MS=15000`
- `WAYSORTED_PUBLIC_URL=https://www.waysorted.com`
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

All operations APIs are protected by dashboard Basic Auth and return `Cache-Control: no-store`.

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
- `GET /api/operations/attribution/campaigns`
- `POST /api/operations/attribution/campaigns`
- `GET /api/newsletter/customers/:subscriberId`
- `/api/newsletter/*` — allowlisted server-side proxy to Newsletter management APIs.

Credit consumption counts committed reservation lifecycles, excludes released and pending holds, and subtracts compensation credits. Tool attribution uses `toolCode`, then `featureCode`, then the explicit `Unattributed` label.

## Verification

```bash
npm test
```

The isolated test suite covers authentication, wallet joins, lifecycle rules, revenue/refunds, tool-state classification, feedback normalization, pagination, low-credit boundaries, Newsletter joins, telemetry token validation and deduplication, sanitized health responses, and removal of retired pages/APIs.

## Vercel

`api/index.js` exports the Express application and `vercel.json` routes requests to it. Configure all required settings for Preview and Production. Never use `NEXT_PUBLIC_*` variables for backend credentials or Newsletter tokens.
