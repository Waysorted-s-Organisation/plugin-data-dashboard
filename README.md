# Waysorted Operations Dashboard

Owner-only operations console for three production data areas:

- **Credits** — billing wallets and completed credit-consuming tool activity.
- **Newsletter** — N1/N2 automations, audience, templates, campaigns and delivery analytics.
- **Recent Activity** — successful authentication sessions with the latest credited tool activity.

The dashboard intentionally does not present the retired Product Overview, Feature Intelligence, Heatmap or manually incremented public counters. Historical plugin telemetry remains stored for a future ingest repair, but is not treated as current operational data.

## Setup

```bash
npm install
cp .env.example .env
npm run dev
```

Open `http://localhost:4080`. The root URL serves Credits.

### Required production settings

- `BACKEND_MONGODB_URI` — server-only MongoDB URI for Waysorted. Use a read-only database user.
- `BACKEND_MONGODB_DB=waysorted`
- `DASHBOARD_BASIC_AUTH_USER`
- `DASHBOARD_BASIC_AUTH_PASS`
- `NEWSLETTER_API_URL`
- `NEWSLETTER_MANAGEMENT_TOKEN`

Optional:

- `CREDIT_LOW_THRESHOLD=20`
- `NEWSLETTER_PROXY_TIMEOUT_MS=15000`
- Backend collection-name overrides listed in `.env.example`.

`BACKEND_MONGODB_URI` never falls back to the analytics URI. Missing configuration returns an explicit `503`; the UI does not substitute zeroes.

### Archived plugin ingest

The compatibility ingest endpoint remains available:

```text
POST /api/plugin-analytics/ingest
```

It uses the separate analytics settings:

- `MONGODB_URI`
- `MONGODB_DB=plugin_data_dashboard`
- `ANALYTICS_INGEST_TOKEN`
- `ANALYTICS_INGEST_TOKEN_REQUIRED`

## Operations APIs

All operations APIs are protected by dashboard Basic Auth and return `Cache-Control: no-store`.

- `GET /api/operations/credits/overview?days=30`
- `GET /api/operations/credits/users?page=1&pageSize=25`
- `GET /api/operations/credits/users/:userId?days=30`
- `GET /api/operations/activity/recent-users?days=7&page=1&pageSize=25`
- `GET /api/operations/health`
- `GET /api/newsletter/customers/:subscriberId`
- `/api/newsletter/*` — allowlisted server-side proxy to Newsletter management APIs.

Credit consumption counts committed reservation lifecycles, excludes released and pending holds, and subtracts compensation credits. Tool attribution uses `toolCode`, then `featureCode`, then the explicit `Unattributed` label.

## Verification

```bash
npm test
```

The isolated test suite covers authentication, wallet joins, email search, pagination, low-credit boundaries, reservation lifecycle accounting, activity grouping, Newsletter billing joins, sanitized health responses, and removal of retired pages/APIs.

## Vercel

`api/index.js` exports the Express application and `vercel.json` routes requests to it. Configure all required settings for Preview and Production. Never use `NEXT_PUBLIC_*` variables for backend credentials or Newsletter tokens.
