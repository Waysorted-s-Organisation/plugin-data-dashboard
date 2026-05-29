# Plugin Data Dashboard

Internal analytics dashboard for the Waysorted Figma plugin.

It ingests event batches from the plugin, stores them in MongoDB, and provides KPI + behavior analysis:
- Authenticated vs anonymous usage
- Tool usage and time spent per tool
- Click heatmap for the plugin UI
- Session tracking and recent events

## 1) Setup

```bash
npm install
cp .env.example .env
```

Set required env values in `.env`:
- `MONGODB_URI` (recommended)
- OR one of these supported URI names:
  - `NEXT_PUBLIC_MONGODB_URI_TOOLS`
  - `NEXT_PUBLIC_MONGODB_URI`
  - `MONGO_URI`
  - `MONGO_URL`
- `MONGODB_DB` (optional, defaults to `plugin_data_dashboard`)
- `PORT` (optional, defaults to `4080`)
- `ANALYTICS_INGEST_TOKEN` (optional but recommended)
- `ANALYTICS_INGEST_TOKEN_REQUIRED` (`false` by default; set `true` to enforce token)
- `CRON_SECRET` (recommended if you enable the automated digest cron)
- `RUNOUT_CREDIT_THRESHOLD` (optional, default `50`; users below this are flagged low-credit)
- `RUNOUT_CREDIT_DAYS` (optional, default `14`)
- `RESEND_API_KEY` (optional, required only for actual newsletter sending)
- `CREDIT_NEWSLETTER_FROM` (optional sender, required only for newsletter sending)
- `CREDIT_NEWSLETTER_TO` (optional comma-separated recipients, required only for newsletter sending)
- Optional direct backend-user sync:
  - `BACKEND_MONGODB_URI`
  - `BACKEND_MONGODB_DB`
  - `BACKEND_USERS_COLLECTION` (defaults to `users`)
  - `BACKEND_USER_BILLING_COLLECTION` (defaults to `userbillings`)
  - `BACKEND_CREDIT_LEDGER_COLLECTION` (defaults to `creditledgers`)

Optional read-access lock for dashboard pages/APIs:
- `DASHBOARD_BASIC_AUTH_USER`
- `DASHBOARD_BASIC_AUTH_PASS`

## 2) Run

```bash
npm run dev
```

Open: `http://localhost:4080`

## Vercel Deployment

This repo is Vercel-serverless ready via:
- `api/index.js` (serverless handler)
- `vercel.json` (routes all requests through the handler)

Set these env vars in Vercel Project Settings:
- `MONGODB_URI` (or one of the fallback URI names listed above)
- `MONGODB_DB` (optional)
- `ANALYTICS_INGEST_TOKEN` (recommended)
- `CRON_SECRET`
- Optional credit digest delivery:
  - `RUNOUT_CREDIT_THRESHOLD`
  - `RUNOUT_CREDIT_DAYS`
  - `RESEND_API_KEY`
  - `CREDIT_NEWSLETTER_FROM`
  - `CREDIT_NEWSLETTER_TO`
- Optional direct backend-user sync:
  - `BACKEND_MONGODB_URI`
  - `BACKEND_MONGODB_DB`
  - `BACKEND_USERS_COLLECTION`
  - `BACKEND_USER_BILLING_COLLECTION`
  - `BACKEND_CREDIT_LEDGER_COLLECTION`
- Optional auth gate:
  - `DASHBOARD_BASIC_AUTH_USER`
  - `DASHBOARD_BASIC_AUTH_PASS`

If you use your existing env naming from `wayweb-dev`, `NEXT_PUBLIC_MONGODB_URI_TOOLS` works directly.

## 3) Plugin Ingest Endpoint

Ingest route:

```text
POST /api/plugin-analytics/ingest
```

Body format expected from plugin runtime:

```json
{
  "source": "figma-plugin-main",
  "sessionId": "session_x",
  "deviceId": "device_x",
  "sentAt": "2026-02-19T00:00:00.000Z",
  "runtime": { "editorType": "figma" },
  "plugin": { "name": "waysorted-plugin", "version": "1.0.1" },
  "events": [
    {
      "eventType": "ui_click",
      "eventAt": "2026-02-19T00:00:00.000Z",
      "source": "ui",
      "tool": "palettable",
      "payload": { "x": 200, "y": 80 }
    }
  ]
}
```

If `ANALYTICS_INGEST_TOKEN` is set, pass it in header:

```text
x-plugin-ingest-token: <token>
```

By default, ingest token enforcement is relaxed for easier plugin auto-ingest.
If you need strict enforcement, set:

```text
ANALYTICS_INGEST_TOKEN_REQUIRED=true
```

## 4) API Endpoints

- `GET /api/plugin-analytics/summary`
- `GET /api/plugin-analytics/tool-usage`
- `GET /api/plugin-analytics/heatmap`
- `GET /api/plugin-analytics/sessions`
- `GET /api/plugin-analytics/recent-events`
- `GET /api/plugin-analytics/dashboard` (single optimized payload for UI)
- `GET /api/plugin-analytics/credit-intelligence`
- `GET /api/plugin-analytics/newsletter/runout-preview`
- `POST /api/plugin-analytics/newsletter/runout-send`

Common query params:
- `from=<ISO date>`
- `to=<ISO date>`
- `tool=<tool id | all>`
- `auth=<authenticated | anonymous | all>`

Additional dashboard endpoint query params:
- `heatmapCompact=1` (default true)
- `heatmapLimit=<n>`
- `heatmapGridX=<n>`
- `heatmapGridY=<n>`
- `sessionsLimit=<n>`
- `eventsLimit=<n>`

## Notes

- This dashboard is internal-only by design.
- The Figma plugin side can send both authenticated and anonymous events; user identity fields are optional in each event.
- Credit Intelligence merges two sources:
  - real user balances/emails from the Waysorted backend Mongo collections
  - backend `creditledgers` for tool-attributed credit spend
  - plugin analytics events for tool time, activity timing, and engagement overlays
- Automated daily digest:
  - `vercel.json` schedules `/api/ops/credit-digest` at `05:00 UTC`.
  - The route validates `Authorization: Bearer ${CRON_SECRET}` as recommended by Vercel Cron Jobs.
  - Email sending uses Resend's `/emails` API when `RESEND_API_KEY`, `CREDIT_NEWSLETTER_FROM`, and `CREDIT_NEWSLETTER_TO` are configured.
