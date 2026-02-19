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
- `MONGODB_URI`
- `MONGODB_DB` (optional, defaults to `plugin_data_dashboard`)
- `PORT` (optional, defaults to `4080`)
- `ANALYTICS_INGEST_TOKEN` (optional but recommended)

Optional read-access lock for dashboard pages/APIs:
- `DASHBOARD_BASIC_AUTH_USER`
- `DASHBOARD_BASIC_AUTH_PASS`

## 2) Run

```bash
npm run dev
```

Open: `http://localhost:4080`

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

## 4) API Endpoints

- `GET /api/plugin-analytics/summary`
- `GET /api/plugin-analytics/tool-usage`
- `GET /api/plugin-analytics/heatmap`
- `GET /api/plugin-analytics/sessions`
- `GET /api/plugin-analytics/recent-events`

Common query params:
- `from=<ISO date>`
- `to=<ISO date>`
- `tool=<tool id | all>`
- `auth=<authenticated | anonymous | all>`

## Notes

- This dashboard is internal-only by design.
- The Figma plugin side can send both authenticated and anonymous events; user identity fields are optional in each event.
