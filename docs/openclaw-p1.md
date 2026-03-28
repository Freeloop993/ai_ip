# OpenClaw P1 Integration Guide

## 1. Agent topology
- `ip-host` (parent/orchestrator)
- `content-analyst`
- `producer-agent`
- `analyst-agent`

Use `openclaw.example.json` as baseline and keep `allowAgents` enabled.

## 2. Coze ingress (primary: pull mode)
- No public inbound required.
- Configure:
  - `COZE_WORKFLOW_TOKEN`
  - `COZE_WORKFLOW_RUN_URL`
  - optional `COZE_WORKFLOW_GRAPH_PARAMETER_URL`
- APIs:
  - `GET /api/coze/graph-parameter`
  - `POST /api/coze/pull-run`
- Behavior:
  1. Server主动调用 Coze workflow `/run`
  2. Parse `videos[]` output
  3. Feed each item into internal `ingest_coze` (dedupe + state machine)

## 2b. Coze ingress (optional push mode)
- Plugin file: `plugins/coze-bridge/index.ts`
- Route: `POST /api/coze-trigger`
- Behavior:
  1. Verify HMAC signature (`COZE_SIGNING_SECRET`)
  2. Inject event to `ip-host` via `injectSystemEvent`

## 3. sessions_spawn chain
- `new_video` -> spawn `content-analyst`
- analysis announce -> `/api/analysis-result`
- if fit score passed -> spawn `producer-agent`
- production announce -> `/api/production-result`

## 4. Callback & publish
- Review callback: `/api/review-callback`
- Platform adapter callback: `/api/callback/{platform}`
- Approved review triggers webhook publisher (`PUBLISH_WEBHOOK_URL`)

## 5. Cron fallback
- Example file: `docs/cron-fallback.example.json`
- Suggested cron task calls `/api/recovery/reconcile`
- Optional second cron task for `/api/recovery/retry-jobs`

## 6. Recovery commands
- Reconcile: `POST /api/recovery/reconcile`
- Retry queue: `POST /api/recovery/retry-jobs`
- Recover stuck: `POST /api/recovery/recover-stuck`
- Dead jobs: `GET /api/retry/dead`
