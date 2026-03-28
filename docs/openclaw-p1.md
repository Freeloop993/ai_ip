# OpenClaw P1 Integration Guide

## 1. Agent topology
- `ip-host` (parent/orchestrator)
- `content-analyst`
- `producer-agent`
- `analyst-agent`

Use `openclaw.example.json` as baseline and keep `allowAgents` enabled.

## 2. Ingress option A: Coze pull mode
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

## 2b. Ingress option B: Coze push mode (optional)
- Plugin file: `plugins/coze-bridge/index.ts`
- Route: `POST /api/coze-trigger`
- Behavior:
  1. Verify HMAC signature (`COZE_SIGNING_SECRET`)
  2. Inject event to `ip-host` via `injectSystemEvent`

## 2c. Ingress option C: Native collector mode (skip Coze)
- Configure:
  - `INGEST_PROVIDER=native`
  - optional `COLLECTOR_TIMEOUT_SECONDS`, `COLLECTOR_DEFAULT_MAX_VIDEOS`
- API:
  - `POST /api/collect/run`
- Behavior:
  1. Server directly collects platform videos (`bilibili`/`xiaohongshu`/`douyin`/`youtube`)
  2. Map to standard ingest event shape
  3. Feed each item into internal `ingest_coze` (dedupe + state machine)

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
