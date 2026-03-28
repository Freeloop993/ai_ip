# AI IP MVP/P1 (Coze + OpenClaw + Feishu)

## Quick Start
1. Copy `.env.example` to `.env`.
2. Choose publish mode:
   - `PUBLISH_PROVIDER=webhook` + `PUBLISH_WEBHOOK_URL`
   - or `PUBLISH_PROVIDER=bilibili_rpa` (Playwright browser automation)
3. Fill optional `OPENCLAW_*`, `KLING_API_KEY`.
4. If enabling Xiaohongshu image-text generation, fill `IMAGE_API_BASE_URL` + `IMAGE_API_KEY`.
3. Run server:

```powershell
$env:PYTHONPATH = "E:\codex code\ai IP\src"
py -3 -m mvp_pipeline.server
```

## Core Endpoints
- `POST /api/coze-trigger` (ingest + event dedupe + optional openclaw dispatch)
- `GET /api/coze/graph-parameter` (fetch Coze workflow input/output schema)
- `POST /api/coze/pull-run` (server主动调用 Coze workflow 并回灌管线)
- `POST /api/analysis-result` (content-analyst announce)
- `POST /api/production-result` (producer-agent announce)
- `POST /api/producer/run` (run real kling production from backend)
- `POST /api/xhs/generate-draft` (generate Xiaohongshu image-text draft via image API)
- `POST /api/review-callback` (preferred) / `POST /api/review`
- `POST /api/callback/{platform}` (`feishu|qq|slack|discord|wecom`)
- `POST /api/publish/login` (interactive QR login for publish provider, e.g. bilibili_rpa)
- `POST /api/publish/accounts/login/start` (start QR login and return QR image)
- `GET /api/publish/accounts/login/status?session_id=...` (poll QR login status)
- `GET /api/publish/accounts` (list saved login profiles)
- `POST /api/publish/accounts/remove` (remove saved login profile)
- `GET /api/providers/requirements` (check required env for providers)
- `GET /api/config/ip` / `POST /api/config/ip` (minimal config center API)
- `GET /api/config/soul` / `POST /api/config/soul` (SOUL 编辑)
- `GET /api/config/runtime` / `POST /api/config/runtime` (发布节奏 + 密钥环境变量引用)
- `POST /api/collab/feishu/pull` (飞书人工字段回流 SQLite)

## Dashboard
- URL: `GET /dashboard`
- API:
  - `GET /api/dashboard/summary`
  - `GET /api/dashboard/agents`
  - `GET /api/dashboard/metrics`
  - `GET /api/dashboard/errors?run_id=...`

## Recovery Endpoints
- `POST /api/recovery/reconcile`
- `POST /api/recovery/retry-jobs`
- `POST /api/recovery/recover-stuck`
- `GET /api/retry/dead`

## Verification
- Coze: HMAC SHA256 via `X-Coze-Signature` + `COZE_SIGNING_SECRET`.
- Callback strict/permissive via `CALLBACK_VERIFY_MODE`.
- Slack/Feishu/WeCom official signatures supported.

## Coze Pull Mode (No Public Inbound Required)
- Configure:
  - `COZE_WORKFLOW_TOKEN`
  - `COZE_WORKFLOW_RUN_URL` (e.g. `https://xxx.coze.site/run`)
  - optional `COZE_WORKFLOW_GRAPH_PARAMETER_URL` (e.g. `https://xxx.coze.site/graph_parameter`)
  - optional `COZE_WORKFLOW_DEFAULT_INPUTS_JSON`
- APIs:
  - `GET /api/coze/graph-parameter`
  - `POST /api/coze/pull-run` with `{ "inputs": {...}, "dry_run": false }`

## Runtime Guarantees (P1)
- Dedupe key: `event_id` and `platform + video_id`.
- Task idempotency: `run_id + agent` upsert semantics.
- Retry/dead-letter queue for dispatch/sync/publish.
- Stuck recovery for `analyzing/producing/publishing`.

## OpenClaw
- `plugins/coze-bridge/index.ts`: plugin route example.
- `openclaw.example.json`: agent allowlist and defaults.
- `docs/cron-fallback.example.json`: cron fallback example.
- `docs/openclaw-p1.md`: end-to-end P1 integration steps.

## Bilibili RPA (Linux/Windows)
- Install deps:
  - `pip install playwright`
  - `playwright install chromium`
- Configure:
  - `PUBLISH_PROVIDER=bilibili_rpa`
  - optional `BILIBILI_PROFILE_DIR`, `BILIBILI_PROFILE_NAME`
- First-time login:
  - `POST /api/publish/accounts/login/start` with `{ "profile": "default", "wait_seconds": 300 }`
  - frontend receives `qr_image_base64`, render QR and poll status API.
  - login success persists session in profile dir.

## Xiaohongshu 图文帖生图
- Configure image API:
  - `IMAGE_API_BASE_URL`
  - `IMAGE_API_KEY`
  - optional `IMAGE_API_PROVIDER` (default `keling`)
- Generate draft:
  - `POST /api/xhs/generate-draft`
  - payload example:
```json
{
  "content_id": 1,
  "title": "今日科技观察",
  "description": "3张图讲清楚这个热点",
  "image_count": 3,
  "aspect_ratio": "3:4",
  "style": "xiaohongshu"
}
```
- Draft generation writes image urls into production result and moves status to `pending_review` for one-click publish after approval.
