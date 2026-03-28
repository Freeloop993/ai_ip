# API Contracts (P1)

## 1) Coze ingress
### POST `/api/coze-trigger`
```json
{
  "event_id": "evt_123",
  "source": "coze",
  "video_url": "https://www.douyin.com/video/748...",
  "video_id": "748...",
  "author": "对标博主A",
  "platform": "douyin",
  "stats": { "plays": 120000, "likes": 4800, "comments": 320 },
  "collected_at": "2026-03-28T09:00:00+08:00"
}
```

### GET `/api/coze/graph-parameter`
Fetch workflow input/output schema from configured Coze workflow endpoint.

### POST `/api/coze/pull-run`
```json
{
  "inputs": {
    "profile_url": "https://...",
    "platform_cookie": "...",
    "feishu_app_token": "...",
    "feishu_table_id": "...",
    "max_videos": 10,
    "download_videos": false
  },
  "dry_run": false
}
```
Server calls Coze workflow run endpoint, maps `videos[]` to internal events, then ingests via same dedupe/state machine.

### POST `/api/collect/run`
Built-in collector mode (skip Coze), directly fetches videos and ingests through same dedupe/state machine.
```json
{
  "targets": [
    {
      "platform": "bilibili",
      "profile_url": "https://space.bilibili.com/404534035",
      "cookie_env": "BILIBILI_COOKIE",
      "max_videos": 5
    }
  ],
  "dry_run": false
}
```
If `targets` is omitted, service will fallback to `ip-config.json` target list.
Current built-in collector supports: `bilibili`, `xiaohongshu`, `douyin`, `youtube`.

## 2) Subagent announce - analysis
### POST `/api/analysis-result`
```json
{
  "content_id": 1,
  "run_id": "run-analysis-1",
  "result": {
    "schema_version": "1.0",
    "topic": "...",
    "hook": "...",
    "structure": "...",
    "hashtags": ["#a", "#b"],
    "fit_score": 8,
    "fit_reason": "...",
    "replicate": true
  }
}
```

## 3) Subagent announce - production
### POST `/api/production-result`
```json
{
  "content_id": 1,
  "run_id": "run-producer-1",
  "result": {
    "schema_version": "1.0",
    "provider": "kling",
    "task_id": "task_xxx",
    "status": "completed",
    "video_url": "https://...",
    "script": "...",
    "tts_text": "...",
    "error": null
  }
}
```
Allowed status: `queued|running|completed|failed`.

## 4) Review callback
### POST `/api/review-callback` (alias `/api/review`)
```json
{
  "content_id": 1,
  "run_id": "run-review-1",
  "decision": "approved|rework",
  "review_source": "feishu|qq|slack|discord|wecom|internal-admin",
  "platform": "douyin",
  "feedback": "optional"
}
```

## 5) Platform adapter callback
### POST `/api/callback/{platform}`
Supported: `feishu|qq|slack|discord|wecom|*`.
Normalized into review callback contract.

## 6) Runtime/Recovery
- POST `/api/producer/run`
- POST `/api/xhs/generate-draft`
- POST `/api/publish/login`
- GET `/api/providers/requirements`
- POST `/api/recovery/reconcile`
- POST `/api/recovery/retry-jobs`
- POST `/api/recovery/recover-stuck`
- GET `/api/content`
- GET `/api/content/{id}/timeline`
- GET `/api/retry/dead`

## 7) Dashboard
- GET `/dashboard`
- GET `/api/dashboard/summary`
- GET `/api/dashboard/agents`
- GET `/api/dashboard/metrics`
- GET `/api/dashboard/errors?run_id=...`

## 8) Config center
### GET `/api/config/ip`
Returns active `ip-config.json` (fallback to `ip-config.example.json`).

### POST `/api/config/ip`
```json
{
  "config": {
    "ip": {
      "name": "new-ip"
    }
  }
}
```
Note: `videoApi.apiKey` is rejected. Use `videoApi.apiKeyEnv` only.

### GET `/api/config/soul`
Returns `agents/ip-host/SOUL.md` content.

### POST `/api/config/soul`
```json
{
  "content": "# your soul prompt"
}
```

### GET `/api/config/runtime`
Returns `runtime-config.json` with defaults fallback.

### POST `/api/config/runtime`
```json
{
  "config": {
    "publish_schedule": {
      "enabled": true,
      "timezone": "Asia/Shanghai",
      "daily_limit": 2,
      "slots": ["09:30", "20:00"]
    },
    "secret_refs": {
      "video_api_key_env": "KLING_API_KEY",
      "publish_webhook_token_env": "PUBLISH_WEBHOOK_TOKEN",
      "feishu_app_secret_env": "FEISHU_APP_SECRET"
    }
  }
}
```

## 9) Feishu backflow
### POST `/api/collab/feishu/pull`
```json
{
  "limit": 100,
  "dry_run": false
}
```
Reads Feishu records, writes manual fields (`脚本内容` / `配音文本`) back to SQLite, and applies manual review decision (`人工确认`) when content is `pending_review`.

## 10) Interactive publish login
### POST `/api/publish/login`
```json
{
  "profile": "default",
  "wait_seconds": 300
}
```
For providers that require browser session (e.g. `bilibili_rpa`), opens login page and waits for QR login, then persists session state.

### POST `/api/publish/accounts/login/start`
```json
{
  "profile": "default",
  "wait_seconds": 300
}
```
Returns `session_id` and `qr_image_base64` when login is pending.

### GET `/api/publish/accounts/login/status?session_id=...`
Returns current QR login status (`pending|success|timeout|failed`).

### GET `/api/publish/accounts`
Lists saved publish login profiles.

### POST `/api/publish/accounts/remove`
```json
{
  "profile": "default"
}
```

## 11) Xiaohongshu image draft generation
### POST `/api/xhs/generate-draft`
```json
{
  "content_id": 1,
  "prompt": "可选，不填则由分析结果自动生成",
  "title": "图文标题",
  "description": "图文正文",
  "tags": ["科技", "AI"],
  "image_count": 3,
  "aspect_ratio": "3:4",
  "style": "xiaohongshu",
  "negative_prompt": ""
}
```
Requires `IMAGE_API_BASE_URL` + `IMAGE_API_KEY`. Success response contains `draft.image_urls` and updates content to `pending_review`.

## 12) Provider requirements
### GET `/api/providers/requirements`
Returns provider readiness and required env keys, e.g. `kling` / `keling`.

## Error Codes
- `MISSING_REQUIRED_FIELD`
- `INVALID_SCHEMA_VERSION`
- `INVALID_FIT_SCORE`
- `INVALID_PRODUCTION_STATUS`
- `CONTENT_NOT_FOUND`
- `INVALID_TRANSITION`
- `COZE_SIGNATURE_FAILED`
- `COZE_WORKFLOW_NOT_CONFIGURED`
- `COZE_WORKFLOW_REQUEST_FAILED`
- `COZE_WORKFLOW_INVALID_OUTPUT`
- `COLLECTOR_PLATFORM_UNSUPPORTED`
- `COLLECTOR_INVALID_PROFILE_URL`
- `COLLECTOR_REQUEST_FAILED`
- `CALLBACK_SIGNATURE_FAILED`
- `OPENCLAW_REQUEST_FAILED`
- `KLING_REQUEST_FAILED`
- `PUBLISH_DISPATCH_FAILED`
- `FEISHU_SYNC_FAILED`
- `IMAGE_API_NOT_CONFIGURED`
- `IMAGE_API_FAILED`
- `RETRY_JOB_FAILED`

