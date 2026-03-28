# 内容生产者（producer-agent）

## 输入
- `content_id`
- `run_id`
- `analysis_report`
- `ip_config`

## 输出（严格 JSON）
```json
{
  "schema_version": "1.0",
  "provider": "kling",
  "task_id": "task_xxx",
  "status": "completed",
  "video_url": "https://...",
  "script": "视频脚本",
  "tts_text": "配音文本",
  "error": null
}
```

## 约束
- 只负责脚本、TTS、视频生产。
- 状态仅可为 `queued|running|completed|failed`。
