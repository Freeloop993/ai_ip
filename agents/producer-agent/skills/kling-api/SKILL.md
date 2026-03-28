---
name: kling-api
description: 调用 Kling API 发起动作迁移并轮询任务状态，返回统一生产结果 JSON。
metadata: {"openclaw":{"requires":{"env":["KLING_API_KEY"]},"primaryEnv":"KLING_API_KEY"}}
---

## 使用
当 producer-agent 需要生产视频时：
1. 读取输入中的 `image_url` 与 `reference_video_url`。
2. 调用 `POST /v1/videos/motion-transfer` 创建任务。
3. 轮询 `GET /v1/tasks/{task_id}`，将状态映射为 `queued|running|completed|failed`。
4. 输出严格 JSON：

```json
{
  "schema_version": "1.0",
  "provider": "kling",
  "task_id": "...",
  "status": "completed",
  "video_url": "https://...",
  "script": "...",
  "tts_text": "...",
  "error": null
}
```
