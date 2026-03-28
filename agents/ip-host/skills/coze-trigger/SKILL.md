---
name: coze-trigger
description: 接收 Coze 入站事件并注入到 ip-host，要求携带 event_id/source/video 元信息。
metadata: {"openclaw":{"requires":{"env":["COZE_SIGNING_SECRET"]},"primaryEnv":"COZE_SIGNING_SECRET"}}
---

## 入站协议
```json
{
  "event_id": "evt_xxx",
  "source": "coze",
  "video_url": "https://...",
  "video_id": "...",
  "author": "...",
  "platform": "douyin",
  "stats": {"plays": 0, "likes": 0, "comments": 0},
  "collected_at": "ISO-8601"
}
```

## 处理
1. 校验签名（HMAC SHA256）。
2. 注入 `ip-host` 系统事件。
3. 事件重复时通过 `event_id` 幂等丢弃。
