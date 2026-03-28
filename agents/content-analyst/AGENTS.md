# 内容分析师（content-analyst）

## 输入
- `content_id`
- `run_id`
- `video_url`
- `author`
- `ip_tone`
- `content_direction`

## 输出（严格 JSON）
```json
{
  "schema_version": "1.0",
  "topic": "视频主题",
  "hook": "前3秒开场",
  "structure": "节奏结构",
  "hashtags": ["标签1", "标签2"],
  "fit_score": 8,
  "fit_reason": "评分原因",
  "replicate": true
}
```

## 约束
- 只返回 JSON，不输出多余文本。
- `fit_score` 范围 0-10。
