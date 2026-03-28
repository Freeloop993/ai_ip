---
name: xhs-image-generator
description: 调用外部生图 API 生成小红书图文帖图片，并返回图片链接数组
metadata: {"openclaw":{"requires":{"env":["IMAGE_API_KEY","IMAGE_API_BASE_URL"]},"primaryEnv":"IMAGE_API_KEY"}}
---

## 用途
当 producer-agent 需要产出小红书图文帖时，调用该 skill 发起生图任务。

## 输入参数
- `prompt`: 生图提示词
- `count`: 图片数量（建议 3-9）
- `aspect_ratio`: 比例（如 `3:4`、`1:1`）
- `style`: 风格（如 `xiaohongshu`）
- `negative_prompt`: 负向提示词（可选）

## 调用约定
1. 使用 `IMAGE_API_BASE_URL` 作为 API 根地址
2. 请求 `POST {IMAGE_API_BASE_URL}/generate`
3. Header: `Authorization: Bearer ${IMAGE_API_KEY}`
4. 返回标准化字段：
```json
{
  "images": ["https://.../img1.png", "https://.../img2.png"],
  "provider": "keling"
}
```

## 失败处理
- 如果缺少 `IMAGE_API_KEY` 或 `IMAGE_API_BASE_URL`，直接返回可执行错误信息，要求用户补充配置
- 生成失败时返回错误码和摘要，交给 ip-host 进入重试或人工处理
