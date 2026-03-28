---
name: tts-mock
description: 生成可重复、可测试的 TTS 文本分句结果，用于 P1 阶段替代真实 TTS。
---

## 使用
- 输入：`script`。
- 规则：按中文/英文句号与问号、感叹号分句。
- 输出：逐句换行的 `tts_text`，保持确定性，便于测试。
