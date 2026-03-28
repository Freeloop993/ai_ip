import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class FeishuConfig:
    app_id: str
    app_secret: str
    app_token: str
    table_id: str


class FeishuBitableClient:
    def __init__(self, config: FeishuConfig) -> None:
        self.config = config
        self._tenant_access_token: Optional[str] = None

    @property
    def enabled(self) -> bool:
        return all(
            [
                self.config.app_id,
                self.config.app_secret,
                self.config.app_token,
                self.config.table_id,
            ]
        )

    def _request(self, method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self._tenant_access_token:
            req.add_header("Authorization", f"Bearer {self._tenant_access_token}")
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)

    def _ensure_token(self) -> None:
        if self._tenant_access_token:
            return
        payload = {
            "app_id": self.config.app_id,
            "app_secret": self.config.app_secret,
        }
        data = self._request(
            "POST",
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            payload,
        )
        token = data.get("tenant_access_token")
        if not token:
            raise RuntimeError(f"failed to get feishu token: {data}")
        self._tenant_access_token = token

    def create_record(self, fields: Dict[str, Any]) -> Optional[str]:
        if not self.enabled:
            return None
        self._ensure_token()
        url = (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.config.app_token}/tables/{self.config.table_id}/records"
        )
        data = self._request("POST", url, {"fields": fields})
        return data.get("data", {}).get("record", {}).get("record_id")

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        self._ensure_token()
        url = (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.config.app_token}/tables/{self.config.table_id}/records/{record_id}"
        )
        self._request("PUT", url, {"fields": fields})

    def list_records(self, *, limit: int = 200) -> list[Dict[str, Any]]:
        if not self.enabled:
            return []
        self._ensure_token()
        page_token = ""
        page_size = min(200, max(1, limit))
        results: list[Dict[str, Any]] = []

        while len(results) < limit:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            query = urllib.parse.urlencode(params)
            url = (
                "https://open.feishu.cn/open-apis/bitable/v1/apps/"
                f"{self.config.app_token}/tables/{self.config.table_id}/records?{query}"
            )
            data = self._request("GET", url)
            items = data.get("data", {}).get("items", [])
            if not items:
                break
            results.extend(items)
            page_token = data.get("data", {}).get("page_token") or ""
            has_more = bool(data.get("data", {}).get("has_more"))
            if not has_more:
                break

        return results[:limit]


def build_collected_fields(payload: Dict[str, Any], status: str) -> Dict[str, Any]:
    stats = payload.get("stats", {})
    return {
        "对标视频链接": payload.get("video_url", ""),
        "博主账号": payload.get("author", ""),
        "播放量": stats.get("plays", 0),
        "点赞数": stats.get("likes", 0),
        "评论数": stats.get("comments", 0),
        "采集时间": payload.get("collected_at", ""),
        "生产状态": status,
    }


def build_analysis_fields(result: Dict[str, Any], status: str) -> Dict[str, Any]:
    return {
        "视频主题": result.get("topic", ""),
        "开场钩子": result.get("hook", ""),
        "内容结构": result.get("structure", ""),
        "话题标签": ", ".join(result.get("hashtags", [])),
        "AI适配评分": result.get("fit_score", 0),
        "AI推荐理由": result.get("fit_reason", ""),
        "是否选用": "AI推荐" if result.get("replicate") else "不推荐",
        "生产状态": status,
    }


def build_production_fields(result: Dict[str, Any], status: str) -> Dict[str, Any]:
    return {
        "脚本内容": result.get("script", ""),
        "配音文本": result.get("tts_text", ""),
        "视频预览链接": result.get("video_url", ""),
        "生产状态": status,
    }


def build_review_fields(decision: str, publish_url: str | None, status: str) -> Dict[str, Any]:
    review_text = "✅通过" if decision == "approved" else "❌重做"
    return {
        "人工确认": review_text,
        "发布结果链接": publish_url or "",
        "生产状态": status,
    }
