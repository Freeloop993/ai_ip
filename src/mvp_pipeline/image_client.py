import json
import urllib.error
import urllib.request
from typing import Any, Dict, List

from .errors import PipelineError


class ImageGeneratorClient:
    def __init__(
        self,
        *,
        provider: str,
        base_url: str,
        api_key: str,
        timeout_seconds: int,
    ) -> None:
        self.provider = (provider or "keling").strip().lower()
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.base_url and self.api_key)

    def _parse_images(self, data: Dict[str, Any]) -> List[str]:
        images = data.get("images")
        if isinstance(images, list):
            out: List[str] = []
            for item in images:
                if isinstance(item, str):
                    out.append(item)
                elif isinstance(item, dict):
                    url = item.get("url") or item.get("image_url")
                    if isinstance(url, str) and url:
                        out.append(url)
            if out:
                return out

        data_obj = data.get("data")
        if isinstance(data_obj, dict):
            nested = data_obj.get("images")
            if isinstance(nested, list):
                out = []
                for item in nested:
                    if isinstance(item, str):
                        out.append(item)
                    elif isinstance(item, dict):
                        url = item.get("url") or item.get("image_url")
                        if isinstance(url, str) and url:
                            out.append(url)
                if out:
                    return out

        urls = data.get("urls")
        if isinstance(urls, list):
            out = [x for x in urls if isinstance(x, str) and x]
            if out:
                return out

        return []

    def generate_images(
        self,
        *,
        prompt: str,
        count: int,
        aspect_ratio: str,
        style: str,
        negative_prompt: str,
    ) -> Dict[str, Any]:
        if not self.enabled:
            raise PipelineError(
                "IMAGE_API_NOT_CONFIGURED",
                "IMAGE_API_BASE_URL and IMAGE_API_KEY are required",
            )

        payload = {
            "provider": self.provider,
            "prompt": prompt,
            "count": max(1, min(9, int(count))),
            "aspect_ratio": aspect_ratio or "3:4",
            "style": style or "xiaohongshu",
            "negative_prompt": negative_prompt or "",
        }
        req = urllib.request.Request(
            f"{self.base_url}/generate",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {self.api_key}")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8") if exc.fp else str(exc)
            raise PipelineError("IMAGE_API_FAILED", detail) from exc
        except Exception as exc:
            raise PipelineError("IMAGE_API_FAILED", str(exc)) from exc

        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise PipelineError("IMAGE_API_FAILED", "invalid json response from image api") from exc

        images = self._parse_images(data)
        if not images:
            raise PipelineError("IMAGE_API_FAILED", "image api returned no image urls")

        return {
            "provider": self.provider,
            "images": images,
            "raw": data,
        }
